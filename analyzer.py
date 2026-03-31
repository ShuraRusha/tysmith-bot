import asyncio
import logging

import aiohttp
from web3 import Web3

from config import WBNB, BUSD, PANCAKE_ROUTER_V2, TOP_HOLDER_MAX_PCT

log = logging.getLogger(__name__)

GOPLUS_URL = "https://api.gopluslabs.io/api/v1/token_security/56"

# Known DEX/locker tags that are safe to ignore in holder checks
SAFE_HOLDER_TAGS = {"pancakeswap", "uniswap", "burned", "dead", "lock", "locker",
                    "unicrypt", "pinksale", "team finance"}

# ── Minimal ABIs ──────────────────────────────────────────────────────────────
PAIR_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0",          "type": "uint112"},
            {"name": "_reserve1",          "type": "uint112"},
            {"name": "_blockTimestampLast","type": "uint32"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ROUTER_ABI_PRICE = [
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path",     "type": "address[]"},
        ],
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# ── On-chain helpers (synchronous — call via asyncio.to_thread) ───────────────

def _get_bnb_price_sync(w3: Web3) -> float:
    """Get BNB price in USD via on-chain WBNB→BUSD route."""
    try:
        router = w3.eth.contract(
            address=Web3.to_checksum_address(PANCAKE_ROUTER_V2),
            abi=ROUTER_ABI_PRICE,
        )
        amounts = router.functions.getAmountsOut(
            Web3.to_wei(1, "ether"),
            [Web3.to_checksum_address(WBNB), Web3.to_checksum_address(BUSD)],
        ).call()
        return amounts[1] / 1e18
    except Exception as e:
        log.warning(f"BNB price fetch failed: {e} — skip trade (no fallback price)")
        return 0.0   # 0 = caller must reject trade, not use a stale guess


def _get_liquidity_usd_sync(
    w3: Web3, pair_address: str, base_token: str, bnb_price: float
) -> float:
    """Get total liquidity of a pair in USD."""
    try:
        pair     = w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=PAIR_ABI)
        reserves = pair.functions.getReserves().call()
        token0   = pair.functions.token0().call()

        if token0.lower() == base_token.lower():
            base_reserve = reserves[0]
        else:
            base_reserve = reserves[1]

        # TVL = base side * price * 2 (pool is balanced at creation)
        base_normalized = base_reserve / 1e18  # WBNB, BUSD, USDT all use 18 decimals

        if base_token.lower() == WBNB.lower():
            return base_normalized * bnb_price * 2
        else:
            return base_normalized * 2   # BUSD / USDT already in USD
    except Exception as e:
        log.error(f"Liquidity check error for {pair_address}: {e}")
        return 0.0


# ── GoPlus fetch with one retry ───────────────────────────────────────────────

async def _goplus_fetch(token_address: str) -> dict | None:
    """Fetch GoPlus data. Retries once after 5s if token not yet indexed."""
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    GOPLUS_URL,
                    params={"contract_addresses": token_address.lower()},
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    body = await resp.json(content_type=None)
            result = body.get("result", {})
            data   = result.get(token_address.lower()) or result.get(token_address)
            if data:
                return data
            if attempt == 0:
                log.info(f"GoPlus: токен ещё не проиндексирован, ждём 5с ({token_address})")
                await asyncio.sleep(5)
        except Exception as e:
            log.error(f"GoPlus API error (attempt {attempt+1}): {e}")
            if attempt == 0:
                await asyncio.sleep(3)
    return None


# ── Public async helpers ──────────────────────────────────────────────────────

async def get_bnb_price(w3: Web3) -> float:
    return await asyncio.to_thread(_get_bnb_price_sync, w3)


# ── Main security check ───────────────────────────────────────────────────────

async def check_token(
    token_address: str,
    pair_address: str,
    base_token: str,
    w3: Web3,
    min_liquidity_usd: float,
    max_buy_tax: float,
    max_sell_tax: float,
) -> dict:
    """
    Full safety check via GoPlus API + on-chain liquidity.

    Returns:
        {"ok": True,  "info": {...}}
        {"ok": False, "reason": "..."}
    """

    # ── 1. GoPlus security analysis ───────────────────────────────────────────
    data = await _goplus_fetch(token_address)
    if data is None:
        return {"ok": False, "reason": "GoPlus: токен не найден — пропускаем"}

    # ── 2. Critical flags — instant reject ───────────────────────────────────
    CRITICAL = {
        "is_honeypot":             "Honeypot",
        "can_take_back_ownership": "Может вернуть ownership",
        "owner_change_balance":    "Владелец может менять балансы",
        "selfdestruct":            "Selfdestruct функция",
        "transfer_pausable":       "Переводы можно заморозить",
        "is_blacklisted":          "Blacklist функция",
        "cannot_buy":              "Покупка заблокирована контрактом",
        "trading_cooldown":        "Trading cooldown (anti-bot)",
    }
    for flag, reason in CRITICAL.items():
        if data.get(flag) == "1":
            return {"ok": False, "reason": reason}

    # ── 3. Tax check ──────────────────────────────────────────────────────────
    buy_tax  = float(data.get("buy_tax")  or 0)
    sell_tax = float(data.get("sell_tax") or 0)
    if buy_tax > max_buy_tax:
        return {"ok": False, "reason": f"Buy tax: {buy_tax:.1f}%"}
    if sell_tax > max_sell_tax:
        return {"ok": False, "reason": f"Sell tax: {sell_tax:.1f}%"}

    # ── 4. Top holder concentration check ────────────────────────────────────
    holders = data.get("holders", [])
    for h in holders[:5]:   # check top 5 holders
        pct  = float(h.get("percent", 0)) * 100   # GoPlus: 0–1 decimal
        tag  = (h.get("tag") or "").lower()
        is_locked   = h.get("is_locked",   0) == 1
        is_contract = h.get("is_contract", 0) == 1

        # Skip DEX contracts, lock contracts, burn addresses
        if is_locked or any(s in tag for s in SAFE_HOLDER_TAGS):
            continue

        if pct > TOP_HOLDER_MAX_PCT:
            return {
                "ok":     False,
                "reason": f"Кит держит {pct:.1f}% токенов — риск дампа",
            }

    # ── 5. Liquidity check ────────────────────────────────────────────────────
    bnb_price = await get_bnb_price(w3)
    if bnb_price == 0.0:
        return {"ok": False, "reason": "Не удалось получить цену BNB — пропускаем"}

    liquidity_usd = await asyncio.to_thread(
        _get_liquidity_usd_sync, w3, pair_address, base_token, bnb_price
    )
    if liquidity_usd < min_liquidity_usd:
        return {"ok": False, "reason": f"Ликвидность: ${liquidity_usd:,.0f}"}

    # ── 6. Build info dict ────────────────────────────────────────────────────
    info = {
        "name":          data.get("token_name",   "Unknown"),
        "symbol":        data.get("token_symbol", "???"),
        "buy_tax":       buy_tax,
        "sell_tax":      sell_tax,
        "liquidity_usd": liquidity_usd,
        "bnb_price":     bnb_price,
        "holder_count":  data.get("holder_count", "?"),
        # ⚠️ Warnings — shown to user, not blockers
        "is_mintable":   data.get("is_mintable")   == "1",
        "hidden_owner":  data.get("hidden_owner")  == "1",
        "is_proxy":      data.get("is_proxy")      == "1",
        "external_call": data.get("external_call") == "1",
    }
    return {"ok": True, "info": info}
