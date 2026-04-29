import asyncio
import logging

import aiohttp
from web3 import Web3

from config import WBNB, BUSD, PANCAKE_ROUTER_V2

log = logging.getLogger(__name__)

GOPLUS_URL = "https://api.gopluslabs.io/api/v1/token_security/56"

# ── Minimal ABIs ──────────────────────────────────────────────────────────────
PAIR_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0",           "type": "uint112"},
            {"name": "_reserve1",           "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"},
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

def get_bnb_price_sync(w3: Web3) -> float:
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
        log.warning(f"BNB price fetch failed: {e} — using fallback 600")
        return 600.0


def get_liquidity_usd_sync(
    w3: Web3, pair_address: str, base_token: str, bnb_price: float
) -> float:
    """Get total liquidity of a pair in USD (both sides combined = reserve * 2)."""
    try:
        pair     = w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=PAIR_ABI)
        reserves = pair.functions.getReserves().call()
        token0   = pair.functions.token0().call()

        base_reserve = reserves[0] if token0.lower() == base_token.lower() else reserves[1]
        base_normalized = base_reserve / 1e18  # WBNB, BUSD, USDT all use 18 decimals

        if base_token.lower() == WBNB.lower():
            return base_normalized * bnb_price * 2
        else:
            return base_normalized * 2   # BUSD / USDT — already in USD
    except Exception as e:
        log.error(f"Liquidity check error for {pair_address}: {e}")
        return 0.0


# ── Public async helpers ──────────────────────────────────────────────────────

async def get_bnb_price(w3: Web3) -> float:
    return await asyncio.to_thread(get_bnb_price_sync, w3)


# ── Security-only check (no liquidity) ───────────────────────────────────────

async def check_token_security(
    token_address: str,
    max_buy_tax: float,
    max_sell_tax: float,
) -> dict:
    """
    GoPlus security analysis + tax check.
    Does NOT check liquidity — that is handled by ExecutionEngine after
    the user approves, so we never reject a token just because liquidity
    hasn't been added yet.

    Returns:
        {"ok": True,  "info": {...}}
        {"ok": False, "reason": "..."}
    """

    # ── 1. GoPlus API ─────────────────────────────────────────────────────────
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

        if not data:
            # Token brand-new — GoPlus may not have indexed it yet; retry once
            log.info(f"GoPlus: токен не найден, ждём 15с ({token_address})")
            await asyncio.sleep(15)
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    GOPLUS_URL,
                    params={"contract_addresses": token_address.lower()},
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    body = await resp.json(content_type=None)
            result = body.get("result", {})
            data   = result.get(token_address.lower()) or result.get(token_address)

            if not data:
                return {"ok": False, "reason": "GoPlus: токен не найден после повторного запроса"}

    except Exception as e:
        log.error(f"GoPlus API error for {token_address}: {e}")
        return {"ok": False, "reason": "GoPlus недоступен — пропускаем"}

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
    for field_name, reason in CRITICAL.items():
        if data.get(field_name) == "1":
            return {"ok": False, "reason": reason}

    # ── 3. Tax check ──────────────────────────────────────────────────────────
    buy_tax  = float(data.get("buy_tax")  or 0)
    sell_tax = float(data.get("sell_tax") or 0)

    if buy_tax > max_buy_tax:
        return {"ok": False, "reason": f"Buy tax слишком высокий: {buy_tax:.1f}%"}
    if sell_tax > max_sell_tax:
        return {"ok": False, "reason": f"Sell tax слишком высокий: {sell_tax:.1f}%"}

    # ── 4. Build info dict ────────────────────────────────────────────────────
    info = {
        "name":          data.get("token_name",   "Unknown"),
        "symbol":        data.get("token_symbol", "???"),
        "buy_tax":       buy_tax,
        "sell_tax":      sell_tax,
        "holder_count":  data.get("holder_count", "?"),
        "is_mintable":   data.get("is_mintable")   == "1",
        "hidden_owner":  data.get("hidden_owner")  == "1",
        "is_proxy":      data.get("is_proxy")      == "1",
        "external_call": data.get("external_call") == "1",
    }
    return {"ok": True, "info": info}
