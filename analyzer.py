import asyncio
import logging
import time

import aiohttp
from web3 import Web3

from config import WBNB, BUSD, PANCAKE_ROUTER_V2, TOP_HOLDER_MAX_PCT

log = logging.getLogger(__name__)

GOPLUS_URL   = "https://api.gopluslabs.io/api/v1/token_security/56"
GOPLUS_TIMEOUT = 1.5   # seconds — fast path; GoPlus is supplementary, not blocking

# Known DEX/locker tags that are safe to ignore in holder checks
SAFE_HOLDER_TAGS = {"pancakeswap", "uniswap", "burned", "dead", "lock", "locker",
                    "unicrypt", "pinksale", "team finance"}

# ── ABIs ──────────────────────────────────────────────────────────────────────

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

ROUTER_ABI_SWAP = [
    {
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path",         "type": "address[]"},
            {"name": "to",           "type": "address"},
            {"name": "deadline",     "type": "uint256"},
        ],
        "name": "swapExactETHForTokensSupportingFeeOnTransferTokens",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    }
]

ERC20_ABI = [
    {"inputs": [], "name": "name",     "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "symbol",   "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}],  "stateMutability": "view", "type": "function"},
]

# ── On-chain helpers ──────────────────────────────────────────────────────────

def _get_bnb_price_sync(w3: Web3) -> float:
    try:
        router  = w3.eth.contract(address=Web3.to_checksum_address(PANCAKE_ROUTER_V2), abi=ROUTER_ABI_PRICE)
        amounts = router.functions.getAmountsOut(
            Web3.to_wei(1, "ether"),
            [Web3.to_checksum_address(WBNB), Web3.to_checksum_address(BUSD)],
        ).call()
        return amounts[1] / 1e18
    except Exception as e:
        log.warning(f"BNB price fetch failed: {e}")
        return 0.0


def _get_liquidity_usd_sync(
    w3: Web3, pair_address: str, base_token: str, bnb_price: float
) -> float:
    try:
        pair     = w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=PAIR_ABI)
        reserves = pair.functions.getReserves().call()
        token0   = pair.functions.token0().call()

        base_reserve = reserves[0] if token0.lower() == base_token.lower() else reserves[1]
        base_norm    = base_reserve / 1e18

        return base_norm * bnb_price * 2 if base_token.lower() == WBNB.lower() else base_norm * 2
    except Exception as e:
        log.error(f"Liquidity check error {pair_address}: {e}")
        return 0.0


def _get_token_info_sync(w3: Web3, token_address: str) -> dict:
    """Fallback on-chain token name/symbol/decimals when GoPlus is unavailable."""
    try:
        token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
        return {
            "name":     token.functions.name().call(),
            "symbol":   token.functions.symbol().call(),
            "decimals": token.functions.decimals().call(),
        }
    except Exception:
        return {"name": "Unknown", "symbol": "???", "decimals": 18}


def _simulate_buy_sync(w3: Web3, token_address: str, wallet_address: str) -> dict:
    """
    Simulate a buy via eth_call — no real transaction, no gas spent.

    Catches:
      - Trading not yet enabled (contract blocks buys)
      - Hardcoded revert / honeypot on entry
      - Zero liquidity / broken pool

    Returns:
      {"ok": True}                        — buy simulation passed
      {"ok": False, "reason": "..."}      — buy would fail
    """
    try:
        token_cs  = Web3.to_checksum_address(token_address)
        wbnb_cs   = Web3.to_checksum_address(WBNB)
        router_cs = Web3.to_checksum_address(PANCAKE_ROUTER_V2)
        sim_wei   = Web3.to_wei(0.005, "ether")   # 0.005 BNB test amount
        deadline  = int(time.time()) + 60

        router_price = w3.eth.contract(address=router_cs, abi=ROUTER_ABI_PRICE)
        router_swap  = w3.eth.contract(address=router_cs, abi=ROUTER_ABI_SWAP)

        # ── Step 1: price quote (pure reserve math, instant) ──────────────────
        try:
            amounts = router_price.functions.getAmountsOut(
                sim_wei, [wbnb_cs, token_cs]
            ).call()
            if amounts[1] == 0:
                return {"ok": False, "reason": "Симуляция: нулевой выход из пула"}
        except Exception as e:
            return {"ok": False, "reason": f"Симуляция: нет ликвидности ({e})"}

        # ── Step 2: simulate actual swap ──────────────────────────────────────
        # eth_call doesn't execute on-chain; nodes don't enforce sender balance.
        # A revert here = contract actively blocks this buy (honeypot / not started).
        try:
            router_swap.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
                0,
                [wbnb_cs, token_cs],
                Web3.to_checksum_address(wallet_address),
                deadline,
            ).call({"from": Web3.to_checksum_address(wallet_address), "value": sim_wei})
        except Exception as e:
            err = str(e).lower()
            # Node-level errors (gas, balance) ≠ contract revert → don't block
            if any(x in err for x in ["insufficient funds", "gas", "nonce"]):
                log.warning(f"Simulation node error (non-blocking): {e}")
                return {"ok": True}   # can't conclude honeypot from node error
            return {"ok": False, "reason": f"Симуляция отклонена контрактом — вероятно honeypot"}

        return {"ok": True}

    except Exception as e:
        # Unexpected error in simulation — don't block the trade
        log.warning(f"simulate_buy unexpected error: {e}")
        return {"ok": True}


# ── GoPlus fetch with short timeout ──────────────────────────────────────────

async def _goplus_fetch(token_address: str) -> dict | None:
    """
    Fetch GoPlus data with a 1.5s timeout.
    Returns None if token not indexed yet or if request times out.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GOPLUS_URL,
                params={"contract_addresses": token_address.lower()},
                timeout=aiohttp.ClientTimeout(total=GOPLUS_TIMEOUT),
            ) as resp:
                body = await resp.json(content_type=None)
        result = body.get("result", {})
        return result.get(token_address.lower()) or result.get(token_address)
    except asyncio.TimeoutError:
        log.info(f"GoPlus timeout ({GOPLUS_TIMEOUT}s) for {token_address} — proceeding without it")
        return None
    except Exception as e:
        log.warning(f"GoPlus error: {e}")
        return None


# ── Public async helpers ──────────────────────────────────────────────────────

async def get_bnb_price(w3: Web3) -> float:
    return await asyncio.to_thread(_get_bnb_price_sync, w3)


# ── Main security check ───────────────────────────────────────────────────────

async def check_token(
    token_address: str,
    pair_address:  str,
    base_token:    str,
    w3:            Web3,
    min_liquidity_usd: float,
    max_buy_tax:       float,
    max_sell_tax:      float,
    wallet_address:    str = "0x0000000000000000000000000000000000000001",
) -> dict:
    """
    Two-track security check running in parallel:

      Track A — On-chain simulation (200–500 ms):
        • price quote via getAmountsOut
        • buy simulation via eth_call (catches honeypots, disabled trading)

      Track B — GoPlus API (timeout 1.5 s):
        • critical flags (ownership, blacklist, selfdestruct …)
        • tax cross-check
        • top-holder concentration

    If simulation fails  → instant reject (don't wait for GoPlus).
    If GoPlus times out  → proceed on simulation result + on-chain data.
    If both pass         → merge results.
    """

    # ── Run simulation + GoPlus in parallel ───────────────────────────────────
    sim_task     = asyncio.to_thread(_simulate_buy_sync, w3, token_address, wallet_address)
    goplus_task  = _goplus_fetch(token_address)

    sim_result, goplus_data = await asyncio.gather(sim_task, goplus_task)

    # ── Track A: simulation result ────────────────────────────────────────────
    if not sim_result["ok"]:
        return {"ok": False, "reason": sim_result["reason"]}

    # ── Track B: GoPlus critical flags ────────────────────────────────────────
    buy_tax  = 0.0
    sell_tax = 0.0
    warnings_from_goplus = []

    if goplus_data:
        CRITICAL = {
            "is_honeypot":             "Honeypot (GoPlus)",
            "can_take_back_ownership": "Может вернуть ownership",
            "owner_change_balance":    "Владелец может менять балансы",
            "selfdestruct":            "Selfdestruct функция",
            "transfer_pausable":       "Переводы можно заморозить",
            "is_blacklisted":          "Blacklist функция",
            "cannot_buy":              "Покупка заблокирована контрактом",
            "trading_cooldown":        "Trading cooldown (anti-bot)",
        }
        for flag, reason in CRITICAL.items():
            if goplus_data.get(flag) == "1":
                return {"ok": False, "reason": reason}

        buy_tax  = float(goplus_data.get("buy_tax")  or 0)
        sell_tax = float(goplus_data.get("sell_tax") or 0)
        if buy_tax > max_buy_tax:
            return {"ok": False, "reason": f"Buy tax: {buy_tax:.1f}%"}
        if sell_tax > max_sell_tax:
            return {"ok": False, "reason": f"Sell tax: {sell_tax:.1f}%"}

        # Top-holder concentration
        for h in (goplus_data.get("holders") or [])[:5]:
            pct  = float(h.get("percent", 0)) * 100
            tag  = (h.get("tag") or "").lower()
            if h.get("is_locked", 0) == 1 or any(s in tag for s in SAFE_HOLDER_TAGS):
                continue
            if pct > TOP_HOLDER_MAX_PCT:
                return {"ok": False, "reason": f"Кит держит {pct:.1f}% — риск дампа"}

        # Non-critical warnings
        if goplus_data.get("is_mintable")   == "1": warnings_from_goplus.append("⚠️ Mintable")
        if goplus_data.get("hidden_owner")  == "1": warnings_from_goplus.append("⚠️ Hidden owner")
        if goplus_data.get("is_proxy")      == "1": warnings_from_goplus.append("⚠️ Proxy контракт")
        if goplus_data.get("external_call") == "1": warnings_from_goplus.append("⚠️ External call")
    else:
        warnings_from_goplus.append("⚠️ GoPlus недоступен — только симуляция")

    # ── Liquidity check ───────────────────────────────────────────────────────
    bnb_price = await get_bnb_price(w3)
    if bnb_price == 0.0:
        return {"ok": False, "reason": "Не удалось получить цену BNB"}

    liquidity_usd = await asyncio.to_thread(
        _get_liquidity_usd_sync, w3, pair_address, base_token, bnb_price
    )
    if liquidity_usd < min_liquidity_usd:
        return {"ok": False, "reason": f"Ликвидность: ${liquidity_usd:,.0f}"}

    # ── Build info dict ───────────────────────────────────────────────────────
    if goplus_data:
        name   = goplus_data.get("token_name",   "Unknown")
        symbol = goplus_data.get("token_symbol", "???")
        holder_count = goplus_data.get("holder_count", "?")
    else:
        # Fallback to on-chain ERC20 metadata
        token_meta   = await asyncio.to_thread(_get_token_info_sync, w3, token_address)
        name         = token_meta["name"]
        symbol       = token_meta["symbol"]
        holder_count = "?"

    info = {
        "name":          name,
        "symbol":        symbol,
        "buy_tax":       buy_tax,
        "sell_tax":      sell_tax,
        "liquidity_usd": liquidity_usd,
        "bnb_price":     bnb_price,
        "holder_count":  holder_count,
        "is_mintable":   bool(goplus_data and goplus_data.get("is_mintable")   == "1"),
        "hidden_owner":  bool(goplus_data and goplus_data.get("hidden_owner")  == "1"),
        "is_proxy":      bool(goplus_data and goplus_data.get("is_proxy")      == "1"),
        "external_call": bool(goplus_data and goplus_data.get("external_call") == "1"),
        "goplus_ok":     goplus_data is not None,
        "extra_warnings": warnings_from_goplus,
    }
    return {"ok": True, "info": info}
