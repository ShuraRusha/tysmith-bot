import asyncio
import logging
import time

import aiohttp
from web3 import Web3

from config import WBNB, BUSD, USDT, PANCAKE_FACTORY_V2, PANCAKE_ROUTER_V2, TOP_HOLDER_MAX_PCT

log = logging.getLogger(__name__)

GOPLUS_URL   = "https://api.gopluslabs.io/api/v1/token_security/56"
GOPLUS_TIMEOUT = 3.0   # increased from 1.5s — new tokens need more time to index

HONEYPOT_IS_URL  = "https://api.honeypot.is/v2/IsHoneypot"
HONEYPOT_TIMEOUT = 3.0

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

TRANSFER_ABI = [
    {
        "name": "transfer",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "to",     "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs":  [{"name": "account", "type": "address"}],
        "outputs": [{"name": "",        "type": "uint256"}],
    },
]

ERC20_ABI = [
    {"inputs": [], "name": "name",        "outputs": [{"type": "string"}],  "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "symbol",      "outputs": [{"type": "string"}],  "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals",    "outputs": [{"type": "uint8"}],   "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "totalSupply", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
]

DEXSCREENER_URL     = "https://api.dexscreener.com/latest/dex/pairs/bsc"
DEXSCREENER_TIMEOUT = 3.0

FACTORY_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
        ],
        "name": "getPair",
        "outputs": [{"name": "pair", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
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


def _simulate_sell_sync(w3: Web3, token_address: str, pair_address: str) -> dict:
    """
    Simulate token transfer FROM the pair contract to detect sell-blocking honeypots.

    The pair always holds token reserves, so transfer(dead, 1) from pair works
    for legitimate tokens.  Honeypots that block ALL sells (not just whitelisted
    wallets) will revert here.  This catches the most common BSC honeypot pattern.

    Returns {"ok": True} on success or on any ambiguous error (don't block good tokens).
    Returns {"ok": False, ...} only when we're confident it's a sell block.
    """
    BURN = Web3.to_checksum_address("0x000000000000000000000000000000000000dead")
    try:
        token_cs = Web3.to_checksum_address(token_address)
        pair_cs  = Web3.to_checksum_address(pair_address)
        token    = w3.eth.contract(address=token_cs, abi=TRANSFER_ABI)

        # Confirm the pair actually holds tokens before simulating
        pair_balance = token.functions.balanceOf(pair_cs).call()
        if pair_balance == 0:
            return {"ok": True}  # brand-new pair with no balance yet — can't judge

        # Simulate transfer of 1 wei from pair → burn address
        token.functions.transfer(BURN, 1).call({"from": pair_cs})
        return {"ok": True}

    except Exception as e:
        err = str(e).lower()
        # Only flag as honeypot on clear contract-level revert, not node/gas errors
        if "execution reverted" in err and not any(
            x in err for x in ["insufficient funds", "gas required", "out of gas"]
        ):
            log.info(f"Sell simulation reverted for {token_address}: {e}")
            return {"ok": False, "reason": "Симуляция продажи: контракт блокирует продажу — вероятно honeypot"}
        log.debug(f"simulate_sell non-blocking error ({token_address}): {e}")
        return {"ok": True}


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

async def _honeypot_is_check(token_address: str) -> dict:
    """
    Check honeypot.is — simulates BOTH buy and sell transactions on-chain.
    This is the most reliable sell-honeypot detector available for free.

    Returns {"ok": False, "reason": "..."} if honeypot detected.
    Returns {"ok": True} if safe OR if API is unavailable (fail-open).
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                HONEYPOT_IS_URL,
                params={"address": token_address, "chainID": "56"},
                timeout=aiohttp.ClientTimeout(total=HONEYPOT_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return {"ok": True}
                body = await resp.json(content_type=None)

        hp = body.get("honeypotResult", {}) or {}
        if hp.get("isHoneypot"):
            reason = hp.get("honeypotReason") or "honeypot.is"
            return {"ok": False, "reason": f"Honeypot обнаружен: {reason}"}

        sim = body.get("simulationResult", {}) or {}
        sell_tax = float(sim.get("sellTax", 0) or 0)
        if sell_tax > 49:
            return {"ok": False, "reason": f"Sell tax {sell_tax:.0f}% — фактически honeypot"}

        return {"ok": True}

    except asyncio.TimeoutError:
        log.info(f"honeypot.is timeout for {token_address} — proceeding")
        return {"ok": True}
    except Exception as e:
        log.warning(f"honeypot.is error: {e}")
        return {"ok": True}


async def _dexscreener_fetch(pair_address: str) -> dict | None:
    """
    Fetch DexScreener data for a pair.
    Returns the pair dict or None if not indexed yet (brand-new pairs).
    Fields used: volume.m5, fdv, marketCap, pairCreatedAt, liquidity.usd
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{DEXSCREENER_URL}/{pair_address}",
                timeout=aiohttp.ClientTimeout(total=DEXSCREENER_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return None
                body = await resp.json(content_type=None)
        pairs = body.get("pairs") or []
        return pairs[0] if pairs else None
    except Exception as e:
        log.debug(f"DexScreener unavailable for {pair_address}: {e}")
        return None


def _get_fdv_usd_sync(w3: Web3, token_address: str, bnb_price: float) -> float:
    """
    Calculate FDV (Fully Diluted Value) = totalSupply * pricePerToken_USD.
    Uses on-chain totalSupply + router getAmountsOut for price.
    Returns 0.0 on any error (fail-open — don't block on missing data).
    """
    try:
        token_cs = Web3.to_checksum_address(token_address)
        wbnb_cs  = Web3.to_checksum_address(WBNB)
        token    = w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        decimals         = token.functions.decimals().call()
        total_supply_raw = token.functions.totalSupply().call()
        total_supply     = total_supply_raw / (10 ** decimals)

        router  = w3.eth.contract(address=Web3.to_checksum_address(PANCAKE_ROUTER_V2), abi=ROUTER_ABI_PRICE)
        amounts = router.functions.getAmountsOut(10 ** decimals, [token_cs, wbnb_cs]).call()
        price_bnb = amounts[1] / 1e18

        return total_supply * price_bnb * bnb_price
    except Exception as e:
        log.debug(f"FDV calc failed for {token_address}: {e}")
        return 0.0


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


def _find_pair_sync(w3: Web3, token_address: str) -> tuple[str, str] | None:
    """
    Find the best PancakeSwap V2 pair for a token (tries WBNB, BUSD, USDT).
    Returns (pair_address, base_token) or None if not found.
    """
    ZERO = "0x0000000000000000000000000000000000000000"
    factory = w3.eth.contract(
        address=Web3.to_checksum_address(PANCAKE_FACTORY_V2), abi=FACTORY_ABI
    )
    for base in [WBNB, BUSD, USDT]:
        try:
            pair = factory.functions.getPair(
                Web3.to_checksum_address(token_address),
                Web3.to_checksum_address(base),
            ).call()
            if pair.lower() != ZERO.lower():
                return pair, base
        except Exception:
            continue
    return None


async def analyze_token(token_address: str, w3: Web3,
                        wallet_address: str = "0x0000000000000000000000000000000000000001") -> dict:
    """
    Full non-rejecting analysis of a token — collects all available metrics
    without early return on failure. Used by the /analyze command.

    Returns a dict with all metrics + per-check pass/fail flags.
    """
    # ── Find pair ─────────────────────────────────────────────────────────────
    pair_info = await asyncio.to_thread(_find_pair_sync, w3, token_address)
    if not pair_info:
        return {"found": False, "reason": "Пара не найдена на PancakeSwap V2"}

    pair_address, base_token = pair_info

    # ── Run all checks in parallel ─────────────────────────────────────────────
    sim_buy_task   = asyncio.to_thread(_simulate_buy_sync,  w3, token_address, wallet_address)
    sim_sell_task  = asyncio.to_thread(_simulate_sell_sync, w3, token_address, pair_address)
    goplus_task    = _goplus_fetch(token_address)
    honeypot_task  = _honeypot_is_check(token_address)
    dexscreen_task = _dexscreener_fetch(pair_address)
    bnb_price_task = asyncio.to_thread(_get_bnb_price_sync, w3)

    sim_result, sell_sim, goplus_data, hp_result, dex, bnb_price = await asyncio.gather(
        sim_buy_task, sim_sell_task, goplus_task, honeypot_task, dexscreen_task, bnb_price_task
    )

    # ── On-chain metrics ──────────────────────────────────────────────────────
    liquidity_usd = await asyncio.to_thread(
        _get_liquidity_usd_sync, w3, pair_address, base_token, bnb_price
    )
    fdv_usd = await asyncio.to_thread(_get_fdv_usd_sync, w3, token_address, bnb_price)

    # ── Token identity ────────────────────────────────────────────────────────
    if goplus_data:
        name         = goplus_data.get("token_name",   "Unknown")
        symbol       = goplus_data.get("token_symbol", "???")
        holder_count = goplus_data.get("holder_count", "?")
    else:
        token_meta   = await asyncio.to_thread(_get_token_info_sync, w3, token_address)
        name         = token_meta["name"]
        symbol       = token_meta["symbol"]
        holder_count = "?"

    # ── GoPlus flags ──────────────────────────────────────────────────────────
    CRITICAL_FLAGS = {
        "is_honeypot":             "Honeypot",
        "can_take_back_ownership": "Возврат ownership",
        "owner_change_balance":    "Владелец меняет балансы",
        "selfdestruct":            "Selfdestruct",
        "transfer_pausable":       "Заморозка переводов",
        "is_blacklisted":          "Blacklist",
        "cannot_buy":              "Покупка заблокирована",
        "cannot_sell_all":         "Продажа всех токенов заблокирована",
        "trading_cooldown":        "Trading cooldown",
        "is_anti_whale":           "Anti-whale ограничения",
    }
    critical_flags = {}
    warnings       = []
    buy_tax  = 0.0
    sell_tax = 0.0
    top10_pct = 0.0
    lp_locked = None  # None = unknown

    if goplus_data:
        for flag, label in CRITICAL_FLAGS.items():
            if goplus_data.get(flag) == "1":
                critical_flags[flag] = label

        buy_tax  = float(goplus_data.get("buy_tax")  or 0)
        sell_tax = float(goplus_data.get("sell_tax") or 0)

        if goplus_data.get("is_mintable")   == "1": warnings.append("Mintable — могут допечатать токены")
        if goplus_data.get("hidden_owner")  == "1": warnings.append("Hidden owner")
        if goplus_data.get("is_proxy")      == "1": warnings.append("Proxy контракт")
        if goplus_data.get("external_call") == "1": warnings.append("External call")

        # Top-10 holder concentration (excl. DEX/locked)
        for h in (goplus_data.get("holders") or [])[:10]:
            pct     = float(h.get("percent", 0)) * 100
            tag     = (h.get("tag") or "").lower()
            is_safe = h.get("is_locked", 0) == 1 or any(s in tag for s in SAFE_HOLDER_TAGS)
            if not is_safe:
                top10_pct += pct

        # LP lock
        lp_holders = goplus_data.get("lp_holders") or []
        if lp_holders:
            lp_locked = True
            for lph in lp_holders[:10]:
                lp_pct    = float(lph.get("percent", 0)) * 100
                is_locked = lph.get("is_locked", 0) == 1
                tag       = (lph.get("tag") or "").lower()
                is_safe   = is_locked or any(s in tag for s in SAFE_HOLDER_TAGS)
                if not is_safe and lp_pct > 50:
                    lp_locked = False
                    break
        # else: lp_locked stays None (not indexed)

    # ── DexScreener data ──────────────────────────────────────────────────────
    age_days = None
    vol_5m   = None
    if dex:
        created_at_ms = dex.get("pairCreatedAt") or 0
        if created_at_ms:
            age_days = (time.time() - created_at_ms / 1000) / 86400
        vol_5m_raw = (dex.get("volume") or {}).get("m5")
        if vol_5m_raw is not None:
            vol_5m = float(vol_5m_raw)

    return {
        "found":           True,
        "token_address":   token_address,
        "pair_address":    pair_address,
        "base_token":      base_token,
        "name":            name,
        "symbol":          symbol,
        "holder_count":    holder_count,
        "bnb_price":       bnb_price,
        "liquidity_usd":   liquidity_usd,
        "fdv_usd":         fdv_usd,
        "buy_tax":         buy_tax,
        "sell_tax":        sell_tax,
        "sim_buy_ok":      sim_result["ok"],
        "sim_buy_reason":  sim_result.get("reason", ""),
        "sim_sell_ok":     sell_sim["ok"],
        "sim_sell_reason": sell_sim.get("reason", ""),
        "hp_is_ok":        hp_result["ok"],
        "hp_is_reason":    hp_result.get("reason", ""),
        "goplus_ok":       goplus_data is not None,
        "critical_flags":  critical_flags,
        "warnings":        warnings,
        "top10_pct":       top10_pct,
        "lp_locked":       lp_locked,
        "age_days":        age_days,
        "vol_5m":          vol_5m,
    }


# ── Main security check ───────────────────────────────────────────────────────

async def check_token(
    token_address: str,
    pair_address:  str,
    base_token:    str,
    w3:            Web3,
    min_liquidity_usd:   float,
    max_buy_tax:         float,
    max_sell_tax:        float,
    wallet_address:      str   = "0x0000000000000000000000000000000000000001",
    require_goplus:      bool  = False,    # True = skip token if GoPlus is down (auto mode)
    lp_holder_max_pct:   float = 30.0,    # reject if any unlocked wallet holds >X% of LP
    min_market_cap_usd:  float = 30_000,
    min_fdv_usd:         float = 200_000,
    max_fdv_usd:         float = 10_000_000,
    max_top10_holder_pct: float = 30.0,   # top-10 non-DEX/locked holders combined
    min_volume_5m_usd:   float = 1_000,   # skip if DexScreener 5-min volume below this
    max_token_age_days:  int   = 30,      # skip if pair is older than this
    min_holder_count:    int   = 50,      # min number of token holders (GoPlus)
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

    # ── Run all checks in parallel ────────────────────────────────────────────
    sim_buy_task   = asyncio.to_thread(_simulate_buy_sync,  w3, token_address, wallet_address)
    sim_sell_task  = asyncio.to_thread(_simulate_sell_sync, w3, token_address, pair_address)
    goplus_task    = _goplus_fetch(token_address)
    honeypot_task  = _honeypot_is_check(token_address)
    dexscreen_task = _dexscreener_fetch(pair_address)

    sim_result, sell_sim, goplus_data, hp_result, dex = await asyncio.gather(
        sim_buy_task, sim_sell_task, goplus_task, honeypot_task, dexscreen_task
    )

    # ── Require GoPlus in auto mode ───────────────────────────────────────────
    # Without GoPlus we can't check LP lock, taxes, or owner flags — too risky.
    if require_goplus and goplus_data is None:
        return {"ok": False, "reason": "GoPlus недоступен — пропуск в авто-режиме (LP-проверка невозможна)"}

    # ── Track A: buy simulation ───────────────────────────────────────────────
    if not sim_result["ok"]:
        return {"ok": False, "reason": sim_result["reason"]}

    # ── Track B: sell simulation (pair-based) ─────────────────────────────────
    if not sell_sim["ok"]:
        return {"ok": False, "reason": sell_sim["reason"]}

    # ── Track C: honeypot.is ──────────────────────────────────────────────────
    if not hp_result["ok"]:
        return {"ok": False, "reason": hp_result["reason"]}

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
            "cannot_sell_all":         "Продажа всех токенов заблокирована",
            "trading_cooldown":        "Trading cooldown (anti-bot)",
            "is_anti_whale":           "Anti-whale: ограничение объёма продажи",
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

        # Minimum holder count — filters out insider/bot-only tokens
        raw_holders = goplus_data.get("holder_count")
        if raw_holders is not None:
            try:
                holder_count_int = int(raw_holders)
                if holder_count_int < min_holder_count:
                    return {
                        "ok":     False,
                        "reason": f"Холдеров слишком мало: {holder_count_int} < {min_holder_count}",
                    }
            except (ValueError, TypeError):
                pass  # can't parse — don't block

        # Top-10 combined holder concentration (excluding DEX pools, burned, locked)
        combined_top10 = 0.0
        for h in (goplus_data.get("holders") or [])[:10]:
            pct      = float(h.get("percent", 0)) * 100
            tag      = (h.get("tag") or "").lower()
            is_safe  = h.get("is_locked", 0) == 1 or any(s in tag for s in SAFE_HOLDER_TAGS)
            if not is_safe:
                combined_top10 += pct
        if combined_top10 > max_top10_holder_pct:
            return {"ok": False, "reason": f"Топ-10 холдеров держат {combined_top10:.1f}% — риск скоординированного дампа"}

        # LP holder lock check — detect rug pull potential
        # If any single unlocked wallet holds >lp_holder_max_pct% of LP, devs can rug instantly.
        lp_holders = goplus_data.get("lp_holders") or []
        if lp_holders:
            for lph in lp_holders[:10]:
                lp_pct    = float(lph.get("percent", 0)) * 100
                is_locked = lph.get("is_locked", 0) == 1
                tag       = (lph.get("tag") or "").lower()
                is_safe   = is_locked or any(s in tag for s in SAFE_HOLDER_TAGS)
                if not is_safe and lp_pct > lp_holder_max_pct:
                    return {
                        "ok":     False,
                        "reason": f"Rug риск: {lp_pct:.0f}% LP не заблокирован — девы могут слить ликвидность",
                    }
        else:
            # LP data empty → GoPlus hasn't indexed it yet
            if require_goplus:
                return {"ok": False, "reason": "LP-холдеры не проиндексированы — rug-риск неизвестен, пропуск"}
            warnings_from_goplus.append("⚠️ LP-холдеры не проиндексированы — rug-риск неизвестен")

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
        return {"ok": False, "reason": f"Ликвидность: ${liquidity_usd:,.0f} < ${min_liquidity_usd:,.0f}"}

    # ── FDV / Market cap check (on-chain) ─────────────────────────────────────
    fdv_usd = await asyncio.to_thread(_get_fdv_usd_sync, w3, token_address, bnb_price)
    if fdv_usd > 0:
        if fdv_usd < min_market_cap_usd:
            return {"ok": False, "reason": f"Market cap: ${fdv_usd:,.0f} < ${min_market_cap_usd:,.0f}"}
        if fdv_usd < min_fdv_usd:
            return {"ok": False, "reason": f"FDV: ${fdv_usd:,.0f} < ${min_fdv_usd:,.0f}"}
        if fdv_usd > max_fdv_usd:
            return {"ok": False, "reason": f"FDV: ${fdv_usd:,.0f} > ${max_fdv_usd:,.0f} (слишком крупный)"}

    # ── DexScreener: token age + 5-minute volume ──────────────────────────────
    # These checks only apply when DexScreener has indexed the pair.
    # Brand-new pairs (seconds old) are not yet indexed → checks skipped (fail-open).
    if dex:
        # Token age
        created_at_ms = dex.get("pairCreatedAt") or 0
        if created_at_ms:
            age_days = (time.time() - created_at_ms / 1000) / 86400
            if age_days > max_token_age_days:
                return {"ok": False, "reason": f"Токен слишком старый: {age_days:.0f} дней (макс {max_token_age_days})"}

        # 5-minute volume
        vol_5m = float((dex.get("volume") or {}).get("m5") or 0)
        if vol_5m > 0 and vol_5m < min_volume_5m_usd:
            return {"ok": False, "reason": f"Объём за 5 мин: ${vol_5m:,.0f} < ${min_volume_5m_usd:,.0f}"}

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
        "fdv_usd":       fdv_usd,
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
