import asyncio
import collections
import logging
import time

import aiohttp
from web3 import Web3

import blacklist
from config import WBNB, BUSD, USDT, PANCAKE_FACTORY_V2, PANCAKE_ROUTER_V2, TOP_HOLDER_MAX_PCT

log = logging.getLogger(__name__)

GOPLUS_BASE_URL  = "https://api.gopluslabs.io/api/v1/token_security"
GOPLUS_TIMEOUT   = 3.0   # increased from 1.5s — new tokens need more time to index

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

DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex/pairs"
DEXSCREENER_TIMEOUT  = 3.0

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

LP_ABI = [
    {"inputs": [],                                                    "name": "totalSupply", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}],               "name": "balanceOf",   "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
]

# keccak256("Transfer(address,address,uint256)") — standard ERC-20 Transfer topic
_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ZERO_TOPIC     = "0x0000000000000000000000000000000000000000000000000000000000000000"

# Known LP locker contract addresses on BSC (lowercase) — safe to hold large LP %
_SAFE_LP_HOLDERS = {
    "0x000000000000000000000000000000000000dead",   # burn
    "0xc765bddb93b0d1c1a88282ba0fa6b2d00e3e0c83",   # Unicrypt BSC
    "0x407993575c91ce7643a4d4ccacc9a98c36ee1bb",   # PinkSale
    "0xe2fe530c047f2d85298b07d9333c05737f1435fb",   # Team Finance
    "0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff",   # DxLock
}

# ── On-chain helpers ──────────────────────────────────────────────────────────

def _get_bnb_price_sync(
    w3: Web3,
    router_address: str = None,
    native_token: str = None,
    stable_token: str = None,
) -> float:
    router_addr  = router_address or PANCAKE_ROUTER_V2
    native_addr  = native_token   or WBNB
    stable_addr  = stable_token   or BUSD
    try:
        router  = w3.eth.contract(address=Web3.to_checksum_address(router_addr), abi=ROUTER_ABI_PRICE)
        amounts = router.functions.getAmountsOut(
            Web3.to_wei(1, "ether"),
            [Web3.to_checksum_address(native_addr), Web3.to_checksum_address(stable_addr)],
        ).call()
        # USDC/USDT use 6 decimals, BUSD uses 18
        decimals = 6 if stable_addr.lower() in (
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC Base
            "0x55d398326f99059ff775485246999027b3197955",  # USDT BSC
        ) else 18
        return amounts[1] / 10 ** decimals
    except Exception as e:
        log.warning(f"Native price fetch failed: {e}")
        return 0.0


_NATIVE_TOKENS = {
    WBNB.lower(),
    "0x4200000000000000000000000000000000000006",  # WETH Base
}


def _get_liquidity_usd_sync(
    w3: Web3, pair_address: str, base_token: str, native_price: float,
    native_token: str = None,
) -> float:
    native_lower = (native_token or WBNB).lower()
    try:
        pair     = w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=PAIR_ABI)
        reserves = pair.functions.getReserves().call()
        token0   = pair.functions.token0().call()

        base_reserve = reserves[0] if token0.lower() == base_token.lower() else reserves[1]
        base_norm    = base_reserve / 1e18

        # If paired with native token (BNB/ETH): use price oracle; stablecoin pairs = 1:1 USD
        if base_token.lower() == native_lower or base_token.lower() in _NATIVE_TOKENS:
            return base_norm * native_price * 2
        # Stablecoin (BUSD/USDT use 18 dec, USDC uses 6)
        if base_token.lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913":
            return (base_reserve / 1e6) * 2   # USDC on Base (6 decimals)
        return base_norm * 2   # BUSD/USDT (18 decimals)
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


_OWNER_ABI = [
    {"inputs": [], "name": "owner",    "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getOwner", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
]
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _get_token_owner_sync(w3: Web3, token_address: str) -> str | None:
    """
    On-chain fallback: try owner() / getOwner() from the Ownable pattern.
    Returns the owner address (lowercased), "renounced" if address(0), or None.
    """
    try:
        token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=_OWNER_ABI)
        for fn_name in ("owner", "getOwner"):
            try:
                addr = token.functions[fn_name]().call()
                addr_l = addr.lower()
                if addr_l == _ZERO_ADDRESS:
                    return "renounced"
                return addr_l
            except Exception:
                continue
    except Exception:
        pass
    return None


def _get_deployer_from_mint_sync(w3: Web3, token_address: str) -> str | None:
    """
    Find deployer via the first mint event: Transfer(from=0x0, to=deployer).
    Works for any ERC20 even when BSCScan is unindexed and owner() is absent.
    Scans last 200 000 blocks (~7 days on BSC) — always covers fresh tokens.
    """
    try:
        token_cs  = Web3.to_checksum_address(token_address)
        cur_block = w3.eth.block_number
        # Try narrow window first (fast), widen on miss
        for lookback in (5_000, 200_000):
            from_block = max(0, cur_block - lookback)
            logs = w3.eth.get_logs({
                "address":   token_cs,
                "topics":    [_TRANSFER_TOPIC, _ZERO_TOPIC],
                "fromBlock": from_block,
                "toBlock":   cur_block,
            })
            if not logs:
                continue
            first = sorted(logs, key=lambda l: (l["blockNumber"], l["logIndex"]))[0]
            topics = first.get("topics", [])
            if len(topics) < 3:
                continue
            addr = "0x" + first["topics"][2].hex()[-40:]
            if addr.lower() != _ZERO_ADDRESS:
                return addr.lower()
        return None
    except Exception as e:
        log.debug(f"Mint deployer lookup failed for {token_address}: {e}")
        return None


def _get_lp_adder_sync(w3: Web3, pair_address: str) -> str | None:
    """
    Find who added initial liquidity by scanning LP-token mint events on the PAIR contract.

    When addLiquidity() is called, PancakeSwap V2 pair emits:
      Transfer(from=0x0, to=<LP adder>, value=<LP amount>)
    This is 100% on-chain data — available immediately, no external API needed.

    Returns lowercased address of the first LP recipient, or None.
    """
    try:
        pair_cs = Web3.to_checksum_address(pair_address)
        cur_block = w3.eth.block_number
        for lookback in (5_000, 200_000):
            from_block = max(0, cur_block - lookback)
            logs = w3.eth.get_logs({
                "address":   pair_cs,
                "topics":    [_TRANSFER_TOPIC, _ZERO_TOPIC],  # LP mint = Transfer from 0x0
                "fromBlock": from_block,
                "toBlock":   cur_block,
            })
            if not logs:
                continue
            for log_entry in sorted(logs, key=lambda l: (l["blockNumber"], l["logIndex"])):
                topics = log_entry.get("topics", [])
                if len(topics) < 3:
                    continue
                addr_l = ("0x" + log_entry["topics"][2].hex()[-40:]).lower()
                if addr_l != _ZERO_ADDRESS:
                    log.debug(f"[LPAdderFallback] {pair_address[:10]}… LP adder via pair mint: {addr_l[:10]}…")
                    return addr_l
        return None
    except Exception as e:
        log.debug(f"LP adder lookup failed for {pair_address}: {e}")
        return None


def _get_deployer_holdings_sync(w3: Web3, token_address: str, deployer_address: str) -> dict:
    """Return deployer wallet balance as % of total token supply."""
    try:
        token        = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
        total_supply = token.functions.totalSupply().call()
        if total_supply == 0:
            return {"pct": 0.0}
        deployer_cs  = Web3.to_checksum_address(deployer_address)
        balance      = token.functions.balanceOf(deployer_cs).call()
        pct          = balance / total_supply * 100
        return {"pct": round(pct, 1)}
    except Exception:
        return {"pct": None}


def _get_deployer_lp_pct_sync(w3: Web3, pair_address: str, deployer_address: str) -> dict:
    """
    Return deployer's LP token balance as % of total LP supply.
    Also checks if LP is held by a known locker contract.

    Returns:
      {"pct": float, "locked": bool, "locker": str|None}
      {"pct": None}  on error
    """
    try:
        pair_cs  = Web3.to_checksum_address(pair_address)
        lp       = w3.eth.contract(address=pair_cs, abi=LP_ABI)
        total    = lp.functions.totalSupply().call()
        if total == 0:
            return {"pct": 0.0, "locked": None, "locker": None}
        dep_cs  = Web3.to_checksum_address(deployer_address)
        dep_bal = lp.functions.balanceOf(dep_cs).call()
        dep_pct = dep_bal / total * 100

        # Check what fraction is in known lockers
        locked_pct = 0.0
        locker_name = None
        _LOCKER_NAMES = {
            "0xc765bddb93b0d1c1a88282ba0fa6b2d00e3e0c83": "Unicrypt",
            "0x407993575c91ce7643a4d4ccacc9a98c36ee1bb": "PinkSale",
            "0xe2fe530c047f2d85298b07d9333c05737f1435fb": "Team Finance",
            "0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff": "DxLock",
            "0x000000000000000000000000000000000000dead": "Burned",
        }
        for addr, name in _LOCKER_NAMES.items():
            try:
                bal = lp.functions.balanceOf(Web3.to_checksum_address(addr)).call()
                pct = bal / total * 100
                if pct > 1.0:
                    locked_pct += pct
                    locker_name = name
            except Exception:
                continue

        return {
            "pct":    round(dep_pct, 1),
            "locked_pct": round(locked_pct, 1),
            "locker": locker_name,
            "locked": locked_pct >= 80.0,
        }
    except Exception:
        return {"pct": None}


def _simulate_sell_sync(w3: Web3, token_address: str, pair_address: str,
                        router_address: str = None) -> dict:
    """
    Simulate a sell by transferring tokens TO the pair/router from non-whitelisted addresses.

    Three checks in sequence:
      1. transfer(pair, 1) from pair     — catches "if to==pair → revert" pattern
      2. transfer(pair, 1) from addr(1)  — catches "whitelist-only" pattern where pair is exempt
         (addr(1) has no tokens; we filter out "insufficient balance" errors so only true honeypot
          reverts — which check sender before balance — are flagged)
      3. transfer(router, 1) from pair   — catches "if to==router → revert" pattern
    """
    # Errors that indicate a legitimate balance/gas issue — NOT a honeypot block
    _BALANCE_ERRORS = (
        "transfer amount exceeds balance", "exceeds balance",
        "insufficient balance", "amount exceeds", "balance exceeded",
        "insufficient funds", "gas required", "out of gas",
    )

    try:
        token_cs = Web3.to_checksum_address(token_address)
        pair_cs  = Web3.to_checksum_address(pair_address)
        token    = w3.eth.contract(address=token_cs, abi=TRANSFER_ABI)

        pair_balance = token.functions.balanceOf(pair_cs).call()
        if pair_balance == 0:
            # Pair has no tokens yet — deployer hasn't added liquidity.
            # We cannot simulate; mark as skipped so callers can warn the user.
            return {"ok": True, "skipped": True,
                    "reason": "Пул пустой при анализе — sell-симуляция не выполнена, проверка будет при покупке"}

        # ── Check 1: transfer TO pair FROM pair ──────────────────────────────
        # Catches honeypots that check: if (recipient == pair) revert
        try:
            token.functions.transfer(pair_cs, 1).call({"from": pair_cs})
        except Exception as e:
            err = str(e).lower()
            if "execution reverted" in err and not any(x in err for x in _BALANCE_ERRORS):
                log.info(f"Sell sim [to=pair, from=pair] reverted for {token_address}: {e}")
                return {"ok": False, "reason": "Симуляция продажи: контракт блокирует продажу — вероятно honeypot"}

        # ── Check 2: transfer TO pair FROM address(1) (not whitelisted) ─────
        # Many honeypots whitelist the pair address but block everyone else.
        # address(1) has 0 token balance; if the contract checks the anti-sell
        # condition BEFORE the balance check (common pattern), it will revert
        # with a honeypot-specific message, not a balance error.
        ADDR1 = "0x0000000000000000000000000000000000000001"
        try:
            token.functions.transfer(pair_cs, 1).call({"from": ADDR1})
        except Exception as e:
            err = str(e).lower()
            if "execution reverted" in err and not any(x in err for x in _BALANCE_ERRORS):
                log.info(f"Sell sim [to=pair, from=addr1] reverted for {token_address}: {e}")
                return {"ok": False, "reason": "Симуляция продажи: контракт блокирует продажу для обычных адресов — вероятно honeypot"}

        # ── Check 3: transfer TO router FROM pair ────────────────────────────
        # Some honeypots check: if (recipient == router) revert
        if router_address:
            router_cs = Web3.to_checksum_address(router_address)
            try:
                token.functions.transfer(router_cs, 1).call({"from": pair_cs})
            except Exception as e:
                err = str(e).lower()
                if "execution reverted" in err and not any(x in err for x in _BALANCE_ERRORS):
                    log.info(f"Sell sim [to=router, from=pair] reverted for {token_address}: {e}")
                    return {"ok": False, "reason": "Симуляция продажи: контракт блокирует продажу через роутер — вероятно honeypot"}

        return {"ok": True}

    except Exception as e:
        log.debug(f"simulate_sell non-blocking error ({token_address}): {e}")
        return {"ok": True}


def _simulate_buy_sync(
    w3: Web3,
    token_address: str,
    wallet_address: str,
    router_address: str = None,
    native_token: str = None,
) -> dict:
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
        wbnb_cs   = Web3.to_checksum_address(native_token or WBNB)
        router_cs = Web3.to_checksum_address(router_address or PANCAKE_ROUTER_V2)
        sim_wei   = Web3.to_wei(0.005, "ether")
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


def _check_lp_onchain_sync(
    w3: Web3, pair_address: str, lp_holder_max_pct: float
) -> dict:
    """
    On-chain LP holder concentration check — works without GoPlus.

    Method:
      1. Get LP totalSupply (pair contract = LP token)
      2. Scan Transfer-from-zero (mint) events in the last 1000 blocks
         to find who received LP tokens initially
      3. Check each recipient's current LP balance
      4. Reject if any non-safe address holds > lp_holder_max_pct% of supply

    For freshly created pairs this is highly accurate:
    - Unlocker LP stays with the deployer → caught immediately
    - Locked LP is in a locker contract → in _SAFE_LP_HOLDERS or balance < threshold

    Returns {"ok": True} or {"ok": False, "reason": "..."}
    Fails open on any RPC error to avoid blocking good tokens.
    """
    try:
        pair_cs  = Web3.to_checksum_address(pair_address)
        lp       = w3.eth.contract(address=pair_cs, abi=LP_ABI)
        total    = lp.functions.totalSupply().call()

        if total == 0:
            return {"ok": True}  # liquidity not added yet — nothing to judge

        current_block = w3.eth.block_number
        from_block    = max(0, current_block - 1000)  # ~50 min of blocks

        logs = w3.eth.get_logs({
            "address":   pair_cs,
            "topics":    [_TRANSFER_TOPIC, _ZERO_TOPIC],  # mint = Transfer from 0x0
            "fromBlock": from_block,
            "toBlock":   current_block,
        })

        seen: set[str] = set()
        for log_entry in logs:
            topics = log_entry.get("topics", [])
            if len(topics) < 3:
                continue
            to_raw = topics[2]
            holder = Web3.to_checksum_address("0x" + to_raw.hex()[-40:])
            addr_l = holder.lower()

            if addr_l in seen or addr_l in _SAFE_LP_HOLDERS:
                continue
            seen.add(addr_l)

            balance = lp.functions.balanceOf(holder).call()
            pct     = balance / total * 100

            if pct > lp_holder_max_pct:
                short = holder[:6] + "…" + holder[-4:]
                return {
                    "ok":     False,
                    "reason": (
                        f"Rug риск: {pct:.0f}% LP в одном кошельке ({short}) — "
                        f"ликвидность не заблокирована"
                    ),
                }

        return {"ok": True}

    except Exception as e:
        log.debug(f"LP on-chain check failed for {pair_address}: {e}")
        return {"ok": True}  # fail-open — don't block on RPC errors


# ── GoPlus fetch with short timeout ──────────────────────────────────────────

async def _honeypot_is_check(
    token_address: str,
    max_buy_tax: float = 999.0,
    max_sell_tax: float = 999.0,
    chain_id: int = 56,
) -> dict:
    """
    Check honeypot.is — simulates BOTH buy and sell transactions on-chain.
    This is the most reliable sell-honeypot + tax detector for brand-new tokens.

    When max_buy_tax / max_sell_tax are passed, the function also enforces tax
    thresholds using the simulation result — works BEFORE GoPlus indexes the token.

    Returns {"ok": False, "reason": "..."}   if honeypot or tax too high.
    Returns {"ok": True, "buy_tax": float, "sell_tax": float}  if safe.
    Returns {"ok": True, "buy_tax": None, "sell_tax": None}    if API unavailable (fail-open).
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                HONEYPOT_IS_URL,
                params={"address": token_address, "chainID": str(chain_id)},
                timeout=aiohttp.ClientTimeout(total=HONEYPOT_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return {"ok": True, "buy_tax": None, "sell_tax": None}
                body = await resp.json(content_type=None)

        hp = body.get("honeypotResult", {}) or {}
        if hp.get("isHoneypot"):
            reason = hp.get("honeypotReason") or "honeypot.is"
            return {"ok": False, "reason": f"Honeypot обнаружен: {reason}"}

        sim = body.get("simulationResult", {}) or {}
        buy_tax  = float(sim.get("buyTax",  0) or 0)
        sell_tax = float(sim.get("sellTax", 0) or 0)

        # Hard honeypot threshold (very high tax = sell is effectively blocked)
        if sell_tax > 49:
            return {"ok": False, "reason": f"Sell tax {sell_tax:.0f}% — фактически honeypot"}

        # Enforce normal tax thresholds from simulation (works instantly, no GoPlus needed)
        if buy_tax > max_buy_tax:
            return {"ok": False, "reason": f"Buy tax {buy_tax:.1f}% (симуляция honeypot.is) > {max_buy_tax:.0f}%"}
        if sell_tax > max_sell_tax:
            return {"ok": False, "reason": f"Sell tax {sell_tax:.1f}% (симуляция honeypot.is) > {max_sell_tax:.0f}%"}

        return {"ok": True, "buy_tax": buy_tax, "sell_tax": sell_tax}

    except asyncio.TimeoutError:
        log.info(f"honeypot.is timeout for {token_address} — proceeding")
        return {"ok": True, "buy_tax": None, "sell_tax": None}
    except Exception as e:
        log.warning(f"honeypot.is error: {e}")
        return {"ok": True, "buy_tax": None, "sell_tax": None}


BSCSCAN_API_URL  = "https://api.bscscan.com/api"
BASESCAN_API_URL = "https://api.basescan.org/api"
EXPLORER_TIMEOUT = 5.0
BSCSCAN_TIMEOUT  = EXPLORER_TIMEOUT   # backwards compat alias


async def _check_deployer_bscscan(
    token_address: str,
    api_key: str,
    max_deploy_count_30d: int = 3,
    explorer_url: str = None,
) -> dict:
    """
    Check token deployer history via block explorer API (BSCScan or Basescan).
    Identical API interface — just pass explorer_url=BASESCAN_API_URL for Base.

    Steps:
      1. Resolve token deployer via getcontractcreation
      2. Scan deployer's last 100 transactions for contract deployments in 30 days
      3. Reject if deployer has created too many contracts recently (serial scammer pattern)

    Returns:
      {"ok": True,  "deployer": addr, "deploy_count_30d": n} — safe or unknown
      {"ok": False, "reason": "..."}                         — serial deployer detected
      {"ok": True,  "deployer": None}                        — API key missing or error (fail-open)
    """
    if not api_key:
        return {"ok": True, "deployer": None}

    api_url = explorer_url or BSCSCAN_API_URL

    try:
        async with aiohttp.ClientSession() as session:
            # ── Step 1: get contract creator ──────────────────────────────────
            async with session.get(
                api_url,
                params={
                    "module":            "contract",
                    "action":            "getcontractcreation",
                    "contractaddresses": token_address,
                    "apikey":            api_key,
                },
                timeout=aiohttp.ClientTimeout(total=EXPLORER_TIMEOUT),
            ) as resp:
                body = await resp.json(content_type=None)

            if body.get("status") != "1" or not body.get("result"):
                return {"ok": True, "deployer": None}

            deployer = body["result"][0].get("contractCreator", "").lower()
            if not deployer:
                return {"ok": True, "deployer": None}

            # ── Blacklist check (instant, no extra API call) ──────────────────
            if blacklist.is_blacklisted(deployer):
                entry = blacklist.get_all().get(deployer, {})
                reason = entry.get("reason") or "ранее помечен как скам"
                hits   = entry.get("hits", 1)
                return {
                    "ok":      False,
                    "reason":  f"Деплоер в чёрном списке ({hits}x): {reason}",
                    "deployer": deployer,
                }

            # ── Step 2: scan deployer tx history ─────────────────────────────
            cutoff = int(time.time()) - 30 * 86400
            async with session.get(
                api_url,
                params={
                    "module":  "account",
                    "action":  "txlist",
                    "address": deployer,
                    "sort":    "desc",
                    "page":    "1",
                    "offset":  "100",
                    "apikey":  api_key,
                },
                timeout=aiohttp.ClientTimeout(total=EXPLORER_TIMEOUT),
            ) as resp:
                body2 = await resp.json(content_type=None)

        txs = body2.get("result") or []
        if isinstance(txs, str):  # explorer API error string
            return {"ok": True, "deployer": deployer}

        # Count contract deployments (to == "") in the last 30 days
        deploy_count_30d = sum(
            1 for tx in txs
            if tx.get("to", "") == "" and int(tx.get("timeStamp", 0)) >= cutoff
        )

        result_info: dict = {
            "ok":               True,
            "deployer":         deployer,
            "deploy_count_30d": deploy_count_30d,
        }

        if deploy_count_30d > max_deploy_count_30d:
            result_info["ok"]     = False
            result_info["reason"] = (
                f"Серийный деплоер: {deploy_count_30d} контрактов за 30 дней — "
                f"высокий риск скама"
            )

        return result_info

    except asyncio.TimeoutError:
        log.info(f"BSCScan timeout for {token_address} — skipping deployer check")
        return {"ok": True, "deployer": None}
    except Exception as e:
        log.warning(f"BSCScan deployer check error: {e}")
        return {"ok": True, "deployer": None}


_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

COLLUSION_CHECK_TIMEOUT = 3.0   # per BSCScan call for funding check


async def _check_buyer_collusion(
    pair_address:     str,
    w3:               Web3,
    from_block:       int,
    to_block:         int,
    deployer_address: str | None = None,
    bscscan_api_key:  str = "",
    explorer_url:     str = "",
    max_check:        int = 10,
) -> dict:
    """
    Detect coordinated buyers (wash trading / rug setup) on a freshly listed pair.

    Checks (in order, fail-fast):
      1. Any single wallet made 3+ buys, or ≥60% of buys → wash trading
      2. Deployer wallet is among the buyers → self-trading
      3. BSCScan: ≥50% of unique buyers' first BNB came from the deployer → funded ring

    Returns:
      {"suspicious": False}
      {"suspicious": True, "label": "🚨 ...", "reason": "wash_trading|deployer_buying|funded_ring"}
    """
    try:
        logs = await asyncio.to_thread(
            w3.eth.get_logs,
            {
                "address":   Web3.to_checksum_address(pair_address),
                "topics":    [_SWAP_TOPIC],
                "fromBlock": from_block,
                "toBlock":   to_block,
            },
        )
    except Exception as e:
        log.debug(f"[Collusion] get_logs error: {e}")
        return {"suspicious": False}

    if not logs:
        return {"suspicious": False}

    tx_hashes = [log["transactionHash"].hex() for log in logs[:max_check]]

    async def _get_sender(tx_hash: str) -> str | None:
        try:
            tx = await asyncio.to_thread(w3.eth.get_transaction, tx_hash)
            return tx["from"].lower() if tx else None
        except Exception:
            return None

    senders_raw = await asyncio.gather(*[_get_sender(h) for h in tx_hashes])
    senders = [s for s in senders_raw if s]
    if not senders:
        return {"suspicious": False}

    # ── Check 1: duplicate senders ───────────────────────────────────────────
    counts = collections.Counter(senders)
    top_addr, top_count = counts.most_common(1)[0]
    if top_count >= 3 or (len(senders) >= 3 and top_count / len(senders) >= 0.6):
        short = top_addr[:6] + "…" + top_addr[-4:]
        return {
            "suspicious": True,
            "reason":     "wash_trading",
            "label":      f"🚨 Wash trading: один кошелёк купил {top_count}/{len(senders)} раз ({short})",
        }

    # ── Check 2: deployer is buying their own token ──────────────────────────
    if deployer_address:
        dep = deployer_address.lower()
        dep_count = senders.count(dep)
        if dep_count >= 1:
            short_dep = dep[:6] + "…" + dep[-4:]
            return {
                "suspicious": True,
                "reason":     "deployer_buying",
                "label":      f"🚨 Деплоер сам покупает токен ({dep_count}x) — вероятный wash trade ({short_dep})",
            }

    # ── Check 3: BSCScan — buyers funded by deployer ─────────────────────────
    if bscscan_api_key and deployer_address:
        api_url   = explorer_url or BSCSCAN_API_URL
        dep_lower = deployer_address.lower()
        unique_buyers = list(dict.fromkeys(senders))[:5]   # preserve order, deduplicate

        async def _funded_by_deployer(addr: str) -> bool:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        api_url,
                        params={
                            "module":  "account",
                            "action":  "txlist",
                            "address": addr,
                            "sort":    "asc",
                            "page":    "1",
                            "offset":  "5",
                            "apikey":  bscscan_api_key,
                        },
                        timeout=aiohttp.ClientTimeout(total=COLLUSION_CHECK_TIMEOUT),
                    ) as resp:
                        data = await resp.json(content_type=None)
                        for tx in data.get("result") or []:
                            if (tx.get("from", "").lower() == dep_lower
                                    and int(tx.get("value", "0")) > 0):
                                return True
                return False
            except Exception:
                return False

        funded = await asyncio.gather(*[_funded_by_deployer(a) for a in unique_buyers])
        funded_count = sum(1 for f in funded if f)
        if funded_count >= 2 and funded_count / len(unique_buyers) >= 0.5:
            return {
                "suspicious": True,
                "reason":     "funded_ring",
                "label":      (
                    f"🚨 {funded_count}/{len(unique_buyers)} покупателей "
                    f"финансированы деплоером — схема раг пула"
                ),
            }

    return {"suspicious": False}


async def _dexscreener_fetch(pair_address: str, chain: str = "bsc") -> dict | None:
    """
    Fetch DexScreener data for a pair.
    Returns the pair dict or None if not indexed yet (brand-new pairs).
    Fields used: volume.m5, fdv, marketCap, pairCreatedAt, liquidity.usd
    """
    try:
        url = f"{DEXSCREENER_BASE_URL}/{chain}/{pair_address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
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


def _get_fdv_usd_sync(
    w3: Web3,
    token_address: str,
    native_price: float,
    router_address: str = None,
    native_token: str = None,
) -> float:
    """
    Calculate FDV = totalSupply * pricePerToken_USD.
    Uses on-chain totalSupply + router getAmountsOut for price.
    Returns 0.0 on any error (fail-open).
    """
    try:
        token_cs  = Web3.to_checksum_address(token_address)
        native_cs = Web3.to_checksum_address(native_token or WBNB)
        token     = w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        decimals         = token.functions.decimals().call()
        total_supply_raw = token.functions.totalSupply().call()
        total_supply     = total_supply_raw / (10 ** decimals)

        router    = w3.eth.contract(
            address=Web3.to_checksum_address(router_address or PANCAKE_ROUTER_V2),
            abi=ROUTER_ABI_PRICE,
        )
        amounts   = router.functions.getAmountsOut(10 ** decimals, [token_cs, native_cs]).call()
        price_native = amounts[1] / 1e18

        return total_supply * price_native * native_price
    except Exception as e:
        log.debug(f"FDV calc failed for {token_address}: {e}")
        return 0.0


async def _goplus_fetch(token_address: str, chain_id: int = 56) -> dict | None:
    """
    Fetch GoPlus data for the given chain.
    Returns None if token not indexed yet or if request times out.
    """
    try:
        url = f"{GOPLUS_BASE_URL}/{chain_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
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

async def get_bnb_price(
    w3: Web3,
    router_address: str = None,
    native_token: str = None,
    stable_token: str = None,
) -> float:
    return await asyncio.to_thread(
        _get_bnb_price_sync, w3, router_address, native_token, stable_token
    )


def _find_pair_sync(
    w3: Web3,
    token_address: str,
    factory_address: str = None,
    base_tokens: list = None,
) -> tuple[str, str] | None:
    """
    Find the best DEX V2 pair for a token.
    Returns (pair_address, base_token) or None if not found.
    """
    ZERO = "0x0000000000000000000000000000000000000000"
    factory = w3.eth.contract(
        address=Web3.to_checksum_address(factory_address or PANCAKE_FACTORY_V2),
        abi=FACTORY_ABI,
    )
    candidates = base_tokens if base_tokens else [WBNB, BUSD, USDT]
    for base in candidates:
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


async def analyze_token(
    token_address: str,
    w3: Web3,
    wallet_address: str = "0x0000000000000000000000000000000000000001",
    bscscan_api_key: str = "",
    chain_id: int = 56,
    router_address: str = None,
    factory_address: str = None,
    native_token: str = None,
    stable_token: str = None,
    base_tokens: list = None,
    dex_chain: str = "bsc",
    explorer_url: str = None,   # override block explorer (default = BSCScan)
) -> dict:
    """
    Full non-rejecting analysis of a token — collects all available metrics
    without early return on failure. Used by the /analyze command.

    Returns a dict with all metrics + per-check pass/fail flags.
    """
    # ── Find pair ─────────────────────────────────────────────────────────────
    pair_info = await asyncio.to_thread(
        _find_pair_sync, w3, token_address, factory_address, base_tokens
    )
    if not pair_info:
        return {"found": False, "reason": "Пара не найдена на DEX"}

    pair_address, base_token = pair_info

    # ── Run all checks in parallel ─────────────────────────────────────────────
    sim_buy_task   = asyncio.to_thread(
        _simulate_buy_sync, w3, token_address, wallet_address, router_address, native_token
    )
    sim_sell_task  = asyncio.to_thread(_simulate_sell_sync, w3, token_address, pair_address)
    goplus_task    = _goplus_fetch(token_address, chain_id)
    honeypot_task  = _honeypot_is_check(token_address, chain_id=chain_id)
    dexscreen_task = _dexscreener_fetch(pair_address, dex_chain)
    price_task     = asyncio.to_thread(
        _get_bnb_price_sync, w3, router_address, native_token, stable_token
    )
    deployer_task  = _check_deployer_bscscan(token_address, bscscan_api_key, explorer_url=explorer_url)

    sim_result, sell_sim, goplus_data, hp_result, dex, bnb_price, deployer_result = await asyncio.gather(
        sim_buy_task, sim_sell_task, goplus_task, honeypot_task, dexscreen_task,
        price_task, deployer_task
    )

    # ── On-chain metrics ──────────────────────────────────────────────────────
    liquidity_usd, fdv_usd, lp_onchain = await asyncio.gather(
        asyncio.to_thread(_get_liquidity_usd_sync, w3, pair_address, base_token, bnb_price, native_token),
        asyncio.to_thread(_get_fdv_usd_sync, w3, token_address, bnb_price, router_address, native_token),
        asyncio.to_thread(_check_lp_onchain_sync, w3, pair_address, 95.0),  # 95% threshold for display only
    )

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

    # ── Deployer address: BSCScan → owner() → first mint fallback ───────────────
    _deployer_addr = deployer_result.get("deployer")
    _deployer_renounced = False
    if not _deployer_addr:
        _owner_fb = await asyncio.to_thread(_get_token_owner_sync, w3, token_address)
        if _owner_fb == "renounced":
            _deployer_renounced = True
        elif _owner_fb:
            _deployer_addr = _owner_fb
            log.info(f"[OwnerFallback] {token_address[:10]}… deployer via owner(): {_deployer_addr[:10]}…")
    if not _deployer_addr:  # also run when renounced — original deployer may still hold LP
        _mint_fb = await asyncio.to_thread(_get_deployer_from_mint_sync, w3, token_address)
        if _mint_fb:
            _deployer_addr = _mint_fb
            log.info(f"[MintFallback] {token_address[:10]}… deployer via first mint: {_deployer_addr[:10]}…")
    if not _deployer_addr:  # LP mint scan on pair contract — most reliable for fresh tokens
        _lp_adder_fb = await asyncio.to_thread(_get_lp_adder_sync, w3, pair_address)
        if _lp_adder_fb:
            _deployer_addr = _lp_adder_fb
            log.info(f"[LPAdderFallback] {token_address[:10]}… deployer via LP mint: {_deployer_addr[:10]}…")

    deployer_pct: float | None = None
    deployer_lp: dict = {}
    if _deployer_addr:
        _dh, _dlp = await asyncio.gather(
            asyncio.to_thread(_get_deployer_holdings_sync, w3, token_address, _deployer_addr),
            asyncio.to_thread(_get_deployer_lp_pct_sync, w3, pair_address, _deployer_addr),
        )
        deployer_pct = _dh.get("pct")
        deployer_lp  = _dlp

    # ── Quality scoring for /analyze display ──────────────────────────────────
    _exp_url_a  = explorer_url or ""
    # get social from dex variable (already fetched)
    _dex_a = locals().get("dex")
    _social_a = bool(
        _dex_a and (
            (_dex_a.get("info") or {}).get("socials") or
            (_dex_a.get("info") or {}).get("websites")
        )
    )
    _pair_age_min_a: float | None = None
    if _dex_a and _dex_a.get("pairCreatedAt"):
        _pair_age_min_a = (time.time() * 1000 - _dex_a["pairCreatedAt"]) / 60000
    _contract_verified_a, _deployer_age_days_a = await asyncio.gather(
        _check_contract_verified(token_address, bscscan_api_key, _exp_url_a),
        _get_deployer_wallet_age(_deployer_addr, bscscan_api_key, _exp_url_a) if _deployer_addr else _null_async(),
    )
    _score_a = _calculate_token_score(
        lp_locked_pct          = deployer_lp.get("locked_pct"),
        lp_locker               = deployer_lp.get("locker"),
        contract_verified       = _contract_verified_a,
        social_present          = _social_a,
        deployer_wallet_age_days= _deployer_age_days_a,
        pair_age_minutes        = _pair_age_min_a,
        buyer_count             = holder_count,
    )

    # ── honeypot.is taxes (works before GoPlus indexes the token) ────────────
    hp_buy_tax  = hp_result.get("buy_tax")   # None if API unavailable
    hp_sell_tax = hp_result.get("sell_tax")

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

        # LP lock — try GoPlus first, then on-chain fallback
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
        else:
            # GoPlus hasn't indexed LP holders yet — use on-chain result
            if lp_onchain.get("ok") is False:
                lp_locked = False   # high concentration detected on-chain
            # lp_locked stays None only if on-chain returned ok=True (can't distinguish locked vs unlocked)

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
        "found":              True,
        "token_address":      token_address,
        "pair_address":       pair_address,
        "base_token":         base_token,
        "name":               name,
        "symbol":             symbol,
        "holder_count":       holder_count,
        "bnb_price":          bnb_price,
        "liquidity_usd":      liquidity_usd,
        "fdv_usd":            fdv_usd,
        # GoPlus taxes (available after ~15-30 min of listing)
        "buy_tax":            buy_tax,
        "sell_tax":           sell_tax,
        # honeypot.is simulation taxes (available immediately at listing)
        "hp_buy_tax":         hp_buy_tax,
        "hp_sell_tax":        hp_sell_tax,
        "sim_buy_ok":         sim_result["ok"],
        "sim_buy_reason":     sim_result.get("reason", ""),
        "sim_sell_ok":        False if sell_sim.get("skipped") else sell_sim["ok"],
        "sim_sell_skipped":   sell_sim.get("skipped", False),
        "sim_sell_reason":    sell_sim.get("reason", ""),
        "hp_is_ok":           hp_result["ok"],
        "hp_is_reason":       hp_result.get("reason", ""),
        "goplus_ok":          goplus_data is not None,
        "critical_flags":     critical_flags,
        "warnings":           warnings,
        "top10_pct":          top10_pct,
        "lp_locked":          lp_locked,
        "age_days":           age_days,
        "vol_5m":             vol_5m,
        # Deployer (BSCScan or on-chain owner() fallback)
        "deployer":              _deployer_addr,
        "deployer_renounced":    _deployer_renounced,
        "deploy_count_30d":      deployer_result.get("deploy_count_30d"),
        "deployer_ok":           deployer_result["ok"],
        "deployer_reason":       deployer_result.get("reason", ""),
        "deployer_pct":          deployer_pct,
        "deployer_lp_pct":       deployer_lp.get("pct"),
        "deployer_lp_locked_pct": deployer_lp.get("locked_pct"),
        "deployer_lp_locker":    deployer_lp.get("locker"),
        # LP on-chain check result (for display in /analyze)
        "lp_onchain_ok":         lp_onchain.get("ok", True),
        "lp_onchain_reason":     lp_onchain.get("reason", ""),
        "token_score":           _score_a,
        "contract_verified":     _contract_verified_a,
        "social_present":        _social_a,
        "deployer_age_days":     _deployer_age_days_a,
        "pair_age_min":          _pair_age_min_a,
    }


# ── Quality scoring helpers ───────────────────────────────────────────────────

async def _check_contract_verified(
    token_address: str, bscscan_api_key: str, explorer_url: str = ""
) -> bool | None:
    """Returns True=verified, False=not verified, None=API unavailable."""
    if not bscscan_api_key:
        return None
    base = explorer_url or "https://api.bscscan.com/api"
    url = (
        f"{base}?module=contract&action=getsourcecode"
        f"&address={token_address}&apikey={bscscan_api_key}"
    )
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3.0)) as s:
            async with s.get(url) as resp:
                data = await resp.json(content_type=None)
        if data.get("status") == "1":
            abi = (data.get("result") or [{}])[0].get("ABI", "")
            return bool(abi and abi != "Contract source code not verified")
    except Exception:
        pass
    return None


async def _get_deployer_wallet_age(
    deployer_address: str, bscscan_api_key: str, explorer_url: str = ""
) -> float | None:
    """Returns deployer wallet age in days (since first tx), or None."""
    if not bscscan_api_key or not deployer_address:
        return None
    base = explorer_url or "https://api.bscscan.com/api"
    url = (
        f"{base}?module=account&action=txlist"
        f"&address={deployer_address}"
        f"&startblock=0&endblock=99999999&page=1&offset=1&sort=asc"
        f"&apikey={bscscan_api_key}"
    )
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3.0)) as s:
            async with s.get(url) as resp:
                data = await resp.json(content_type=None)
        if data.get("status") == "1" and data.get("result"):
            first_ts = int(data["result"][0]["timeStamp"])
            return (time.time() - first_ts) / 86400
    except Exception:
        pass
    return None


async def _null_async() -> None:
    """Placeholder coroutine for asyncio.gather when a task is optional."""
    return None


def _calculate_token_score(
    lp_locked_pct:         float | None,
    lp_locker:             str   | None,
    contract_verified:     bool  | None,
    social_present:        bool  | None,
    deployer_wallet_age_days: float | None,
    pair_age_minutes:      float | None,
    buyer_count,
    min_lp_lock_pct:       float = 80.0,
    min_deployer_age_days: float = 30.0,
    min_listing_age_min:   float = 0.0,
    min_buyers_score:      int   = 10,
) -> dict:
    """
    Quality score 0-10 for a token.
    LP lock (3) + verified (2) + social (2) + deployer age (1) + listing age (1) + buyers (1).
    """
    score = 0
    factors = []

    # ── 1. LP lock (3 pts) ────────────────────────────────────────────────────
    lp_pct = lp_locked_pct if lp_locked_pct is not None else 0.0
    if lp_locked_pct is not None:
        locker_str = f" в {lp_locker}" if lp_locker else ""
        if lp_pct >= min_lp_lock_pct:
            score += 3
            factors.append(f"✅ LP заблокирована {lp_pct:.0f}%{locker_str}: +3")
        elif lp_pct >= 50:
            score += 1
            factors.append(f"⚠️ LP частично {lp_pct:.0f}%{locker_str}: +1 (нужно ≥{min_lp_lock_pct:.0f}%)")
        else:
            factors.append(f"❌ LP заблокирована лишь {lp_pct:.0f}%: +0 (rug risk)")
    else:
        factors.append("❌ LP не заблокирована / неизвестно: +0")

    # ── 2. Contract verified (2 pts) ─────────────────────────────────────────
    if contract_verified is True:
        score += 2
        factors.append("✅ Контракт верифицирован на BSCScan: +2")
    elif contract_verified is False:
        factors.append("❌ Контракт НЕ верифицирован: +0")
    else:
        factors.append("❓ Верификация контракта: неизвестно: +0")

    # ── 3. Social presence (2 pts) ────────────────────────────────────────────
    if social_present is True:
        score += 2
        factors.append("✅ Социальные сети / сайт найдены: +2")
    elif social_present is False:
        factors.append("❌ Нет сайта/Twitter/Telegram: +0")
    else:
        factors.append("❓ Социальные сети: не проверено: +0")

    # ── 4. Deployer wallet age (1 pt) ─────────────────────────────────────────
    if deployer_wallet_age_days is not None:
        if deployer_wallet_age_days >= min_deployer_age_days:
            score += 1
            factors.append(f"✅ Деплоер активен {deployer_wallet_age_days:.0f} дн.: +1")
        else:
            factors.append(f"⚠️ Деплоер новый кошелёк ({deployer_wallet_age_days:.1f} дн.): +0")
    else:
        factors.append("❓ Возраст кошелька деплоера: неизвестно: +0")

    # ── 5. Listing age (1 pt) ─────────────────────────────────────────────────
    if min_listing_age_min <= 0:
        score += 1
        factors.append("✅ Возраст листинга: проверка отключена: +1")
    elif pair_age_minutes is not None:
        if pair_age_minutes >= min_listing_age_min:
            score += 1
            factors.append(f"✅ Листинг {pair_age_minutes:.1f} мин. назад: +1")
        else:
            factors.append(f"⚠️ Листинг {pair_age_minutes:.1f} мин. (мин. {min_listing_age_min:.0f}): +0")
    else:
        factors.append("❓ Возраст листинга: неизвестно: +0")

    # ── 6. Buyer count (1 pt) ─────────────────────────────────────────────────
    if min_buyers_score <= 0:
        score += 1
        factors.append("✅ Покупатели: проверка отключена: +1")
    else:
        bc = None
        if buyer_count is not None and buyer_count != "?":
            try:
                bc = int(buyer_count)
            except (ValueError, TypeError):
                pass
        if bc is not None:
            if bc >= min_buyers_score:
                score += 1
                factors.append(f"✅ {bc} покупателей: +1")
            else:
                factors.append(f"⚠️ Только {bc} покупателей (мин. {min_buyers_score}): +0")
        else:
            factors.append(f"❓ Покупателей: неизвестно: +0")

    grade = "🟢" if score >= 8 else ("🟡" if score >= 5 else "🔴")
    return {
        "score":     score,
        "max_score": 10,
        "grade":     grade,
        "pct":       score * 10,
        "factors":   factors,
    }


# ── Main security check ───────────────────────────────────────────────────────

async def check_token(
    token_address: str,
    pair_address:  str,
    base_token:    str,
    w3:            Web3,
    min_liquidity_usd:    float,
    max_buy_tax:          float,
    max_sell_tax:         float,
    wallet_address:       str   = "0x0000000000000000000000000000000000000001",
    lp_holder_max_pct:       float = 30.0,
    max_deployer_lp_pct:     float = 30.0,
    min_market_cap_usd:      float = 30_000,
    min_fdv_usd:          float = 200_000,
    max_fdv_usd:          float = 10_000_000,
    max_top10_holder_pct: float = 30.0,
    min_volume_5m_usd:    float = 1_000,
    max_token_age_days:   int   = 30,
    min_holder_count:     int   = 25,
    bscscan_api_key:      str   = "",
    max_deployer_tokens_30d: int = 3,
    # ── Chain-specific params (defaults = BSC PancakeSwap V2) ─────────────────
    chain_id:       int = 56,
    router_address: str = None,
    native_token:   str = None,
    stable_token:   str = None,
    dex_chain:      str = "bsc",
    explorer_url:   str = None,   # override block explorer (default = BSCScan)
    min_token_score:        int   = 0,
    min_lp_lock_pct_score:  float = 80.0,
    min_listing_age_min:    float = 0.0,
    min_buyers_score:       int   = 10,
    min_deployer_age_days:  float = 30.0,
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

    # ── Stage 1: fast on-chain checks only (~300ms) ──────────────────────────
    # Run these BEFORE the slow API calls so that if there's no liquidity yet
    # (the most common case — PairCreated fires before addLiquidity) we return
    # immediately instead of waiting 5-10s for BSCScan / GoPlus to time out.
    sim_result, sell_sim, lp_result = await asyncio.gather(
        asyncio.to_thread(_simulate_buy_sync,    w3, token_address, wallet_address, router_address, native_token),
        asyncio.to_thread(_simulate_sell_sync,   w3, token_address, pair_address, router_address),
        asyncio.to_thread(_check_lp_onchain_sync, w3, pair_address, lp_holder_max_pct),
    )

    # If buy sim fails with a liquidity error, return NOW — don't wait for APIs.
    # bot.py will put this token in the wait-for-liquidity queue and use
    # check_token_fast() (~300ms) when liquidity appears.
    if not sim_result["ok"]:
        reason = sim_result["reason"]
        if "нет ликвидности" in reason or "нулевой выход" in reason:
            return {"ok": False, "reason": reason}
        return {"ok": False, "reason": reason}

    if not sell_sim["ok"]:
        return {"ok": False, "reason": sell_sim["reason"]}

    if not lp_result["ok"]:
        return {"ok": False, "reason": lp_result["reason"]}

    # ── Stage 2: external API checks (3-10s, only runs when liquidity exists) ─
    # By the time we reach here, trading is active and the pool has reserves.
    # Now we can afford to wait for GoPlus / honeypot.is / BSCScan results.
    goplus_task    = _goplus_fetch(token_address, chain_id)
    honeypot_task  = _honeypot_is_check(token_address, max_buy_tax, max_sell_tax, chain_id)
    dexscreen_task = _dexscreener_fetch(pair_address, dex_chain)
    deployer_task  = _check_deployer_bscscan(token_address, bscscan_api_key, max_deployer_tokens_30d, explorer_url=explorer_url)

    goplus_data, hp_result, dex, deployer_result = await asyncio.gather(
        goplus_task, honeypot_task, dexscreen_task, deployer_task
    )

    # ── Fast blockers (instant, no GoPlus needed) ─────────────────────────────

    # Helper: build security cache from already-completed gather results.
    # Used when a token fails due to zero liquidity (not a real security issue) so
    # the wait-for-liquidity retry can use the fast path instead of re-running gather.
    def _build_security_partial():
        return {
            "goplus_data":    goplus_data,
            "hp_result":      hp_result,
            "deployer_result": deployer_result,
            "dex":            dex,
            "wallet_address": wallet_address,
            "router_address": router_address,
            "native_token":   native_token,
            "stable_token":   stable_token,
            "dex_chain":      dex_chain,
            "max_buy_tax":    max_buy_tax,
            "max_sell_tax":   max_sell_tax,
            "min_holder_count": min_holder_count,
            "max_top10_holder_pct":  max_top10_holder_pct,
            "max_token_age_days":    max_token_age_days,
            "min_volume_5m_usd":     min_volume_5m_usd,
            "max_deployer_lp_pct":   max_deployer_lp_pct,
            "min_token_score":        min_token_score,
            "min_lp_lock_pct_score":  min_lp_lock_pct_score,
            "min_listing_age_min":    min_listing_age_min,
            "min_buyers_score":       min_buyers_score,
            "min_deployer_age_days":  min_deployer_age_days,
            "bscscan_api_key":        bscscan_api_key,
            "explorer_url":           explorer_url or "",
        }

    # Buy simulation — catches: trading not enabled, honeypot on entry
    if not sim_result["ok"]:
        reason = sim_result["reason"]
        # Zero reserves = PairCreated fired before addLiquidity. Other API tasks already
        # ran in parallel — cache their results for the fast-path retry.
        if "нет ликвидности" in reason or "нулевой выход" in reason:
            return {"ok": False, "reason": reason, "_security": _build_security_partial()}
        return {"ok": False, "reason": reason}

    # Sell simulation (from pair) — catches: sell-blocking honeypots (FSOLon, SEDGon)
    if not sell_sim["ok"]:
        return {"ok": False, "reason": sell_sim["reason"]}

    # honeypot.is — catches: honeypot + enforces buy/sell tax thresholds immediately
    if not hp_result["ok"]:
        return {"ok": False, "reason": hp_result["reason"]}

    # If BOTH GoPlus AND honeypot.is failed to respond (timeout/error), we have
    # no external confirmation of safety — too risky to buy.
    goplus_unavailable   = goplus_data is None
    honeypot_unavailable = hp_result.get("buy_tax") is None and hp_result.get("sell_tax") is None
    if goplus_unavailable and honeypot_unavailable:
        return {"ok": False, "reason": "GoPlus и honeypot.is недоступны — невозможно проверить токен на honeypot"}

    # Deployer blacklist + serial deployer check (via BSCScan)
    if not deployer_result["ok"]:
        return {"ok": False, "reason": deployer_result["reason"]}

    # On-chain LP holder check — replaces GoPlus LP lock check
    # Works immediately for brand-new pairs, no external API needed
    if not lp_result["ok"]:
        return {"ok": False, "reason": lp_result["reason"]}

    # All security checks passed — build partial cache for wait-for-liquidity fast-path.
    _security_partial = _build_security_partial()

    # ── GoPlus — supplementary, non-blocking ──────────────────────────────────
    # GoPlus typically responds within 3s for indexed tokens.
    # For brand-new tokens it returns None — we proceed anyway using on-chain data.
    # When GoPlus IS available, we use it as an extra layer of validation.
    buy_tax  = 0.0
    sell_tax = 0.0
    warnings_from_goplus = []

    # Sell simulation was skipped (pair had no tokens when analysis ran).
    # The pre-buy check will re-run it when liquidity exists, but warn the user now.
    if sell_sim.get("skipped"):
        warnings_from_goplus.append(
            "🚨 Sell-симуляция НЕ ПРОВЕРЕНА — пул был пустым при анализе. "
            "Проверка выполнится перед покупкой"
        )

    if goplus_data:
        CRITICAL = {
            "is_honeypot":          "Honeypot (GoPlus)",
            "owner_change_balance": "Владелец может менять балансы",
            "selfdestruct":         "Selfdestruct функция",
            "cannot_buy":           "Покупка заблокирована контрактом",
            "cannot_sell_all":      "Продажа всех токенов заблокирована",
        }
        for flag, reason in CRITICAL.items():
            if goplus_data.get(flag) == "1":
                return {"ok": False, "reason": reason}

        # Предупреждения — риски есть, но для короткого сниперинга приемлемо:
        # transfer_pausable: очень распространено (~40% BSC токенов) — владелец может
        #   приостановить торговлю, но для T+0 снайпера с TP1 это не блокер
        # is_blacklisted: контракт имеет blacklist функцию — типично для анти-бот защиты,
        #   не означает что наш адрес заблокирован
        # can_take_back_ownership: владелец может вернуть права (common, ~40% токенов)
        # is_anti_whale: лимит объёма одной транзакции
        # trading_cooldown: задержка между транзакциями
        if goplus_data.get("transfer_pausable")    == "1": warnings_from_goplus.append("⚠️ Паузируемые переводы (типично для анти-бот)")
        if goplus_data.get("is_blacklisted")       == "1": warnings_from_goplus.append("⚠️ Blacklist функция в контракте")
        if goplus_data.get("can_take_back_ownership") == "1": warnings_from_goplus.append("⚠️ Возврат ownership возможен")
        if goplus_data.get("is_anti_whale")           == "1": warnings_from_goplus.append("⚠️ Anti-whale: лимит объёма одной транзакции")
        if goplus_data.get("trading_cooldown")        == "1": warnings_from_goplus.append("⚠️ Trading cooldown: задержка между транзакциями")

        buy_tax  = float(goplus_data.get("buy_tax")  or 0)
        sell_tax = float(goplus_data.get("sell_tax") or 0)
        if buy_tax > max_buy_tax:
            return {"ok": False, "reason": f"Buy tax (GoPlus): {buy_tax:.1f}%"}
        if sell_tax > max_sell_tax:
            return {"ok": False, "reason": f"Sell tax (GoPlus): {sell_tax:.1f}%"}

        # Minimum holder count
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
                pass

        # Top-10 combined holder concentration
        combined_top10 = 0.0
        for h in (goplus_data.get("holders") or [])[:10]:
            pct     = float(h.get("percent", 0)) * 100
            tag     = (h.get("tag") or "").lower()
            is_safe = h.get("is_locked", 0) == 1 or any(s in tag for s in SAFE_HOLDER_TAGS)
            if not is_safe:
                combined_top10 += pct
        if combined_top10 > max_top10_holder_pct:
            return {"ok": False, "reason": f"Топ-10 холдеров держат {combined_top10:.1f}% — риск скоординированного дампа"}

        # Non-critical warnings
        if goplus_data.get("is_mintable")   == "1": warnings_from_goplus.append("⚠️ Mintable")
        if goplus_data.get("hidden_owner")  == "1": warnings_from_goplus.append("⚠️ Hidden owner")
        if goplus_data.get("is_proxy")      == "1": warnings_from_goplus.append("⚠️ Proxy контракт")
        if goplus_data.get("external_call") == "1": warnings_from_goplus.append("⚠️ External call")
    # GoPlus None = not yet indexed — perfectly normal for brand-new tokens, not a problem

    # ── Liquidity check ───────────────────────────────────────────────────────
    native_price = await get_bnb_price(w3, router_address, native_token, stable_token)
    if native_price == 0.0:
        return {"ok": False, "reason": "Не удалось получить цену нативного токена"}

    liquidity_usd = await asyncio.to_thread(
        _get_liquidity_usd_sync, w3, pair_address, base_token, native_price, native_token
    )
    if liquidity_usd < min_liquidity_usd:
        return {"ok": False, "reason": f"Ликвидность: ${liquidity_usd:,.0f} < ${min_liquidity_usd:,.0f}",
                "_security": _security_partial}

    # ── FDV / Market cap check (on-chain) ─────────────────────────────────────
    fdv_usd = await asyncio.to_thread(
        _get_fdv_usd_sync, w3, token_address, native_price, router_address, native_token
    )
    if fdv_usd > 0:
        if fdv_usd < min_market_cap_usd:
            return {"ok": False, "reason": f"Market cap: ${fdv_usd:,.0f} < ${min_market_cap_usd:,.0f}",
                    "_security": _security_partial}
        if fdv_usd < min_fdv_usd:
            return {"ok": False, "reason": f"FDV: ${fdv_usd:,.0f} < ${min_fdv_usd:,.0f}",
                    "_security": _security_partial}
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

    _dep_addr = deployer_result.get("deployer")
    _dep_renounced = False
    if not _dep_addr:
        _owner_fb = await asyncio.to_thread(_get_token_owner_sync, w3, token_address)
        if _owner_fb == "renounced":
            _dep_renounced = True
        elif _owner_fb:
            _dep_addr = _owner_fb
    if not _dep_addr:  # also run when renounced — original deployer may still hold LP
        _mint_fb = await asyncio.to_thread(_get_deployer_from_mint_sync, w3, token_address)
        if _mint_fb:
            _dep_addr = _mint_fb
    if not _dep_addr:  # LP mint scan on pair contract — most reliable for fresh tokens
        _lp_adder_fb = await asyncio.to_thread(_get_lp_adder_sync, w3, pair_address)
        if _lp_adder_fb:
            _dep_addr = _lp_adder_fb
    deployer_pct: float | None = None
    deployer_lp_c: dict = {}
    if _dep_addr:
        _dh_c, _dlp_c = await asyncio.gather(
            asyncio.to_thread(_get_deployer_holdings_sync, w3, token_address, _dep_addr),
            asyncio.to_thread(_get_deployer_lp_pct_sync, w3, pair_address, _dep_addr),
        )
        deployer_pct   = _dh_c.get("pct")
        deployer_lp_c  = _dlp_c

    # ── Hard block: deployer holds unlocked LP → rug pull risk ────────────────
    _dep_lp_pct    = deployer_lp_c.get("pct")
    _dep_lp_locked = deployer_lp_c.get("locked_pct") or 0.0
    if _dep_lp_pct is not None:
        _dep_unlocked = _dep_lp_pct - _dep_lp_locked
        if _dep_unlocked > max_deployer_lp_pct:
            _lock_note = f" (из них {_dep_lp_locked:.0f}% в локере)" if _dep_lp_locked > 0 else ""
            return {"ok": False, "reason": f"Деплоер держит {_dep_lp_pct:.0f}% LP незаблокированными{_lock_note} — rug pull риск"}
    elif not _dep_addr:
        # Deployer unknown — use tight LP concentration check to catch unlocked LP holders
        _lp_tight = await asyncio.to_thread(_check_lp_onchain_sync, w3, pair_address, max_deployer_lp_pct)
        if not _lp_tight["ok"]:
            return {"ok": False, "reason": _lp_tight["reason"]}

    # ── Quality scoring ────────────────────────────────────────────────────────
    # Social presence from DexScreener (already fetched in Stage 2 — no new API call)
    _social_present = bool(
        dex and (
            (dex.get("info") or {}).get("socials") or
            (dex.get("info") or {}).get("websites")
        )
    )
    # Pair age from DexScreener pairCreatedAt (milliseconds)
    _pair_age_min: float | None = None
    if dex and dex.get("pairCreatedAt"):
        _pair_age_min = (time.time() * 1000 - dex["pairCreatedAt"]) / 60000

    # Contract verification + deployer wallet age in parallel (BSCScan calls, ~1-2s each)
    _exp_url = explorer_url or ""
    _contract_verified, _deployer_age_days = await asyncio.gather(
        _check_contract_verified(token_address, bscscan_api_key, _exp_url),
        _get_deployer_wallet_age(_dep_addr, bscscan_api_key, _exp_url) if _dep_addr else _null_async(),
    )

    _score = _calculate_token_score(
        lp_locked_pct          = deployer_lp_c.get("locked_pct"),
        lp_locker               = deployer_lp_c.get("locker"),
        contract_verified       = _contract_verified,
        social_present          = _social_present,
        deployer_wallet_age_days= _deployer_age_days,
        pair_age_minutes        = _pair_age_min,
        buyer_count             = holder_count,
        min_lp_lock_pct         = min_lp_lock_pct_score,
        min_deployer_age_days   = min_deployer_age_days,
        min_listing_age_min     = min_listing_age_min,
        min_buyers_score        = min_buyers_score,
    )
    if min_token_score > 0 and _score["score"] < min_token_score:
        return {
            "ok": False,
            "reason": (
                f"Рейтинг {_score['score']}/{_score['max_score']} {_score['grade']}"
                f" — ниже минимума {min_token_score}. "
                + "; ".join(f for f in _score["factors"] if "+0" in f or "❌" in f)[:120]
            ),
        }

    info = {
        "name":           name,
        "symbol":         symbol,
        "buy_tax":        buy_tax,
        "sell_tax":       sell_tax,
        "liquidity_usd":  liquidity_usd,
        "fdv_usd":        fdv_usd,
        "bnb_price":      native_price,
        "holder_count":   holder_count,
        "is_mintable":    bool(goplus_data and goplus_data.get("is_mintable")   == "1"),
        "hidden_owner":   bool(goplus_data and goplus_data.get("hidden_owner")  == "1"),
        "is_proxy":       bool(goplus_data and goplus_data.get("is_proxy")      == "1"),
        "external_call":  bool(goplus_data and goplus_data.get("external_call") == "1"),
        "goplus_ok":      goplus_data is not None,
        "extra_warnings":    warnings_from_goplus,
        "sim_sell_skipped":  sell_sim.get("skipped", False),
        # Deployer info (BSCScan or on-chain owner() fallback)
        "deployer":              _dep_addr,
        "deployer_renounced":    _dep_renounced,
        "deployer_pct":          deployer_pct,
        "deployer_lp_pct":       deployer_lp_c.get("pct"),
        "deployer_lp_locked_pct": deployer_lp_c.get("locked_pct"),
        "deployer_lp_locker":    deployer_lp_c.get("locker"),
        "token_score":           _score,
        "contract_verified":     _contract_verified,
        "social_present":        _social_present,
        "deployer_age_days":     _deployer_age_days,
        "pair_age_min":          _pair_age_min,
    }
    return {"ok": True, "info": info}


async def check_token_fast(
    token_address: str,
    pair_address:  str,
    base_token:    str,
    w3:            Web3,
    min_liquidity_usd:  float,
    min_market_cap_usd: float,
    min_fdv_usd:        float,
    max_fdv_usd:        float,
    security:           dict,   # cached from _security key in prior check_token() rejection
) -> dict:
    """
    Fast-path re-check for wait-for-liquidity retries.

    Skips the expensive 5-second asyncio.gather() of external APIs.
    Runs only the three on-chain checks that change after liquidity is added:
      1. buy simulation (~200ms)
      2. liquidity amount
      3. FDV / market cap

    Uses the previously validated security context (GoPlus, honeypot.is, deployer, etc.)
    so total latency is ~300ms instead of ~5000ms.

    Returns same format as check_token(): {"ok": True, "info": {...}} or {"ok": False, "reason": "..."}
    """
    router_address = security.get("router_address")
    native_token   = security.get("native_token")
    stable_token   = security.get("stable_token")
    wallet_address = security.get("wallet_address") or "0x0000000000000000000000000000000000000001"
    goplus_data    = security.get("goplus_data")
    hp_result      = security.get("hp_result") or {"ok": True}
    deployer_result = security.get("deployer_result") or {"ok": True}
    dex            = security.get("dex")
    max_buy_tax    = security.get("max_buy_tax", 10.0)
    max_sell_tax   = security.get("max_sell_tax", 10.0)
    min_holder_count      = security.get("min_holder_count", 0)
    max_top10_holder_pct  = security.get("max_top10_holder_pct", 60.0)
    max_token_age_days    = security.get("max_token_age_days", 7)
    min_volume_5m_usd     = security.get("min_volume_5m_usd", 0.0)
    max_deployer_lp_pct   = security.get("max_deployer_lp_pct", 30.0)
    min_token_score       = security.get("min_token_score",       0)
    min_lp_lock_pct_score = security.get("min_lp_lock_pct_score", 80.0)
    min_listing_age_min   = security.get("min_listing_age_min",   0.0)
    min_buyers_score      = security.get("min_buyers_score",      10)
    min_deployer_age_days = security.get("min_deployer_age_days", 30.0)
    bscscan_api_key       = security.get("bscscan_api_key",       "")
    contract_verified_c   = security.get("contract_verified")     # None if not cached

    # Re-run only the on-chain checks that depend on liquidity being present.
    # All three run in parallel — total ~200-300ms.
    sim_buy_task  = asyncio.to_thread(
        _simulate_buy_sync, w3, token_address, wallet_address, router_address, native_token
    )
    sim_sell_task = asyncio.to_thread(_simulate_sell_sync, w3, token_address, pair_address, router_address)
    price_task    = asyncio.to_thread(
        _get_bnb_price_sync, w3, router_address, native_token, stable_token
    )
    sim_result, sell_sim, native_price = await asyncio.gather(
        sim_buy_task, sim_sell_task, price_task
    )

    if not sim_result["ok"]:
        return {"ok": False, "reason": sim_result["reason"]}
    if not sell_sim["ok"]:
        return {"ok": False, "reason": sell_sim["reason"]}
    if native_price == 0.0:
        return {"ok": False, "reason": "Не удалось получить цену нативного токена"}

    liquidity_usd = await asyncio.to_thread(
        _get_liquidity_usd_sync, w3, pair_address, base_token, native_price, native_token
    )
    if liquidity_usd < min_liquidity_usd:
        return {"ok": False, "reason": f"Ликвидность: ${liquidity_usd:,.0f} < ${min_liquidity_usd:,.0f}"}

    fdv_usd = await asyncio.to_thread(
        _get_fdv_usd_sync, w3, token_address, native_price, router_address, native_token
    )
    if fdv_usd > 0:
        if fdv_usd < min_market_cap_usd:
            return {"ok": False, "reason": f"Market cap: ${fdv_usd:,.0f} < ${min_market_cap_usd:,.0f}"}
        if fdv_usd < min_fdv_usd:
            return {"ok": False, "reason": f"FDV: ${fdv_usd:,.0f} < ${min_fdv_usd:,.0f}"}
        if fdv_usd > max_fdv_usd:
            return {"ok": False, "reason": f"FDV: ${fdv_usd:,.0f} > ${max_fdv_usd:,.0f} (слишком крупный)"}

    # Build warnings and info from cached security data
    warnings_from_goplus = []
    buy_tax  = 0.0
    sell_tax = 0.0

    if sell_sim.get("skipped"):
        warnings_from_goplus.append(
            "🚨 Sell-симуляция НЕ ПРОВЕРЕНА — пул был пустым при анализе. "
            "Проверка выполнится перед покупкой"
        )

    if goplus_data:
        if goplus_data.get("transfer_pausable")    == "1": warnings_from_goplus.append("⚠️ Паузируемые переводы")
        if goplus_data.get("is_blacklisted")       == "1": warnings_from_goplus.append("⚠️ Blacklist функция")
        if goplus_data.get("can_take_back_ownership") == "1": warnings_from_goplus.append("⚠️ Возврат ownership возможен")
        if goplus_data.get("is_anti_whale")           == "1": warnings_from_goplus.append("⚠️ Anti-whale: лимит объёма одной транзакции")
        if goplus_data.get("trading_cooldown")        == "1": warnings_from_goplus.append("⚠️ Trading cooldown: задержка между транзакциями")
        if goplus_data.get("is_mintable")   == "1": warnings_from_goplus.append("⚠️ Mintable")
        if goplus_data.get("hidden_owner")  == "1": warnings_from_goplus.append("⚠️ Hidden owner")
        if goplus_data.get("is_proxy")      == "1": warnings_from_goplus.append("⚠️ Proxy контракт")
        if goplus_data.get("external_call") == "1": warnings_from_goplus.append("⚠️ External call")
        buy_tax  = float(goplus_data.get("buy_tax")  or 0)
        sell_tax = float(goplus_data.get("sell_tax") or 0)
        if buy_tax > max_buy_tax:
            return {"ok": False, "reason": f"Buy tax (GoPlus): {buy_tax:.1f}% > {max_buy_tax:.0f}%"}
        if sell_tax > max_sell_tax:
            return {"ok": False, "reason": f"Sell tax (GoPlus): {sell_tax:.1f}% > {max_sell_tax:.0f}%"}
        name   = goplus_data.get("token_name",   "Unknown")
        symbol = goplus_data.get("token_symbol", "???")
        holder_count = goplus_data.get("holder_count", "?")
    else:
        token_meta   = await asyncio.to_thread(_get_token_info_sync, w3, token_address)
        name         = token_meta["name"]
        symbol       = token_meta["symbol"]
        holder_count = "?"

    _dep_addr_f = deployer_result.get("deployer")
    _dep_renounced_f = False
    if not _dep_addr_f:
        _owner_fb_f = await asyncio.to_thread(_get_token_owner_sync, w3, token_address)
        if _owner_fb_f == "renounced":
            _dep_renounced_f = True
        elif _owner_fb_f:
            _dep_addr_f = _owner_fb_f
    if not _dep_addr_f:  # also run when renounced — original deployer may still hold LP
        _mint_fb_f = await asyncio.to_thread(_get_deployer_from_mint_sync, w3, token_address)
        if _mint_fb_f:
            _dep_addr_f = _mint_fb_f
    if not _dep_addr_f:  # LP mint scan on pair contract — most reliable for fresh tokens
        _lp_adder_fb_f = await asyncio.to_thread(_get_lp_adder_sync, w3, pair_address)
        if _lp_adder_fb_f:
            _dep_addr_f = _lp_adder_fb_f
    deployer_pct_f: float | None = None
    deployer_lp_f: dict = {}
    if _dep_addr_f:
        _dh_f, _dlp_f = await asyncio.gather(
            asyncio.to_thread(_get_deployer_holdings_sync, w3, token_address, _dep_addr_f),
            asyncio.to_thread(_get_deployer_lp_pct_sync, w3, pair_address, _dep_addr_f),
        )
        deployer_pct_f = _dh_f.get("pct")
        deployer_lp_f  = _dlp_f

    # ── Hard block: deployer holds unlocked LP → rug pull risk ────────────────
    _dep_lp_pct_f    = deployer_lp_f.get("pct")
    _dep_lp_locked_f = deployer_lp_f.get("locked_pct") or 0.0
    if _dep_lp_pct_f is not None:
        _dep_unlocked_f = _dep_lp_pct_f - _dep_lp_locked_f
        if _dep_unlocked_f > max_deployer_lp_pct:
            _lock_note_f = f" (из них {_dep_lp_locked_f:.0f}% в локере)" if _dep_lp_locked_f > 0 else ""
            return {"ok": False, "reason": f"Деплоер держит {_dep_lp_pct_f:.0f}% LP незаблокированными{_lock_note_f} — rug pull риск"}
    elif not _dep_addr_f:
        # Deployer unknown — use tight LP concentration check
        _lp_tight_f = await asyncio.to_thread(_check_lp_onchain_sync, w3, pair_address, max_deployer_lp_pct)
        if not _lp_tight_f["ok"]:
            return {"ok": False, "reason": _lp_tight_f["reason"]}

    # ── Quality scoring (fast path) ────────────────────────────────────────────
    _social_present_f = bool(
        dex and (
            (dex.get("info") or {}).get("socials") or
            (dex.get("info") or {}).get("websites")
        )
    )
    _pair_age_min_f: float | None = None
    if dex and dex.get("pairCreatedAt"):
        _pair_age_min_f = (time.time() * 1000 - dex["pairCreatedAt"]) / 60000
    _exp_url_f = security.get("explorer_url") or ""
    if contract_verified_c is None and bscscan_api_key:
        contract_verified_c = await _check_contract_verified(token_address, bscscan_api_key, _exp_url_f)
    _deployer_age_days_f: float | None = None
    if _dep_addr_f:
        _deployer_age_days_f = await _get_deployer_wallet_age(_dep_addr_f, bscscan_api_key, _exp_url_f)
    _score_f = _calculate_token_score(
        lp_locked_pct           = deployer_lp_f.get("locked_pct"),
        lp_locker                = deployer_lp_f.get("locker"),
        contract_verified        = contract_verified_c,
        social_present           = _social_present_f,
        deployer_wallet_age_days = _deployer_age_days_f,
        pair_age_minutes         = _pair_age_min_f,
        buyer_count              = holder_count,
        min_lp_lock_pct          = min_lp_lock_pct_score,
        min_deployer_age_days    = min_deployer_age_days,
        min_listing_age_min      = min_listing_age_min,
        min_buyers_score         = min_buyers_score,
    )
    if min_token_score > 0 and _score_f["score"] < min_token_score:
        return {
            "ok": False,
            "reason": (
                f"Рейтинг {_score_f['score']}/{_score_f['max_score']} {_score_f['grade']}"
                f" — ниже минимума {min_token_score}. "
                + "; ".join(f for f in _score_f["factors"] if "+0" in f or "❌" in f)[:120]
            ),
        }

    info = {
        "name":           name,
        "symbol":         symbol,
        "buy_tax":        buy_tax,
        "sell_tax":       sell_tax,
        "liquidity_usd":  liquidity_usd,
        "fdv_usd":        fdv_usd,
        "bnb_price":      native_price,
        "holder_count":   holder_count,
        "is_mintable":    bool(goplus_data and goplus_data.get("is_mintable")   == "1"),
        "hidden_owner":   bool(goplus_data and goplus_data.get("hidden_owner")  == "1"),
        "is_proxy":       bool(goplus_data and goplus_data.get("is_proxy")      == "1"),
        "external_call":  bool(goplus_data and goplus_data.get("external_call") == "1"),
        "goplus_ok":      goplus_data is not None,
        "extra_warnings":    warnings_from_goplus,
        "sim_sell_skipped":  sell_sim.get("skipped", False),
        "deployer":              _dep_addr_f,
        "deployer_renounced":    _dep_renounced_f,
        "deployer_pct":          deployer_pct_f,
        "deployer_lp_pct":       deployer_lp_f.get("pct"),
        "deployer_lp_locked_pct": deployer_lp_f.get("locked_pct"),
        "deployer_lp_locker":    deployer_lp_f.get("locker"),
        "token_score":           _score_f,
        "contract_verified":     contract_verified_c,
        "social_present":        _social_present_f,
        "deployer_age_days":     _deployer_age_days_f,
        "pair_age_min":          _pair_age_min_f,
    }
    return {"ok": True, "info": info}


async def fetch_security_partial(
    token_address:  str,
    pair_address:   str,
    bscscan_api_key: str = "",
    max_buy_tax:    float = 20.0,
    max_sell_tax:   float = 20.0,
    chain_id:       int   = 56,
    wallet_address: str   = "0x0000000000000000000000000000000000000001",
    router_address: str   = None,
    native_token:   str   = None,
    stable_token:   str   = None,
    dex_chain:      str   = "bsc",
    explorer_url:   str   = None,
    max_deployer_tokens_30d: int = 20,
    min_holder_count:     int   = 0,
    max_top10_holder_pct: float = 60.0,
    max_token_age_days:   int   = 7,
    min_volume_5m_usd:    float = 0.0,
) -> dict:
    """
    Run ONLY the external API checks (GoPlus, honeypot.is, BSCScan, DexScreener).
    No on-chain calls — safe to call before the pair/liquidity exists.

    Used by the mempool watcher to pre-compute security while createPair() is
    still pending. Returns a security-partial dict compatible with check_token_fast().

    Also returns a quick "pre_rejected" flag when APIs already confirm a bad token
    (honeypot, blacklisted deployer) so on_pair_found can skip all further checks.
    """
    goplus_data, hp_result, dex, deployer_result = await asyncio.gather(
        _goplus_fetch(token_address, chain_id),
        _honeypot_is_check(token_address, max_buy_tax, max_sell_tax, chain_id),
        _dexscreener_fetch(pair_address, dex_chain),
        _check_deployer_bscscan(token_address, bscscan_api_key, max_deployer_tokens_30d, explorer_url=explorer_url),
    )

    # Fast pre-reject on definitive API findings (honeypot.is / deployer blacklist)
    pre_reject = None
    if not hp_result["ok"]:
        pre_reject = hp_result["reason"]
    elif not deployer_result["ok"]:
        pre_reject = deployer_result["reason"]
    elif goplus_data:
        for flag, reason in {
            "is_honeypot":          "Honeypot (GoPlus)",
            "cannot_buy":           "Покупка заблокирована контрактом",
            "cannot_sell_all":      "Продажа всех токенов заблокирована",
            "selfdestruct":         "Selfdestruct функция",
            "owner_change_balance": "Владелец может менять балансы",
        }.items():
            if goplus_data.get(flag) == "1":
                pre_reject = reason
                break

    security = {
        "goplus_data":          goplus_data,
        "hp_result":            hp_result,
        "deployer_result":      deployer_result,
        "dex":                  dex,
        "wallet_address":       wallet_address,
        "router_address":       router_address,
        "native_token":         native_token,
        "stable_token":         stable_token,
        "dex_chain":            dex_chain,
        "max_buy_tax":          max_buy_tax,
        "max_sell_tax":         max_sell_tax,
        "min_holder_count":     min_holder_count,
        "max_top10_holder_pct": max_top10_holder_pct,
        "max_token_age_days":   max_token_age_days,
        "min_volume_5m_usd":    min_volume_5m_usd,
    }
    return {"security": security, "pre_reject": pre_reject}
