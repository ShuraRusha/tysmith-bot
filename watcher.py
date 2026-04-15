import asyncio
import json
import logging

import websockets
from web3 import Web3

from config import PANCAKE_FACTORY_V2, BASE_TOKENS, BSC_WS_RPCS

log = logging.getLogger(__name__)

# keccak256("PairCreated(address,address,address,uint256)")
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# ── CREATE2 pair address prediction ──────────────────────────────────────────
# PancakeSwap V2 Factory INIT_CODE_HASH (deterministic pair address computation)
_INIT_CODE_HASH = bytes.fromhex(
    "00fb7f630766e6a796048ea87d01acd3068e8ff67d078148a3fa3f4a84f69bd5"
)
# createPair(address,address) selector
_CREATE_PAIR_SIG = "0xc9c65396"
_FACTORY_LOWER   = PANCAKE_FACTORY_V2.lower()


def compute_pair_address(token_a: str, token_b: str) -> str:
    """Predict CREATE2 pair address without an on-chain call."""
    a, b = token_a.lower(), token_b.lower()
    t0, t1 = (a, b) if a < b else (b, a)
    salt = Web3.solidity_keccak(
        ["address", "address"],
        [Web3.to_checksum_address(t0), Web3.to_checksum_address(t1)],
    )
    factory_bytes = bytes.fromhex(PANCAKE_FACTORY_V2[2:])
    raw = Web3.keccak(b"\xff" + factory_bytes + salt + _INIT_CODE_HASH)
    return Web3.to_checksum_address("0x" + raw.hex()[-40:])


async def _watch_single(ws_url: str, callback):
    """Connect to one WS endpoint and stream PairCreated events."""
    backoff = 2
    while True:
        try:
            log.info(f"WS connecting: {ws_url[:60]}...")
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": PANCAKE_FACTORY_V2,
                            "topics":  [PAIR_CREATED_TOPIC],
                        },
                    ],
                }))

                resp = json.loads(await ws.recv())
                if "error" in resp:
                    raise RuntimeError(f"Subscribe error: {resp['error']}")
                sub_id = resp.get("result")
                log.info(f"WS subscribed ({ws_url[:40]}…) sub_id={sub_id}")
                backoff = 2

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("method") != "eth_subscription":
                            continue

                        log_entry = msg["params"]["result"]
                        topics    = log_entry.get("topics", [])
                        if len(topics) < 3:
                            continue

                        token0 = Web3.to_checksum_address("0x" + topics[1][-40:])
                        token1 = Web3.to_checksum_address("0x" + topics[2][-40:])

                        raw_data = log_entry.get("data", "")
                        if len(raw_data) < 66:
                            continue
                        pair = Web3.to_checksum_address("0x" + raw_data[26:66])

                        t0 = token0.lower()
                        t1 = token1.lower()

                        if t0 in BASE_TOKENS and t1 not in BASE_TOKENS:
                            new_token, base_token = token1, token0
                        elif t1 in BASE_TOKENS and t0 not in BASE_TOKENS:
                            new_token, base_token = token0, token1
                        else:
                            continue

                        log.info(f"New pair detected: {new_token} / {base_token} @ {pair}")
                        asyncio.create_task(callback(new_token, base_token, pair))

                    except Exception as e:
                        log.warning(f"Event parse error: {e}")

        except Exception as e:
            log.error(f"WS error ({ws_url[:40]}…): {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)   # max 15s — публичная нода нестабильна, восстанавливаемся быстро


# Dedup: don't process the same pair twice if multiple WS see it
_seen_pairs: set[str] = set()
_seen_lock = asyncio.Lock()
MAX_SEEN = 5000


async def _dedup_callback(callback, new_token, base_token, pair):
    """Wrapper that deduplicates events across multiple WS connections."""
    async with _seen_lock:
        if pair in _seen_pairs:
            return
        _seen_pairs.add(pair)
        if len(_seen_pairs) > MAX_SEEN:
            # Trim oldest half
            to_remove = list(_seen_pairs)[:MAX_SEEN // 2]
            for p in to_remove:
                _seen_pairs.discard(p)
    await callback(new_token, base_token, pair)


async def watch_pairs(ws_url: str, callback):
    """
    Subscribe to PancakeSwap V2 PairCreated events.

    If multiple WS endpoints are configured (BSC_WS_RPCS), connects to all
    of them in parallel for redundancy — first event wins (deduped).
    Falls back to single ws_url if BSC_WS_RPCS has only one entry.
    """
    urls = BSC_WS_RPCS if len(BSC_WS_RPCS) > 1 else [ws_url]

    if len(urls) == 1:
        # Single connection — no dedup overhead
        await _watch_single(urls[0], callback)
    else:
        log.info(f"Multi-WS mode: {len(urls)} endpoints for redundancy")

        async def dedup_cb(new_token, base_token, pair):
            await _dedup_callback(callback, new_token, base_token, pair)

        tasks = [asyncio.create_task(_watch_single(u, dedup_cb)) for u in urls]
        await asyncio.gather(*tasks)


# ── Mempool watcher (pending createPair transactions) ────────────────────────

async def watch_pending_pairs(ws_url: str, callback):
    """
    Subscribe to newPendingTransactions and detect createPair() calls before
    they are mined. For each detected pair creation:
      1. Decode tokenA/tokenB from calldata
      2. Compute the pair address via CREATE2 (no on-chain call)
      3. Fire callback(new_token, base_token, predicted_pair)

    The callback should start pre-analysis so results are cached by the time
    PairCreated event fires.

    Requires a node that exposes the full mempool (e.g. NodeReal premium).
    Public nodes typically don't support newPendingTransactions.
    """
    backoff = 2
    while True:
        try:
            log.info(f"Mempool WS connecting: {ws_url[:60]}...")
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                # Subscribe to pending transaction hashes
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": ["newPendingTransactions"],
                }))

                resp = json.loads(await ws.recv())
                if "error" in resp:
                    raise RuntimeError(f"Mempool subscribe error: {resp['error']}")
                sub_id = resp.get("result")
                log.info(f"Mempool subscribed, sub_id={sub_id}")
                backoff = 2

                # Batch tx hash fetches to avoid overwhelming the node
                pending_hashes: asyncio.Queue[str] = asyncio.Queue(maxsize=500)

                async def _fetch_worker():
                    """Fetch full tx for pending hashes and filter for createPair."""
                    while True:
                        tx_hash = await pending_hashes.get()
                        try:
                            # eth_getTransactionByHash via WS
                            req_id = hash(tx_hash) & 0xFFFFFFFF
                            await ws.send(json.dumps({
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "method": "eth_getTransactionByHash",
                                "params": [tx_hash],
                            }))
                        except Exception:
                            pass

                # Start fetch workers
                workers = [asyncio.create_task(_fetch_worker()) for _ in range(3)]

                async for raw in ws:
                    try:
                        msg = json.loads(raw)

                        # Handle subscription notifications (new pending tx hash)
                        if msg.get("method") == "eth_subscription":
                            tx_hash = msg["params"]["result"]
                            if not pending_hashes.full():
                                pending_hashes.put_nowait(tx_hash)
                            continue

                        # Handle RPC responses (getTransactionByHash results)
                        result = msg.get("result")
                        if not result or not isinstance(result, dict):
                            continue

                        tx_to   = (result.get("to") or "").lower()
                        tx_data = result.get("input") or ""

                        # Filter: only factory createPair calls
                        if tx_to != _FACTORY_LOWER:
                            continue
                        if not tx_data.startswith(_CREATE_PAIR_SIG):
                            continue

                        # Decode createPair(address, address)
                        if len(tx_data) < 138:  # 0x + 8 sig + 64 addr + 64 addr
                            continue
                        token_a = Web3.to_checksum_address("0x" + tx_data[34:74])
                        token_b = Web3.to_checksum_address("0x" + tx_data[98:138])

                        ta = token_a.lower()
                        tb = token_b.lower()

                        if ta in BASE_TOKENS and tb not in BASE_TOKENS:
                            new_token, base_token = token_b, token_a
                        elif tb in BASE_TOKENS and ta not in BASE_TOKENS:
                            new_token, base_token = token_a, token_b
                        else:
                            continue

                        pair = compute_pair_address(new_token, base_token)
                        log.info(
                            f"Mempool: pending createPair detected! "
                            f"{new_token[:10]}… / {base_token[:10]}… → pair {pair[:10]}…"
                        )
                        asyncio.create_task(callback(new_token, base_token, pair))

                    except Exception as e:
                        # Don't spam logs for every malformed message
                        pass

                for w in workers:
                    w.cancel()

        except Exception as e:
            log.warning(f"Mempool WS error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
