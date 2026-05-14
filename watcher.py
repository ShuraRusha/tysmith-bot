import asyncio
import json
import logging
import time

import websockets
from web3 import Web3

from config import PANCAKE_FACTORY_V2, BASE_TOKENS, BSC_WS_RPCS

log = logging.getLogger(__name__)

# keccak256("PairCreated(address,address,address,uint256)")
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# keccak256("PoolCreated(address,address,bool,address,uint256)") — Aerodrome V2
AERODROME_POOL_CREATED_TOPIC = "0x2128d88d14c80cb081c7252935b7e6a14609e6d5f8a62da55a4276cbf4ab8ea4"

# Per-endpoint connection status — read by /debug command in bot.py
# key: ws_url (first 60 chars), value: {connected, last_event_ts, events_total, reconnects, last_error}
_ws_endpoint_status: dict[str, dict] = {}

# Raised when node explicitly rejects newPendingTransactions (wrong tier/plan).
# Caught by the caller to disable mempool without endless retries.
class MemPoolNotSupportedError(Exception):
    pass

# ── CREATE2 pair address prediction ──────────────────────────────────────────
# createPair(address,address) selector (same on all Uniswap V2 forks)
_CREATE_PAIR_SIG = "0xc9c65396"

# PancakeSwap V2 INIT_CODE_HASH
_PANCAKE_INIT_CODE_HASH = bytes.fromhex(
    "00fb7f630766e6a796048ea87d01acd3068e8ff67d078148a3fa3f4a84f69bd5"
)
# Uniswap V2 on Base INIT_CODE_HASH
_UNISWAP_V2_BASE_INIT_CODE_HASH = bytes.fromhex(
    "96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f"
)
# BiSwap V2 INIT_CODE_HASH
_BISWAP_INIT_CODE_HASH = bytes.fromhex(
    "fea293c909d87cd4153593f077b76bb7e94340200f4ee84211ae8e4f9bd7ffdf"
)
# BaseSwap V2 INIT_CODE_HASH
_BASESWAP_INIT_CODE_HASH = bytes.fromhex(
    "b618a2730fae167f5f8ac7bd659dd8436d571872655bcb6fd11f2158c8a64a3b"
)


def compute_pair_address(
    token_a: str,
    token_b: str,
    factory_address: str = None,
    init_code_hash: bytes = None,
) -> str:
    """Predict CREATE2 pair address without an on-chain call."""
    factory = factory_address or PANCAKE_FACTORY_V2
    hash_   = init_code_hash  or _PANCAKE_INIT_CODE_HASH
    a, b = token_a.lower(), token_b.lower()
    t0, t1 = (a, b) if a < b else (b, a)
    salt = Web3.solidity_keccak(
        ["address", "address"],
        [Web3.to_checksum_address(t0), Web3.to_checksum_address(t1)],
    )
    factory_bytes = bytes.fromhex(factory[2:])
    raw = Web3.keccak(b"\xff" + factory_bytes + salt + hash_)
    return Web3.to_checksum_address("0x" + raw.hex()[-40:])


def _ep_key(ws_url: str) -> str:
    return ws_url[:60]


def _ep_init(ws_url: str):
    key = _ep_key(ws_url)
    if key not in _ws_endpoint_status:
        _ws_endpoint_status[key] = {
            "url":          ws_url,
            "connected":    False,
            "last_event_ts": 0.0,
            "events_total": 0,
            "reconnects":   0,
            "last_error":   "",
        }
    return key


async def _watch_single(ws_url: str, callback, factory_address: str = None, base_tokens: set = None,
                        event_topic: str = PAIR_CREATED_TOPIC, volatile_only: bool = False):
    """Connect to one WS endpoint and stream PairCreated/PoolCreated events."""
    factory = factory_address or PANCAKE_FACTORY_V2
    tokens  = base_tokens or BASE_TOKENS
    backoff = 2
    ep = _ep_init(ws_url)
    # BSC produces pairs every ~10-30s — if nothing arrives in 90s, the node is stale.
    # Many free nodes accept subscriptions but silently stop delivering events.
    _RECV_TIMEOUT = 90
    while True:
        _ws_endpoint_status[ep]["connected"] = False
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
                            "address": factory,
                            "topics":  [event_topic],
                        },
                    ],
                }))

                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if "error" in resp:
                    raise RuntimeError(f"Subscribe error: {resp['error']}")
                sub_id = resp.get("result")
                log.info(f"WS subscribed ({ws_url[:40]}…) sub_id={sub_id}")
                backoff = 2
                _ws_endpoint_status[ep]["connected"] = True
                _ws_endpoint_status[ep]["last_error"] = ""

                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=_RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        log.warning(
                            f"WS: no events for {_RECV_TIMEOUT}s ({ws_url[:40]}…) "
                            "— node is stale, reconnecting"
                        )
                        _ws_endpoint_status[ep]["reconnects"] += 1
                        break  # exit inner loop → reconnect

                    try:
                        msg = json.loads(raw)
                        if msg.get("method") != "eth_subscription":
                            continue

                        log_entry = msg["params"]["result"]
                        topics    = log_entry.get("topics", [])
                        min_topics = 4 if volatile_only else 3
                        if len(topics) < min_topics:
                            continue
                        if volatile_only:
                            # topics[3] = stable bool; all zeros = volatile (False) = what we want
                            if topics[3] != "0x" + "0" * 64:
                                continue  # skip stable pools

                        # Verify the event is from the expected factory.
                        # Some public BSC nodes ignore the address filter in eth_subscribe
                        # and return ALL PairCreated events — this guard prevents cross-factory noise.
                        event_addr = log_entry.get("address", "")
                        if event_addr.lower() != factory.lower():
                            continue

                        token0 = Web3.to_checksum_address("0x" + topics[1][-40:])
                        token1 = Web3.to_checksum_address("0x" + topics[2][-40:])

                        raw_data = log_entry.get("data", "")
                        if len(raw_data) < 66:
                            continue
                        pair = Web3.to_checksum_address("0x" + raw_data[26:66])

                        t0 = token0.lower()
                        t1 = token1.lower()

                        if t0 in tokens and t1 not in tokens:
                            new_token, base_token = token1, token0
                        elif t1 in tokens and t0 not in tokens:
                            new_token, base_token = token0, token1
                        else:
                            continue

                        creation_block = int(log_entry.get("blockNumber", "0x0"), 16)
                        log.info(f"New pair detected: {new_token} / {base_token} @ {pair}")
                        _ws_endpoint_status[ep]["last_event_ts"] = time.time()
                        _ws_endpoint_status[ep]["events_total"] += 1
                        asyncio.create_task(callback(new_token, base_token, pair, creation_block))

                    except Exception as e:
                        log.warning(f"Event parse error: {e}")

        except asyncio.TimeoutError:
            err = f"subscribe timeout"
            log.error(f"WS {err} ({ws_url[:40]}…). Reconnecting in {backoff}s...")
            _ws_endpoint_status[ep]["last_error"] = err
            _ws_endpoint_status[ep]["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)
        except Exception as e:
            err = str(e)[:80]
            log.error(f"WS error ({ws_url[:40]}…): {e}. Reconnecting in {backoff}s...")
            _ws_endpoint_status[ep]["last_error"] = err
            _ws_endpoint_status[ep]["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)   # max 15s — публичная нода нестабильна, восстанавливаемся быстро


# Dedup: don't process the same pair twice if multiple WS see it
_seen_pairs: set[str] = set()
_seen_lock = asyncio.Lock()
MAX_SEEN = 5000


async def _dedup_callback(callback, new_token, base_token, pair, creation_block=0):
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
    await callback(new_token, base_token, pair, creation_block)


async def watch_pairs(
    ws_url: str,
    callback,
    factory_address: str = None,
    base_tokens: set = None,
    ws_rpcs: list = None,
    event_topic: str = PAIR_CREATED_TOPIC,
    volatile_only: bool = False,
):
    """
    Subscribe to Uniswap V2 / PancakeSwap V2 PairCreated events (or Aerodrome PoolCreated).

    If multiple WS endpoints are configured, connects to all in parallel
    for redundancy — first event wins (deduped).
    """
    urls = ws_rpcs if ws_rpcs and len(ws_rpcs) > 1 else (
        BSC_WS_RPCS if len(BSC_WS_RPCS) > 1 else [ws_url]
    )

    if len(urls) == 1:
        await _watch_single(urls[0], callback, factory_address, base_tokens,
                            event_topic=event_topic, volatile_only=volatile_only)
    else:
        log.info(f"Multi-WS mode: {len(urls)} endpoints for redundancy")

        async def dedup_cb(new_token, base_token, pair, creation_block=0):
            await _dedup_callback(callback, new_token, base_token, pair, creation_block)

        tasks = [
            asyncio.create_task(_watch_single(u, dedup_cb, factory_address, base_tokens,
                                              event_topic=event_topic, volatile_only=volatile_only))
            for u in urls
        ]
        await asyncio.gather(*tasks)


# ── Mempool watcher (pending createPair transactions) ────────────────────────

async def watch_pending_pairs(
    ws_url: str,
    callback,
    factory_address: str = None,
    init_code_hash: bytes = None,
    base_tokens: set = None,
):
    """
    Subscribe to newPendingTransactions and detect createPair() calls before
    they are mined.

    Requires a node that exposes the full mempool (e.g. NodeReal premium).
    Public nodes typically don't support newPendingTransactions.
    """
    factory_lower = (factory_address or PANCAKE_FACTORY_V2).lower()
    tokens        = base_tokens or BASE_TOKENS
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

                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if "error" in resp:
                    err = resp["error"]
                    msg = str(err.get("message", err) if isinstance(err, dict) else err).lower()
                    # Codes: -32601 = method not found, -32000 = subscription not supported
                    code = err.get("code", 0) if isinstance(err, dict) else 0
                    if code in (-32601, -32000) or any(
                        x in msg for x in ("not found", "not support", "unsupport", "unavailable")
                    ):
                        raise MemPoolNotSupportedError(
                            f"Node rejected newPendingTransactions: {resp['error']}"
                        )
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
                        if tx_to != factory_lower:
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

                        if ta in tokens and tb not in tokens:
                            new_token, base_token = token_b, token_a
                        elif tb in tokens and ta not in tokens:
                            new_token, base_token = token_a, token_b
                        else:
                            continue

                        pair = compute_pair_address(
                            new_token, base_token,
                            factory_address, init_code_hash,
                        )
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

        except MemPoolNotSupportedError:
            raise  # propagate so caller can disable mempool permanently
        except Exception as e:
            log.warning(f"Mempool WS error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
