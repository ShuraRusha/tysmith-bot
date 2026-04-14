import asyncio
import json
import logging

import websockets
from web3 import Web3

from config import PANCAKE_FACTORY_V2, BASE_TOKENS, BSC_WS_RPCS

log = logging.getLogger(__name__)

# keccak256("PairCreated(address,address,address,uint256)")
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"


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
