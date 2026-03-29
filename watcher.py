import asyncio
import json
import logging

import websockets
from web3 import Web3

from config import PANCAKE_FACTORY_V2, BASE_TOKENS

log = logging.getLogger(__name__)

# keccak256("PairCreated(address,address,address,uint256)")
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"


async def watch_pairs(ws_url: str, callback):
    """
    Subscribe to PancakeSwap V2 PairCreated events via WebSocket.
    Calls callback(token_address, base_token, pair_address) for each new
    token paired against WBNB / BUSD / USDT.
    Auto-reconnects with exponential backoff on any error.
    """
    backoff = 2
    while True:
        try:
            log.info(f"Connecting to BSC WebSocket: {ws_url}")
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
                log.info(f"Subscribed to PairCreated events (sub_id={sub_id})")
                backoff = 2  # reset on successful connection

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("method") != "eth_subscription":
                            continue

                        log_entry = msg["params"]["result"]
                        topics    = log_entry.get("topics", [])
                        if len(topics) < 3:
                            continue

                        # topics[1] and topics[2] are 32-byte padded addresses
                        token0 = Web3.to_checksum_address("0x" + topics[1][-40:])
                        token1 = Web3.to_checksum_address("0x" + topics[2][-40:])

                        # data = pair_address (32 bytes) + pair_index (32 bytes)
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
                            # both base tokens or neither — skip
                            continue

                        log.info(f"New pair detected: {new_token} / {base_token} @ {pair}")
                        asyncio.create_task(callback(new_token, base_token, pair))

                    except Exception as e:
                        log.warning(f"Event parse error: {e}")

        except Exception as e:
            log.error(f"WebSocket error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
