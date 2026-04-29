import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import Web3

import config
from analyzer import get_bnb_price_sync, get_liquidity_usd_sync

log = logging.getLogger(__name__)

FAST_POLL_INTERVAL = 1.0   # first FAST_POLL_COUNT attempts every 1 s
FAST_POLL_COUNT    = 15    # then switch to BSC block time
SLOW_POLL_INTERVAL = 3.0   # ~1 block on BSC
EXECUTION_TTL      = 300   # max seconds from enqueue before giving up
BUY_RETRY_LIMIT    = 2

# keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


@dataclass
class Candidate:
    token_address:  str
    base_token:     str
    pair_address:   str
    info:           dict
    creation_block: int
    enqueued_at:    float = field(default_factory=time.time)


class ExecutionEngine:
    """
    Receives vetted token candidates and executes buys in the first block
    where liquidity meets the minimum threshold.

    Discovery (watcher + analyzer) and Execution are fully decoupled:
    the engine never fires before the market is ready.
    """

    def __init__(self, w3: Web3, trader, pos_manager, notify_fn):
        self.w3          = w3
        self.trader      = trader
        self.pos_manager = pos_manager
        self.notify      = notify_fn
        self._queue: asyncio.Queue[Candidate] = asyncio.Queue()

    async def enqueue(self, candidate: Candidate):
        await self._queue.put(candidate)
        log.info(
            f"Queued: {candidate.token_address} "
            f"({candidate.info.get('symbol')}) — waiting for liquidity"
        )

    async def run(self):
        """Background task — each candidate is handled in its own coroutine."""
        while True:
            candidate = await self._queue.get()
            asyncio.create_task(self._handle(candidate))

    # ── Core flow ─────────────────────────────────────────────────────────────

    async def _handle(self, candidate: Candidate):
        sym = candidate.info.get("symbol", "???")

        liq = await self._wait_for_liquidity(candidate)
        if liq is None:
            await self.notify(f"⏰ *{sym}* — ликвидность не появилась, пропускаем.")
            return

        if not await self._pre_buy_checks(sym, candidate.token_address):
            return

        result = await self._buy_with_retry(candidate)

        buy_block    = await asyncio.to_thread(lambda: self.w3.eth.block_number)
        delta_blocks = buy_block - candidate.creation_block
        swaps_before = await self._count_swaps_before(
            candidate.pair_address, candidate.creation_block, buy_block
        )

        await self._on_buy_result(result, candidate, liq, delta_blocks, swaps_before)

    # ── Guards ────────────────────────────────────────────────────────────────

    async def _pre_buy_checks(self, sym: str, addr: str) -> bool:
        if addr in self.pos_manager.positions:
            await self.notify(f"⚠️ Позиция по *{sym}* уже открыта.")
            return False
        if len(self.pos_manager.positions) >= config.MAX_POSITIONS:
            await self.notify(
                f"🚫 Лимит позиций ({config.MAX_POSITIONS}) — *{sym}* пропущен."
            )
            return False
        if not self.trader.has_enough_bnb(config.BUY_AMOUNT_BNB):
            await self.notify(f"💸 Недостаточно BNB для *{sym}*.")
            return False
        return True

    # ── Wait for liquidity ────────────────────────────────────────────────────

    async def _wait_for_liquidity(self, candidate: Candidate) -> Optional[float]:
        """
        Poll pair reserves with fast interval first, then block-time.
        Returns USD liquidity once threshold is met, or None on timeout.
        """
        sym       = candidate.info.get("symbol", "???")
        bnb_price = await asyncio.to_thread(get_bnb_price_sync, self.w3)
        attempt   = 0

        while True:
            if time.time() - candidate.enqueued_at > EXECUTION_TTL:
                log.info(f"[{sym}] Execution TTL ({EXECUTION_TTL}s) expired")
                return None

            liq = await asyncio.to_thread(
                get_liquidity_usd_sync,
                self.w3,
                candidate.pair_address,
                candidate.base_token,
                bnb_price,
            )

            if liq >= config.MIN_LIQUIDITY_USD:
                log.info(f"[{sym}] Liquidity ready: ${liq:,.0f} (attempt {attempt + 1})")
                return liq

            interval = FAST_POLL_INTERVAL if attempt < FAST_POLL_COUNT else SLOW_POLL_INTERVAL
            attempt += 1
            await asyncio.sleep(interval)

    # ── Buy with retries ──────────────────────────────────────────────────────

    async def _buy_with_retry(self, candidate: Candidate) -> dict:
        sym  = candidate.info.get("symbol", "???")
        last: dict = {"ok": False, "reason": "no attempts"}

        for attempt in range(BUY_RETRY_LIMIT):
            result = await asyncio.to_thread(
                self.trader.buy, candidate.token_address, config.BUY_AMOUNT_BNB
            )
            if result["ok"]:
                return result
            last = result
            log.warning(
                f"[{sym}] Buy attempt {attempt + 1}/{BUY_RETRY_LIMIT} failed: "
                f"{result['reason']}"
            )
            if attempt < BUY_RETRY_LIMIT - 1:
                await asyncio.sleep(2)

        return last

    # ── Speed metrics ─────────────────────────────────────────────────────────

    async def _count_swaps_before(
        self, pair_address: str, from_block: int, to_block: int
    ) -> int:
        """Count Swap events on the pair between creation and our buy block."""
        try:
            logs = await asyncio.to_thread(
                self.w3.eth.get_logs,
                {
                    "address":   Web3.to_checksum_address(pair_address),
                    "topics":    [SWAP_TOPIC],
                    "fromBlock": from_block,
                    "toBlock":   to_block,
                },
            )
            return len(logs)
        except Exception as e:
            log.debug(f"Swap count error for {pair_address}: {e}")
            return -1

    # ── Post-buy ──────────────────────────────────────────────────────────────

    async def _on_buy_result(
        self,
        result: dict,
        candidate: Candidate,
        liq_usd: float,
        delta_blocks: int,
        swaps_before: int,
    ):
        from position import Position

        sym  = candidate.info.get("symbol", "???")
        addr = candidate.token_address

        if result["ok"]:
            tokens = result["tokens_received"]
            decs   = result["decimals"]

            price_entry = await asyncio.to_thread(
                self.trader.get_price, addr, candidate.base_token
            )
            if price_entry <= 0 and tokens > 0:
                price_entry = config.BUY_AMOUNT_BNB / (tokens / 10 ** decs)

            pos = Position(
                token_address     = addr,
                symbol            = sym,
                name              = candidate.info.get("name", sym),
                pair_address      = candidate.pair_address,
                buy_price_bnb     = price_entry,
                tokens_amount     = tokens,
                decimals          = decs,
                buy_bnb           = config.BUY_AMOUNT_BNB,
                take_profit_1     = config.TAKE_PROFIT_1,
                take_profit_1_pct = config.TAKE_PROFIT_1_PCT,
                take_profit_2     = config.TAKE_PROFIT_2,
                stop_loss         = config.STOP_LOSS,
            )
            self.pos_manager.add(pos)

            asyncio.create_task(
                asyncio.to_thread(self.trader.approve_token, addr)
            )

            swaps_str = str(swaps_before) if swaps_before >= 0 else "?"
            await self.notify(
                f"✅ *Куплено!* — {sym}\n\n"
                f"Получено: {tokens / 10**decs:.4f} {sym}\n"
                f"Цена входа: {price_entry:.8f} BNB\n"
                f"Ликвидность: ${liq_usd:,.0f}\n"
                f"⚡ Блоков после создания пары: *{delta_blocks}*\n"
                f"🏁 Свапов до нас: *{swaps_str}*\n"
                f"Tx: `{result['tx_hash']}`\n\n"
                f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%\n"
                f"TP2: +{config.TAKE_PROFIT_2}% → остаток  |  SL: -{config.STOP_LOSS}%"
            )
            log.info(
                f"[{sym}] BUY OK | delta_blocks={delta_blocks} | "
                f"swaps_before={swaps_before} | liq=${liq_usd:,.0f} | "
                f"tx={result['tx_hash']}"
            )
        else:
            await self.notify(f"❌ *Ошибка покупки* — {sym}\n{result['reason']}")
            log.error(f"[{sym}] BUY FAILED: {result['reason']}")
