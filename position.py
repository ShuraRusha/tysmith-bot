import asyncio
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class Position:
    token_address:    str
    symbol:           str
    name:             str
    pair_address:     str
    buy_price_bnb:    float   # price of 1 token in BNB at entry
    tokens_amount:    int     # total raw amount bought (with decimals)
    decimals:         int
    buy_bnb:          float   # BNB spent
    take_profit_1:    float   # % gain to trigger partial exit
    take_profit_1_pct: float  # % of position to sell at TP1 (e.g. 25)
    trailing_stop_pct: float  # % drop from peak to trigger full exit (e.g. 10)
    stop_loss:        float   # % loss before TP1 to cut position (e.g. 15)
    # Runtime state
    tp1_done:         bool  = field(default=False)
    peak_price:       float = field(default=0.0)   # highest price seen — updated in monitor


class PositionManager:
    def __init__(self, trader, notify_fn):
        self.trader    = trader
        self.notify    = notify_fn
        self.positions: dict[str, Position] = {}
        self.on_close  = None  # optional callback(pos, pnl_pct, reason, sell_price)

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add(self, pos: Position):
        pos.peak_price = pos.buy_price_bnb  # initialise peak at entry
        self.positions[pos.token_address] = pos
        log.info(
            f"Position opened: {pos.symbol} | "
            f"entry={pos.buy_price_bnb:.8f} BNB | "
            f"TP1=+{pos.take_profit_1}% ({pos.take_profit_1_pct}%) | "
            f"TrailingStop={pos.trailing_stop_pct}% below peak | "
            f"SL=-{pos.stop_loss}%"
        )

    def remove(self, token_address: str):
        self.positions.pop(token_address, None)

    def get_all(self) -> list[Position]:
        return list(self.positions.values())

    # ── Background monitor ────────────────────────────────────────────────────

    async def monitor(self):
        """
        Check all open positions every 5 seconds.

        Exit logic:
          Phase 1 (before TP1):
            • price >= TP1  → partial sell (TP1_PCT%), switch to Phase 2
            • price <= -SL  → full sell (cut loss)

          Phase 2 (after TP1, trailing stop active):
            • price sets new peak  → update peak
            • drop from peak >= TRAILING_STOP_PCT → full sell (lock remaining gains)
        """
        while True:
            await asyncio.sleep(5)
            for token_addr in list(self.positions):
                pos = self.positions.get(token_addr)
                if not pos:
                    continue
                try:
                    current_price = await asyncio.to_thread(
                        self.trader.get_price, token_addr
                    )
                    if current_price <= 0 or pos.buy_price_bnb <= 0:
                        continue

                    # Always track the highest price seen
                    if current_price > pos.peak_price:
                        pos.peak_price = current_price

                    pnl_pct = (current_price - pos.buy_price_bnb) / pos.buy_price_bnb * 100

                    if not pos.tp1_done:
                        # ── Phase 1: fixed TP1 + fixed SL ────────────────────
                        if pnl_pct >= pos.take_profit_1:
                            log.info(f"TP1 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                            await self._close_partial(pos, pnl_pct)

                        elif pnl_pct <= -pos.stop_loss:
                            log.info(f"SL hit for {pos.symbol}: {pnl_pct:.1f}%")
                            await self._close_full(pos, pnl_pct, reason="SL", sell_price=current_price)

                    else:
                        # ── Phase 2: trailing stop on remaining tokens ─────────
                        drop_from_peak = (
                            (pos.peak_price - current_price) / pos.peak_price * 100
                        )
                        if drop_from_peak >= pos.trailing_stop_pct:
                            log.info(
                                f"Trailing stop hit for {pos.symbol}: "
                                f"peak={pos.peak_price:.8f}, now={current_price:.8f}, "
                                f"drop={drop_from_peak:.1f}%"
                            )
                            await self._close_full(pos, pnl_pct, reason="Trailing Stop", sell_price=current_price)

                except Exception as e:
                    log.error(f"Monitor error for {token_addr}: {e}")

    # ── Execution helpers ─────────────────────────────────────────────────────

    async def _close_partial(self, pos: Position, pnl_pct: float):
        """Sell TP1_PCT% at TP1 then activate trailing stop for remainder."""
        sell_amount = int(pos.tokens_amount * pos.take_profit_1_pct / 100)
        result = await asyncio.to_thread(
            self.trader.sell, pos.token_address, sell_amount
        )
        if result["ok"]:
            pos.tp1_done      = True
            pos.tokens_amount = pos.tokens_amount - sell_amount
            await self.notify(
                f"🟡 *TP1 — {pos.symbol}*\n"
                f"Продано *{pos.take_profit_1_pct:.0f}%* позиции при +{pnl_pct:.1f}%\n"
                f"Остаток: trailing stop активен "
                f"({pos.trailing_stop_pct}% ниже пика)\n"
                f"Tx: `{result['tx_hash']}`"
            )
        else:
            await self.notify(
                f"⚠️ Ошибка TP1 для *{pos.symbol}*: {result['reason']}"
            )

    async def _close_full(self, pos: Position, pnl_pct: float, reason: str, sell_price: float = 0.0):
        """Sell all remaining tokens."""
        result = await asyncio.to_thread(
            self.trader.sell, pos.token_address, pos.tokens_amount
        )
        if result["ok"]:
            labels = {
                "SL":             "🛑 Стоп-лосс",
                "Trailing Stop":  "🔒 Trailing Stop",
            }
            label = labels.get(reason, f"✅ {reason}")
            await self.notify(
                f"{label} — *{pos.symbol}*\n"
                f"P&L: {pnl_pct:+.1f}%\n"
                f"Потрачено: {pos.buy_bnb} BNB\n"
                f"Tx: `{result['tx_hash']}`"
            )
            if self.on_close:
                self.on_close(pos, pnl_pct, reason, sell_price)
            self.remove(pos.token_address)
        else:
            await self.notify(
                f"⚠️ Ошибка закрытия *{pos.symbol}*: {result['reason']}"
            )
