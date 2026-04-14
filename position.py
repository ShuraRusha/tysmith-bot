import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field

log = logging.getLogger(__name__)

_DATA_DIR      = os.getenv("DATA_DIR", "/data")
POSITIONS_FILE = os.path.join(_DATA_DIR, "tysmith_positions.json")


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
    # Analytics metadata
    liquidity_usd:    float = field(default=0.0)
    buy_tax:          float = field(default=0.0)
    sell_tax:         float = field(default=0.0)
    opened_at:        float = field(default_factory=time.time)
    # Runtime state
    tp1_done:         bool  = field(default=False)
    peak_price:       float = field(default=0.0)
    sell_failures:    int   = field(default=0)
    stuck:            bool  = field(default=False)


class PositionManager:
    def __init__(self, trader, notify_fn):
        self.trader    = trader
        self.notify    = notify_fn
        self.positions: dict[str, Position] = {}
        self.on_close  = None  # optional callback(pos, pnl_pct, reason, sell_price)

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save(self):
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            data = {addr: asdict(pos) for addr, pos in self.positions.items()}
            with open(POSITIONS_FILE, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Failed to save positions: {e}")

    def load(self) -> int:
        """Load positions from disk. Returns number of restored positions."""
        try:
            if not os.path.exists(POSITIONS_FILE):
                return 0
            with open(POSITIONS_FILE) as f:
                data = json.load(f)
            for addr, d in data.items():
                try:
                    self.positions[addr] = Position(**d)
                except Exception as e:
                    log.warning(f"Skipping corrupt position {addr}: {e}")
            count = len(self.positions)
            if count:
                log.info(f"Restored {count} open position(s) from disk")
            return count
        except Exception as e:
            log.warning(f"Could not load positions: {e}")
            return 0

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add(self, pos: Position):
        if pos.peak_price == 0.0:
            pos.peak_price = pos.buy_price_bnb
        self.positions[pos.token_address] = pos
        self._save()
        log.info(
            f"Position opened: {pos.symbol} | "
            f"entry={pos.buy_price_bnb:.8f} BNB | "
            f"TP1=+{pos.take_profit_1}% ({pos.take_profit_1_pct}%) | "
            f"TrailingStop={pos.trailing_stop_pct}% below peak | "
            f"SL=-{pos.stop_loss}%"
        )

    def remove(self, token_address: str):
        self.positions.pop(token_address, None)
        self._save()

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
                    if pos.stuck:
                        continue   # honeypot / unsellable — stop retrying

                    current_price = await asyncio.to_thread(
                        self.trader.get_price, token_addr
                    )
                    if current_price <= 0 or pos.buy_price_bnb <= 0:
                        continue

                    if current_price > pos.peak_price:
                        pos.peak_price = current_price
                        self._save()

                    pnl_pct = (current_price - pos.buy_price_bnb) / pos.buy_price_bnb * 100

                    if not pos.tp1_done:
                        if pnl_pct >= pos.take_profit_1:
                            log.info(f"TP1 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                            await self._close_partial(pos, pnl_pct)
                        elif pnl_pct <= -pos.stop_loss:
                            log.info(f"SL hit for {pos.symbol}: {pnl_pct:.1f}%")
                            await self._close_full(pos, pnl_pct, reason="SL", sell_price=current_price)
                    else:
                        drop_from_peak = (pos.peak_price - current_price) / pos.peak_price * 100
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
            self._save()
            await self.notify(
                f"🟡 *TP1 — {pos.symbol}*\n"
                f"Продано *{pos.take_profit_1_pct:.0f}%* позиции при +{pnl_pct:.1f}%\n"
                f"Остаток: trailing stop активен "
                f"({pos.trailing_stop_pct}% ниже пика)\n"
                f"Tx: `{result['tx_hash']}`"
            )
        else:
            await self._handle_sell_failure(pos, result["reason"])

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
            await self._handle_sell_failure(pos, result["reason"])

    async def _handle_sell_failure(self, pos: Position, reason: str):
        """Track consecutive sell failures; mark position stuck after 5 attempts."""
        MAX_FAILURES = 5
        pos.sell_failures += 1
        self._save()
        log.warning(f"Sell failure #{pos.sell_failures} for {pos.symbol}: {reason}")

        if pos.sell_failures >= MAX_FAILURES:
            pos.stuck = True
            self._save()
            log.error(f"{pos.symbol} marked STUCK after {MAX_FAILURES} sell failures")
            await self.notify(
                f"🚫 *{pos.symbol}* — продажа невозможна\n\n"
                f"Продажа отклонена контрактом *{MAX_FAILURES} раз подряд*.\n"
                f"Скорее всего это *honeypot* — токен можно купить, но нельзя продать.\n\n"
                f"Бот прекратил попытки.\n"
                f"Попробуй продать вручную на PancakeSwap (slippage 99%):\n"
                f"pancakeswap.finance → Swap → вставь адрес:\n"
                f"`{pos.token_address}`"
            )
        else:
            await self.notify(
                f"⚠️ Ошибка продажи *{pos.symbol}* ({pos.sell_failures}/{MAX_FAILURES}): "
                f"{reason}"
            )
