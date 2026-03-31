import asyncio
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class Position:
    token_address:  str
    symbol:         str
    name:           str
    pair_address:   str
    buy_price_bnb:  float   # price of 1 token in BNB at entry
    tokens_amount:  int     # total raw amount bought (with decimals)
    decimals:       int
    buy_bnb:        float   # BNB spent
    take_profit_1:  float   # % gain to trigger first partial exit
    take_profit_1_pct: float  # % of position to sell at TP1 (e.g. 50)
    take_profit_2:  float   # % gain to trigger full exit
    stop_loss:      float   # % loss to cut position
    tp1_done:       bool = field(default=False)   # True after first exit executed


class PositionManager:
    def __init__(self, trader, notify_fn):
        self.trader    = trader
        self.notify    = notify_fn
        self.positions: dict[str, Position] = {}

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add(self, pos: Position):
        self.positions[pos.token_address] = pos
        log.info(
            f"Position opened: {pos.symbol} | "
            f"entry={pos.buy_price_bnb:.8f} BNB | "
            f"amount={pos.tokens_amount / 10**pos.decimals:.4f} | "
            f"TP1=+{pos.take_profit_1}% ({pos.take_profit_1_pct}%) | "
            f"TP2=+{pos.take_profit_2}% | SL=-{pos.stop_loss}%"
        )

    def remove(self, token_address: str):
        self.positions.pop(token_address, None)

    def get_all(self) -> list[Position]:
        return list(self.positions.values())

    # ── Background TP/SL monitor ──────────────────────────────────────────────

    async def monitor(self):
        """Check all open positions every 30 seconds and trigger TP1/TP2/SL."""
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

                    pnl_pct = (current_price - pos.buy_price_bnb) / pos.buy_price_bnb * 100

                    # ── TP2: full exit ────────────────────────────────────────
                    if pnl_pct >= pos.take_profit_2:
                        log.info(f"TP2 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                        await self._close_full(pos, pnl_pct, reason="TP2")

                    # ── TP1: partial exit (only once) ─────────────────────────
                    elif pnl_pct >= pos.take_profit_1 and not pos.tp1_done:
                        log.info(f"TP1 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                        await self._close_partial(pos, pnl_pct)

                    # ── SL: cut loss ──────────────────────────────────────────
                    elif pnl_pct <= -pos.stop_loss:
                        log.info(f"SL hit for {pos.symbol}: {pnl_pct:.1f}%")
                        await self._close_full(pos, pnl_pct, reason="SL")

                except Exception as e:
                    log.error(f"Monitor error for {token_addr}: {e}")

    # ── Execution helpers ─────────────────────────────────────────────────────

    async def _close_partial(self, pos: Position, pnl_pct: float):
        """Sell TP1_PCT% of the position at TP1."""
        sell_amount = int(pos.tokens_amount * pos.take_profit_1_pct / 100)
        result = await asyncio.to_thread(
            self.trader.sell, pos.token_address, sell_amount
        )
        if result["ok"]:
            pos.tp1_done      = True
            pos.tokens_amount = pos.tokens_amount - sell_amount  # remaining tokens
            await self.notify(
                f"🟡 *TP1 сработал — {pos.symbol}*\n"
                f"Продано *{pos.take_profit_1_pct:.0f}%* позиции\n"
                f"P&L: +{pnl_pct:.1f}%\n"
                f"Остаток идёт на TP2 (+{pos.take_profit_2}%)\n"
                f"Tx: `{result['tx_hash']}`"
            )
        else:
            await self.notify(
                f"⚠️ Ошибка TP1 для *{pos.symbol}*: {result['reason']}"
            )

    async def _close_full(self, pos: Position, pnl_pct: float, reason: str):
        """Sell all remaining tokens (TP2 or SL)."""
        result = await asyncio.to_thread(
            self.trader.sell, pos.token_address, pos.tokens_amount
        )
        if result["ok"]:
            if reason == "TP2":
                emoji = "✅"
                label = "TP2 сработал"
            else:
                emoji = "🛑"
                label = "SL сработал"
            await self.notify(
                f"{emoji} *{label} — {pos.symbol}*\n"
                f"P&L: {pnl_pct:+.1f}%\n"
                f"Потрачено: {pos.buy_bnb} BNB\n"
                f"Tx: `{result['tx_hash']}`"
            )
            self.remove(pos.token_address)
        else:
            await self.notify(
                f"⚠️ Ошибка закрытия *{pos.symbol}*: {result['reason']}"
            )
