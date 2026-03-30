import asyncio
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class Position:
    token_address: str
    symbol:        str
    name:          str
    pair_address:  str
    buy_price_bnb: float   # price of 1 token in BNB at buy time
    tokens_amount: int     # raw amount including decimals
    decimals:      int
    buy_bnb:       float   # how much BNB was spent
    take_profit:   float   # %, e.g. 50 means +50%
    stop_loss:     float   # %, e.g. 25 means -25%


class PositionManager:
    def __init__(self, trader, notify_fn):
        """
        trader    — Trader instance (synchronous buy/sell/get_price)
        notify_fn — async callable(text: str) to send Telegram messages
        """
        self.trader    = trader
        self.notify    = notify_fn
        self.positions: dict[str, Position] = {}

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add(self, pos: Position):
        self.positions[pos.token_address] = pos
        log.info(
            f"Position opened: {pos.symbol} | "
            f"entry={pos.buy_price_bnb:.8f} BNB | "
            f"amount={pos.tokens_amount / 10**pos.decimals:.4f} tokens"
        )

    def remove(self, token_address: str):
        self.positions.pop(token_address, None)

    def get_all(self) -> list[Position]:
        return list(self.positions.values())

    # ── Background TP/SL monitor ──────────────────────────────────────────────

    async def monitor(self):
        """Check all open positions every 30 seconds and trigger TP/SL."""
        while True:
            await asyncio.sleep(30)
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

                    if pnl_pct >= pos.take_profit:
                        log.info(f"TP triggered for {pos.symbol}: {pnl_pct:+.1f}%")
                        await self._close(pos, pnl_pct, reason="TP")

                    elif pnl_pct <= -pos.stop_loss:
                        log.info(f"SL triggered for {pos.symbol}: {pnl_pct:+.1f}%")
                        await self._close(pos, pnl_pct, reason="SL")

                except Exception as e:
                    log.error(f"Monitor error for {token_addr}: {e}")

    async def _close(self, pos: Position, pnl_pct: float, reason: str):
        """Execute sell and notify user."""
        result = await asyncio.to_thread(
            self.trader.sell, pos.token_address, pos.tokens_amount
        )
        if result["ok"]:
            emoji = "✅" if pnl_pct >= 0 else "🛑"
            await self.notify(
                f"{emoji} *{reason} сработал — {pos.symbol}*\n"
                f"P&L: {pnl_pct:+.1f}%\n"
                f"Потрачено: {pos.buy_bnb} BNB\n"
                f"Tx: `{result['tx_hash']}`"
            )
            self.remove(pos.token_address)
        else:
            await self.notify(
                f"⚠️ Ошибка закрытия позиции *{pos.symbol}*\n{result['reason']}"
            )
