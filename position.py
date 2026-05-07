import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime

import pytz

log = logging.getLogger(__name__)

MOSCOW_TZ      = pytz.timezone("Europe/Moscow")
DATA_DIR       = os.getenv("DATA_DIR", "/data")
DEMO_LOG_FILE  = os.path.join(DATA_DIR, "tysmith_demo_trades.json")


@dataclass
class Position:
    token_address:  str
    symbol:         str
    name:           str
    pair_address:   str
    buy_price_bnb:  float   # price of 1 token in BNB at entry
    tokens_amount:  int     # total raw amount bought (with decimals)
    decimals:       int
    buy_bnb:        float   # BNB spent (virtual in demo mode)
    take_profit_1:  float   # % gain to trigger first partial exit
    take_profit_1_pct: float  # % of position to sell at TP1 (e.g. 50)
    take_profit_2:  float   # % gain to trigger full exit
    stop_loss:      float   # % loss to cut position
    tp1_done:       bool = field(default=False)   # True after first exit executed
    demo:           bool = field(default=False)   # True = paper trade, no real tx


class PositionManager:
    def __init__(self, trader, notify_fn):
        self.trader    = trader
        self.notify    = notify_fn
        self.positions: dict[str, Position] = {}
        self._demo_trades: list[dict] = self._load_demo_trades()

    # -- CRUD --

    def add(self, pos: Position):
        self.positions[pos.token_address] = pos
        mode = "[DEMO] " if pos.demo else ""
        log.info(
            f"{mode}Position opened: {pos.symbol} | "
            f"entry={pos.buy_price_bnb:.8f} BNB | "
            f"amount={pos.tokens_amount / 10**pos.decimals:.4f} | "
            f"TP1=+{pos.take_profit_1}% ({pos.take_profit_1_pct}%) | "
            f"TP2=+{pos.take_profit_2}% | SL=-{pos.stop_loss}%"
        )

    def remove(self, token_address: str):
        self.positions.pop(token_address, None)

    def get_all(self) -> list[Position]:
        return list(self.positions.values())

    # -- Demo statistics --

    def _load_demo_trades(self) -> list[dict]:
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(DEMO_LOG_FILE) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_demo_trades(self):
        try:
            with open(DEMO_LOG_FILE, "w") as f:
                json.dump(self._demo_trades, f, indent=2)
        except Exception as e:
            log.warning(f"Demo trades save failed: {e}")

    def _record_demo_trade(self, pos: Position, pnl_pct: float, reason: str, sell_price: float):
        self._demo_trades.append({
            "symbol":      pos.symbol,
            "pnl_pct":     round(pnl_pct, 2),
            "reason":      reason,
            "buy_bnb":     pos.buy_bnb,
            "entry_price": pos.buy_price_bnb,
            "exit_price":  sell_price,
            "closed_at":   datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M"),
        })
        self._save_demo_trades()

    def get_demo_stats(self) -> dict:
        trades = self._demo_trades
        if not trades:
            return {"total": 0}

        wins   = [t for t in trades if t["pnl_pct"] > 0]
        losses = [t for t in trades if t["pnl_pct"] <= 0]
        avg    = sum(t["pnl_pct"] for t in trades) / len(trades)
        best   = max(trades, key=lambda t: t["pnl_pct"])
        worst  = min(trades, key=lambda t: t["pnl_pct"])

        # Virtual BNB P&L: sum of (pnl_pct / 100) * buy_bnb for each trade
        total_invested = sum(t["buy_bnb"] for t in trades)
        total_pnl_bnb  = sum(t["pnl_pct"] / 100 * t["buy_bnb"] for t in trades)

        return {
            "total":          len(trades),
            "wins":           len(wins),
            "losses":         len(losses),
            "win_rate":       len(wins) / len(trades) * 100,
            "avg_pnl":        avg,
            "best":           best,
            "worst":          worst,
            "total_invested": total_invested,
            "total_pnl_bnb":  total_pnl_bnb,
        }

    def reset_demo_stats(self):
        self._demo_trades = []
        try:
            os.remove(DEMO_LOG_FILE)
        except FileNotFoundError:
            pass

    # -- Background TP/SL monitor --

    async def monitor(self):
        """Check all open positions every 5 seconds and trigger TP1/TP2/SL."""
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

                    if pnl_pct >= pos.take_profit_2:
                        log.info(f"TP2 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                        await self._close_full(pos, pnl_pct, reason="TP2", current_price=current_price)

                    elif pnl_pct >= pos.take_profit_1 and not pos.tp1_done:
                        log.info(f"TP1 hit for {pos.symbol}: +{pnl_pct:.1f}%")
                        await self._close_partial(pos, pnl_pct, current_price=current_price)

                    elif pnl_pct <= -pos.stop_loss:
                        log.info(f"SL hit for {pos.symbol}: {pnl_pct:.1f}%")
                        await self._close_full(pos, pnl_pct, reason="SL", current_price=current_price)

                except Exception as e:
                    log.error(f"Monitor error for {token_addr}: {e}")

    # -- Execution helpers --

    async def _close_partial(self, pos: Position, pnl_pct: float, current_price: float = 0.0):
        """Sell TP1_PCT% of the position at TP1."""
        sell_amount = int(pos.tokens_amount * pos.take_profit_1_pct / 100)
        prefix = "🎭 [DEMO] " if pos.demo else ""

        if pos.demo:
            pos.tp1_done      = True
            pos.tokens_amount = pos.tokens_amount - sell_amount
            await self.notify(
                f"{prefix}*TP1 сработал — {pos.symbol}*\n"
                f"Продано *{pos.take_profit_1_pct:.0f}%* позиции (виртуально)\n"
                f"P&L: +{pnl_pct:.1f}%\n"
                f"Остаток идёт на TP2 (+{pos.take_profit_2}%)"
            )
        else:
            result = await asyncio.to_thread(
                self.trader.sell, pos.token_address, sell_amount
            )
            if result["ok"]:
                pos.tp1_done      = True
                pos.tokens_amount = pos.tokens_amount - sell_amount
                await self.notify(
                    f"*TP1 сработал — {pos.symbol}*\n"
                    f"Продано *{pos.take_profit_1_pct:.0f}%* позиции\n"
                    f"P&L: +{pnl_pct:.1f}%\n"
                    f"Остаток идёт на TP2 (+{pos.take_profit_2}%)\n"
                    f"Tx: `{result['tx_hash']}`"
                )
            else:
                await self.notify(
                    f"Warning: Ошибка TP1 для *{pos.symbol}*: {result['reason']}"
                )

    async def _close_full(self, pos: Position, pnl_pct: float, reason: str, current_price: float = 0.0):
        """Sell all remaining tokens (TP2 or SL)."""
        prefix = "🎭 [DEMO] " if pos.demo else ""

        if reason == "TP2":
            emoji, label = "✅", "TP2 сработал"
        else:
            emoji, label = "🛑", "SL сработал"

        if pos.demo:
            self._record_demo_trade(pos, pnl_pct, reason, current_price)
            self.remove(pos.token_address)
            await self.notify(
                f"{prefix}*{label} — {pos.symbol}*\n"
                f"P&L: {pnl_pct:+.1f}%\n"
                f"Вложено: {pos.buy_bnb} BNB (виртуально)\n"
                f"Прибыль: {pos.buy_bnb * pnl_pct / 100:+.4f} BNB виртуально"
            )
        else:
            result = await asyncio.to_thread(
                self.trader.sell, pos.token_address, pos.tokens_amount
            )
            if result["ok"]:
                await self.notify(
                    f"{emoji} *{label} — {pos.symbol}*\n"
                    f"P&L: {pnl_pct:+.1f}%\n"
                    f"Потрачено: {pos.buy_bnb} BNB\n"
                    f"Tx: `{result['tx_hash']}`"
                )
                self.remove(pos.token_address)
            else:
                await self.notify(
                    f"Warning: Ошибка закрытия *{pos.symbol}*: {result['reason']}"
                )
