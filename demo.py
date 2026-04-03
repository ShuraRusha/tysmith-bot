"""
Demo trading mode — real token analysis, virtual money.

Uses live on-chain prices and real detection logic, but never
executes actual blockchain transactions.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

import pytz

log = logging.getLogger(__name__)
MOSCOW_TZ = pytz.timezone("Europe/Moscow")
DEMO_FILE  = "/tmp/tysmith_demo.json"


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class DemoPosition:
    token_address:    str
    symbol:           str
    name:             str
    buy_price_bnb:    float
    tokens_amount:    float   # virtual units (buy_bnb / buy_price_bnb)
    buy_bnb:          float
    take_profit_1:    float
    take_profit_1_pct: float
    trailing_stop_pct: float
    stop_loss:        float
    tp1_done:         bool  = field(default=False)
    peak_price:       float = field(default=0.0)
    opened_at:        str   = field(default="")


# ── Manager ───────────────────────────────────────────────────────────────────

class DemoManager:
    """
    Manages a virtual $1000 portfolio.

    All prices are fetched from the live blockchain so results
    reflect what real trades would have looked like.
    """

    def __init__(self, trader, notify_fn, initial_bnb: float):
        self.trader           = trader
        self.notify           = notify_fn
        self.initial_bnb      = initial_bnb
        self.balance_bnb      = initial_bnb
        self.positions: dict[str, DemoPosition] = {}
        self.trades:    list[dict]              = []
        self.enabled          = False
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self):
        try:
            if os.path.exists(DEMO_FILE):
                d = json.load(open(DEMO_FILE))
                self.enabled     = d.get("enabled", False)
                self.balance_bnb = d.get("balance_bnb", self.initial_bnb)
                self.initial_bnb = d.get("initial_bnb", self.initial_bnb)
                self.trades      = d.get("trades", [])
                for addr, p in d.get("positions", {}).items():
                    self.positions[addr] = DemoPosition(**p)
                log.info(f"Demo loaded: balance={self.balance_bnb:.4f} BNB, "
                         f"trades={len(self.trades)}, open={len(self.positions)}")
        except Exception as e:
            log.warning(f"Demo load error: {e}")

    def _save(self):
        try:
            data = {
                "enabled":     self.enabled,
                "balance_bnb": self.balance_bnb,
                "initial_bnb": self.initial_bnb,
                "trades":      self.trades[-500:],
                "positions":   {
                    addr: {
                        k: getattr(p, k) for k in DemoPosition.__dataclass_fields__
                    }
                    for addr, p in self.positions.items()
                },
            }
            json.dump(data, open(DEMO_FILE, "w"), ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Demo save error: {e}")

    # ── Virtual trading ───────────────────────────────────────────────────────

    def buy(
        self, token_address: str, symbol: str, name: str,
        buy_price_bnb: float, buy_bnb: float,
        take_profit_1: float, take_profit_1_pct: float,
        trailing_stop_pct: float, stop_loss: float,
    ) -> bool:
        if buy_bnb > self.balance_bnb or buy_price_bnb <= 0:
            return False
        self.balance_bnb -= buy_bnb
        pos = DemoPosition(
            token_address     = token_address,
            symbol            = symbol,
            name              = name,
            buy_price_bnb     = buy_price_bnb,
            tokens_amount     = buy_bnb / buy_price_bnb,
            buy_bnb           = buy_bnb,
            take_profit_1     = take_profit_1,
            take_profit_1_pct = take_profit_1_pct,
            trailing_stop_pct = trailing_stop_pct,
            stop_loss         = stop_loss,
            peak_price        = buy_price_bnb,
            opened_at         = datetime.now(MOSCOW_TZ).strftime("%H:%M %d.%m"),
        )
        self.positions[token_address] = pos
        self._save()
        return True

    def _sell_partial(self, pos: DemoPosition, sell_price: float, pct: float):
        """Sell `pct`% of position. Returns (pnl_bnb for this part)."""
        sell_tokens   = pos.tokens_amount * pct / 100
        sell_bnb      = sell_tokens * sell_price
        cost_portion  = pos.buy_bnb * pct / 100
        pnl_bnb       = sell_bnb - cost_portion
        self.balance_bnb      += sell_bnb
        pos.tokens_amount     -= sell_tokens
        pos.buy_bnb           -= cost_portion
        self._save()
        return pnl_bnb

    def _sell_full(self, pos: DemoPosition, sell_price: float, reason: str):
        """Sell all remaining tokens. Returns (pnl_pct, pnl_bnb)."""
        sell_bnb      = pos.tokens_amount * sell_price
        pnl_bnb       = sell_bnb - pos.buy_bnb
        pnl_pct       = pnl_bnb / pos.buy_bnb * 100 if pos.buy_bnb else 0
        self.balance_bnb += sell_bnb
        self.trades.append({
            "symbol":     pos.symbol,
            "buy_price":  pos.buy_price_bnb,
            "sell_price": sell_price,
            "buy_bnb":    pos.buy_bnb,
            "pnl_bnb":    round(pnl_bnb,  6),
            "pnl_pct":    round(pnl_pct,  2),
            "reason":     reason,
            "opened_at":  pos.opened_at,
            "closed_at":  datetime.now(MOSCOW_TZ).strftime("%H:%M %d.%m"),
        })
        del self.positions[pos.token_address]
        self._save()
        return pnl_pct, pnl_bnb

    # ── Background monitor ────────────────────────────────────────────────────

    async def monitor(self):
        """Check all demo positions every 5 s using live on-chain prices."""
        while True:
            await asyncio.sleep(5)
            if not self.enabled:
                continue
            for addr in list(self.positions):
                pos = self.positions.get(addr)
                if not pos:
                    continue
                try:
                    price = await asyncio.to_thread(self.trader.get_price, addr)
                    if price <= 0 or pos.buy_price_bnb <= 0:
                        continue

                    if price > pos.peak_price:
                        pos.peak_price = price

                    pnl_pct = (price - pos.buy_price_bnb) / pos.buy_price_bnb * 100

                    if not pos.tp1_done:
                        # Phase 1 — fixed TP1 and SL
                        if pnl_pct >= pos.take_profit_1:
                            pnl_bnb = self._sell_partial(pos, price, pos.take_profit_1_pct)
                            pos.tp1_done = True
                            self._save()
                            await self.notify(
                                f"📊 *\[ДЕМО\] TP1 — {pos.symbol}*\n"
                                f"Продано {pos.take_profit_1_pct:.0f}% при *+{pnl_pct:.1f}%*\n"
                                f"Часть P&L: *{pnl_bnb:+.4f} BNB*\n"
                                f"Остаток переходит на trailing stop"
                            )
                        elif pnl_pct <= -pos.stop_loss:
                            pnl_pct_r, pnl_bnb = self._sell_full(pos, price, "SL")
                            await self.notify(
                                f"📊 *\[ДЕМО\] 🛑 Стоп-лосс — {pos.symbol}*\n"
                                f"P&L: *{pnl_pct_r:+.1f}%* ({pnl_bnb:+.4f} BNB)\n"
                                f"Виртуальный баланс: {self.balance_bnb:.4f} BNB"
                            )
                    else:
                        # Phase 2 — trailing stop
                        drop = (pos.peak_price - price) / pos.peak_price * 100
                        if drop >= pos.trailing_stop_pct:
                            pnl_pct_r, pnl_bnb = self._sell_full(pos, price, "Trailing Stop")
                            await self.notify(
                                f"📊 *\[ДЕМО\] 🔒 Trailing Stop — {pos.symbol}*\n"
                                f"Пик: {pos.peak_price:.8f} | Сейчас: {price:.8f}\n"
                                f"P&L: *{pnl_pct_r:+.1f}%* ({pnl_bnb:+.4f} BNB)\n"
                                f"Виртуальный баланс: {self.balance_bnb:.4f} BNB"
                            )

                except Exception as e:
                    log.error(f"Demo monitor error {addr}: {e}")

    # ── Statistics ────────────────────────────────────────────────────────────

    def get_stats(self, bnb_price: float) -> dict:
        closed    = self.trades
        total     = len(closed)
        wins      = [t for t in closed if t["pnl_pct"] > 0]
        losses    = [t for t in closed if t["pnl_pct"] <= 0]
        closed_pnl = sum(t["pnl_bnb"] for t in closed)

        # Current portfolio value (balance + open positions marked to market)
        portfolio_bnb = self.balance_bnb   # open positions tracked separately

        pnl_total_bnb = portfolio_bnb - self.initial_bnb + closed_pnl
        pnl_total_pct = pnl_total_bnb / self.initial_bnb * 100 if self.initial_bnb else 0

        best  = max(closed, key=lambda t: t["pnl_pct"]) if closed else None
        worst = min(closed, key=lambda t: t["pnl_pct"]) if closed else None

        return {
            "balance_bnb":   portfolio_bnb,
            "balance_usd":   portfolio_bnb * bnb_price,
            "initial_bnb":   self.initial_bnb,
            "initial_usd":   self.initial_bnb * bnb_price,
            "pnl_bnb":       round(pnl_total_bnb, 6),
            "pnl_pct":       round(pnl_total_pct, 2),
            "pnl_usd":       round(pnl_total_bnb * bnb_price, 2),
            "total_trades":  total,
            "wins":          len(wins),
            "losses":        len(losses),
            "win_rate":      round(len(wins) / total * 100, 1) if total else 0,
            "open":          len(self.positions),
            "best":          best,
            "worst":         worst,
        }

    def reset(self, initial_bnb: float):
        self.initial_bnb  = initial_bnb
        self.balance_bnb  = initial_bnb
        self.positions    = {}
        self.trades       = []
        self._save()
