"""
Ty Smith Crypto Signal Bot v2
Автономный трейдинговый советник для Telegram
Монеты: BTC, ETH, SOL, LINK | Интервал: каждый час

АНАЛИТИКА:
  - RSI + MACD + Bollinger Bands
  - Fear & Greed Index (реальный API)
  - BTC Dominance + общая капитализация рынка
  - Funding Rate (Binance Futures)
  - Объём ликвидаций (longs/shorts)
  - Volume Spike Alert
  - Итоговый score с учётом всех факторов
"""

import os
import asyncio
import logging
from datetime import datetime
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "8557968994:AAGzIC3Hd00UVAr-zliHcovtYAg_WOrSet0")
CHAT_ID   = os.getenv("CHAT_ID",   "7675712715")

COINS = {
    "bitcoin":   {"symbol": "BTC",  "emoji": "₿",  "binance": "BTCUSDT"},
    "ethereum":  {"symbol": "ETH",  "emoji": "Ξ",  "binance": "ETHUSDT"},
    "solana":    {"symbol": "SOL",  "emoji": "◎",  "binance": "SOLUSDT"},
    "chainlink": {"symbol": "LINK", "emoji": "🔗", "binance": "LINKUSDT"},
}

INTERVAL_HOURS = 1
# ──────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# ИСТОЧНИКИ ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_prices() -> dict:
    ids = ",".join(COINS.keys())
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd"
        f"&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"
    )
    async with aiohttp.ClientSession() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()


async def fetch_ohlc(coin_id: str, days: int = 7) -> list:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days={days}"
    async with aiohttp.ClientSession() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()


async def fetch_fear_greed() -> dict:
    """
    Реальный Fear & Greed Index от alternative.me.
    0-20 = Extreme Fear, 21-40 = Fear, 41-60 = Neutral,
    61-80 = Greed, 81-100 = Extreme Greed
    """
    try:
        url = "https://api.alternative.me/fng/?limit=2"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        today     = data["data"][0]
        yesterday = data["data"][1]
        val  = int(today["value"])
        prev = int(yesterday["value"])
        label = today["value_classification"]
        delta = val - prev

        if val <= 20:   emoji = "🤯"
        elif val <= 40: emoji = "😨"
        elif val <= 60: emoji = "😐"
        elif val <= 80: emoji = "😊"
        else:           emoji = "😈"

        sign = "+" if delta >= 0 else ""
        return {"value": val, "label": label, "emoji": emoji,
                "delta": f"{sign}{delta}", "ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def fetch_btc_dominance() -> dict:
    """
    BTC Dominance — доля биткоина в общей капитализации рынка.
    >58%: альты слабеют. <46%: альт-сезон.
    """
    try:
        url = "https://api.coingecko.com/api/v3/global"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        dom        = data["data"]["market_cap_percentage"]["btc"]
        total_mcap = data["data"]["total_market_cap"]["usd"]
        total_vol  = data["data"]["total_volume"]["usd"]

        if dom > 58:    signal = "🔴 Доминация высокая — альты под давлением"
        elif dom > 52:  signal = "🟡 Доминация умеренная — выбирай монеты осторожно"
        elif dom > 46:  signal = "🟢 Нейтрально — альты могут расти"
        else:           signal = "🚀 Альт-сезон — широкий рост альткоинов вероятен"

        return {
            "dominance": round(dom, 1),
            "signal": signal,
            "total_mcap_B": round(total_mcap / 1e9, 0),
            "total_vol_B":  round(total_vol  / 1e9, 0),
            "ok": True,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def fetch_funding_rate(symbol: str) -> dict:
    """
    Funding Rate (Binance Futures) — ставка финансирования каждые 8 часов.
    Норма: 0.01%.
    >0.1%: рынок перегрет лонгами — риск каскадных ликвидаций.
    <-0.05%: перегрет шортами — возможен шорт-сквиз.
    """
    try:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=1"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        rate = float(data[0]["fundingRate"]) * 100

        if rate > 0.15:    interp = "🔴 Перегрев лонгов — высокий риск слива"
        elif rate > 0.05:  interp = "🟡 Лонги доминируют — умеренный риск"
        elif rate > -0.02: interp = "🟢 Нейтрально"
        elif rate > -0.08: interp = "🟡 Шорты доминируют — возможен сквиз"
        else:              interp = "🔴 Перегрев шортов — риск резкого роста"

        return {"rate": round(rate, 4), "interp": interp, "ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def fetch_liquidations(symbol: str) -> dict:
    """
    Принудительные ликвидации (Binance).
    Много лонг-ликвидаций = медвежье давление.
    Много шорт-ликвидаций = бычий сигнал (шорт-сквиз).
    """
    try:
        url = f"https://fapi.binance.com/fapi/v1/allForceOrders?symbol={symbol}&limit=50"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()

        longs_usd  = sum(float(x["origQty"]) * float(x["price"])
                         for x in data if x["side"] == "BUY")
        shorts_usd = sum(float(x["origQty"]) * float(x["price"])
                         for x in data if x["side"] == "SELL")

        def fmt_m(v): return f"${v/1e6:.1f}M"

        total = longs_usd + shorts_usd
        if total < 500_000:
            signal = "🟢 Тихо — ликвидаций мало"
        elif longs_usd > shorts_usd * 2:
            signal = "🔴 Много лонг-ликвидаций — медвежье давление"
        elif shorts_usd > longs_usd * 2:
            signal = "🟢 Много шорт-ликвидаций — бычий сигнал (сквиз)"
        else:
            signal = "🟡 Смешанные ликвидации — неопределённость"

        return {"longs": fmt_m(longs_usd), "shorts": fmt_m(shorts_usd),
                "signal": signal, "ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ
# ══════════════════════════════════════════════════════════════════════════════

def calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        (gains if d > 0 else losses).append(abs(d))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 1)


def calc_macd(closes: list):
    """MACD (12/26/9). Возвращает (macd, signal, histogram)."""
    def ema(data, n):
        k = 2 / (n + 1)
        e = [data[0]]
        for p in data[1:]:
            e.append(p * k + e[-1] * (1 - k))
        return e
    if len(closes) < 26:
        return 0, 0, 0
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    macd_line = [a - b for a, b in zip(e12, e26)]
    sig = ema(macd_line, 9)
    return round(macd_line[-1], 2), round(sig[-1], 2), round(macd_line[-1] - sig[-1], 2)


def calc_bollinger(closes: list, period: int = 20):
    """
    Bollinger Bands (20, 2σ).
    Нижняя полоса = динамическая поддержка.
    Верхняя полоса = динамическое сопротивление.
    """
    if len(closes) < period:
        return None, None, None
    w = closes[-period:]
    mid = sum(w) / period
    std = (sum((x - mid) ** 2 for x in w) / period) ** 0.5
    return round(mid - 2 * std, 0), round(mid, 0), round(mid + 2 * std, 0)


def calc_volume_spike(volumes: list) -> dict:
    """
    Объём > 1.8x среднего за неделю = аномальный всплеск.
    Обычно предшествует сильному движению.
    """
    if len(volumes) < 5:
        return {"spike": False, "ratio": 1.0}
    avg = sum(volumes[:-1]) / len(volumes[:-1])
    ratio = volumes[-1] / avg if avg > 0 else 1.0
    return {"spike": ratio > 1.8, "ratio": round(ratio, 2)}


def generate_signal(change_24h, rsi, macd_hist, price,
                    funding_ok=False, funding_rate=0.0) -> dict:
    score = 0

    # RSI (вес: до ±3)
    if rsi < 30:      score += 3
    elif rsi < 40:    score += 2
    elif rsi < 50:    score += 1
    elif rsi > 75:    score -= 3
    elif rsi > 65:    score -= 2
    elif rsi > 55:    score -= 1

    # MACD (вес: ±1)
    score += 1 if macd_hist > 0 else -1

    # Momentum 24h (вес: до ±2)
    if change_24h > 5:    score += 2
    elif change_24h > 2:  score += 1
    elif change_24h < -5: score -= 2
    elif change_24h < -2: score -= 1

    # Funding Rate (вес: ±1)
    if funding_ok:
        if funding_rate > 0.1:    score -= 1
        elif funding_rate < -0.05: score += 1

    if score >= 4:
        action, conf = "🟢 ПОКУПАТЬ", "Высокая"
        target, stop = round(price * 1.07, 0), round(price * 0.95, 0)
    elif score >= 2:
        action, conf = "🔵 НАКАПЛИВАТЬ", "Умеренная"
        target, stop = round(price * 1.04, 0), round(price * 0.97, 0)
    elif score <= -4:
        action, conf = "🔴 ПРОДАВАТЬ", "Высокая"
        target, stop = round(price * 0.93, 0), round(price * 1.04, 0)
    elif score <= -2:
        action, conf = "🟠 ОСТОРОЖНО", "Умеренная"
        target, stop = round(price * 0.97, 0), round(price * 1.02, 0)
    else:
        action, conf = "⚪️ НЕЙТРАЛЬНО", "Низкая"
        target, stop = round(price * 1.02, 0), round(price * 0.98, 0)

    return {"action": action, "confidence": conf,
            "target": target, "stop": stop, "score": score}


# ══════════════════════════════════════════════════════════════════════════════
# СБОРКА ОТЧЁТА
# ══════════════════════════════════════════════════════════════════════════════

async def build_report() -> str:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    prices, fg, dom = await asyncio.gather(
        fetch_prices(),
        fetch_fear_greed(),
        fetch_btc_dominance(),
    )

    lines = [
        "📊 *TY SMITH SIGNAL REPORT v2*",
        f"🕐 {now} UTC",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "*🌍 РЫНОЧНЫЙ КОНТЕКСТ*",
    ]

    # Fear & Greed
    if fg.get("ok"):
        val = fg["value"]
        lines.append(
            f"  F&G: {fg['emoji']} *{val}/100* — {fg['label']}  (Δ {fg['delta']} за сутки)"
        )
        if val <= 25:
            lines.append("  _→ Экстремальный страх: история говорит — покупай_")
        elif val <= 45:
            lines.append("  _→ Страх: рынок пессимистичен, ищи точки входа_")
        elif val <= 55:
            lines.append("  _→ Нейтрально: жди подтверждения направления_")
        elif val <= 75:
            lines.append("  _→ Жадность: будь осторожен с новыми позициями_")
        else:
            lines.append("  _→ Экстремальная жадность: высокий риск коррекции_")

    # BTC Dominance
    if dom.get("ok"):
        lines.append(f"  BTC Dom: *{dom['dominance']}%*  — {dom['signal']}")
        lines.append(
            f"  Рынок: `${dom['total_mcap_B']:,.0f}B`  |  Объём: `${dom['total_vol_B']:,.0f}B`"
        )

    lines += ["", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]

    # ── МОНЕТЫ ────────────────────────────────────────────────────────────────
    for coin_id, meta in COINS.items():
        try:
            d      = prices.get(coin_id, {})
            price  = d.get("usd", 0)
            change = d.get("usd_24h_change", 0)
            vol    = d.get("usd_24h_vol", 0)
            mcap   = d.get("usd_market_cap", 0)
            sign   = "+" if change >= 0 else ""

            ohlc    = await fetch_ohlc(coin_id, days=7)
            closes  = [c[4] for c in ohlc] if ohlc else []
            volumes = [abs(c[2] - c[3]) * c[4] for c in ohlc] if ohlc else []

            rsi             = calc_rsi(closes)
            _, _, macd_hist = calc_macd(closes)
            bb_low, bb_mid, bb_high = calc_bollinger(closes)
            vol_spike       = calc_volume_spike(volumes)

            binance_sym = meta.get("binance", "")
            fr, liq = await asyncio.gather(
                fetch_funding_rate(binance_sym),
                fetch_liquidations(binance_sym),
            )

            sig = generate_signal(
                change, rsi, macd_hist, price,
                funding_ok=fr.get("ok", False),
                funding_rate=fr.get("rate", 0),
            )

            # Позиция в Bollinger
            if bb_low and bb_high and price:
                if price <= bb_low:
                    bb_pos = "🟢 У нижней полосы (поддержка)"
                elif price >= bb_high:
                    bb_pos = "🔴 У верхней полосы (сопротивление)"
                else:
                    pct = int((price - bb_low) / (bb_high - bb_low) * 100)
                    bb_pos = f"⚪️ В середине полосы ({pct}%)"
            else:
                bb_pos = "н/д"

            rsi_tag = "🔴 перекуплен" if rsi > 70 else ("🟢 перепродан" if rsi < 30 else "⚪️ норма")
            macd_tag = "↗️ бычий" if macd_hist > 0 else "↘️ медвежий"

            lines += [
                f"{meta['emoji']} *{meta['symbol']}*  `${price:,.0f}`  `{sign}{change:.2f}%`",
                "",
                f"  📐 *Технический анализ*",
                f"  RSI: `{rsi}` — {rsi_tag}",
                f"  MACD: `{macd_hist}` — {macd_tag}",
            ]

            if bb_low:
                lines.append(
                    f"  Bollinger: `${bb_low:,.0f}` ↔ `${bb_mid:,.0f}` ↔ `${bb_high:,.0f}`"
                )
                lines.append(f"  Позиция: {bb_pos}")

            if vol_spike["spike"]:
                lines.append(f"  ⚡️ Объём: `×{vol_spike['ratio']}` от нормы — жди импульс!")
            else:
                lines.append(f"  Объём: `×{vol_spike['ratio']}` от среднего (норма)")

            lines.append("")
            lines.append(f"  💹 *Деривативы (Binance Futures)*")

            if fr.get("ok"):
                lines.append(f"  Funding Rate: `{fr['rate']:+.4f}%`  {fr['interp']}")
                lines.append(f"  _норма: 0.01% / 8ч_")
            else:
                lines.append("  Funding Rate: недоступен")

            if liq.get("ok"):
                lines.append(f"  Ликвидации: Longs `{liq['longs']}` / Shorts `{liq['shorts']}`")
                lines.append(f"  {liq['signal']}")
            else:
                lines.append("  Ликвидации: недоступны")

            lines += [
                "",
                f"  🎯 *СИГНАЛ: {sig['action']}*  ({sig['confidence']})",
                f"  Score: `{sig['score']:+d}`  |  Цель: `${sig['target']:,.0f}`  |  Стоп: `${sig['stop']:,.0f}`",
                f"  Объём 24h: `${vol/1e6:.0f}M`  |  Кап: `${mcap/1e9:.1f}B`",
                "",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ]

        except Exception as e:
            lines.append(f"⚠️ Ошибка {meta['symbol']}: {e}\n")

    lines += [
        "*📋 ЛЕГЕНДА*",
        "  RSI <30 = перепродан 🟢 | >70 = перекуплен 🔴",
        "  Funding >0.1% = перегрев лонгов | <-0.05% = шорт-сквиз",
        "  Bollinger Low = поддержка | High = сопротивление",
        "  Ликвидации Shorts > Longs = бычий сигнал",
        "  Dominance >58% = альты слабые | <46% = альт-сезон",
        "",
        "_⚠️ Не является финансовой рекомендацией. DYOR._",
    ]

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════════════════════

async def send_signals():
    log.info("Генерируем отчёт...")
    try:
        text = await build_report()
        bot = Bot(token=BOT_TOKEN)
        if len(text) > 4000:
            text = text[:3990] + "\n\n_...сообщение обрезано_"
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN)
        log.info("Отчёт отправлен.")
    except Exception as e:
        log.error(f"Ошибка отправки: {e}")


async def main():
    log.info("Ty Smith Bot v2 запущен.")
    await send_signals()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_signals, "interval", hours=INTERVAL_HOURS)
    scheduler.start()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
