import os, asyncio, logging, io
from datetime import datetime
import pytz
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dashboard import generate_all_cards

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN = "8557968994:AAGzIC3Hd00UVAr-zliHcovtYAg_WOrSet0"
CHAT_ID   = "7675712715"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

COINS = {
    "bitcoin":   {"symbol": "BTC",  "bybit": "BTCUSDT"},
    "ethereum":  {"symbol": "ETH",  "bybit": "ETHUSDT"},
    "solana":    {"symbol": "SOL",  "bybit": "SOLUSDT"},
    "chainlink": {"symbol": "LINK", "bybit": "LINKUSDT"},
}

async def get(url):
    async with aiohttp.ClientSession(headers=HEADERS) as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json(content_type=None)

async def fetch_prices():
    ids = ",".join(COINS.keys())
    url = "https://api.coingecko.com/api/v3/simple/price"
    url += f"?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"
    return await get(url)

async def fetch_ohlc(coin_id):
    return await get(f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days=7")

async def fetch_fear_greed():
    try:
        data = await get("https://api.alternative.me/fng/?limit=2")
        t = data["data"][0]
        y = data["data"][1]
        val = int(t["value"])
        delta = val - int(y["value"])
        sign = "+" if delta >= 0 else ""
        return {"value": val, "label": t["value_classification"], "delta": f"{sign}{delta}", "ok": True}
    except Exception as e:
        log.error(f"FG error: {e}")
        return {"ok": False}

async def fetch_dominance():
    try:
        data = await get("https://api.coingecko.com/api/v3/global")
        dom  = data["data"]["market_cap_percentage"]["btc"]
        mcap = data["data"]["total_market_cap"]["usd"]
        vol  = data["data"]["total_volume"]["usd"]
        if dom > 58:
            sig = "Альты под давлением"
        elif dom > 52:
            sig = "Выбирай осторожно"
        elif dom > 46:
            sig = "Альты могут расти"
        else:
            sig = "Альт-сезон!"
        return {"dom": round(dom,1), "sig": sig, "mcap": round(mcap/1e9,0), "vol": round(vol/1e9,0), "ok": True}
    except Exception as e:
        log.error(f"DOM error: {e}")
        return {"ok": False}

async def fetch_funding_bybit(symbol):
    try:
        url  = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1"
        data = await get(url)
        rate = float(data["result"]["list"][0]["fundingRate"]) * 100
        if rate > 0.15:
            interp = "Перегрев лонгов"
        elif rate > 0.05:
            interp = "Лонги доминируют"
        elif rate > -0.02:
            interp = "Нейтрально"
        elif rate > -0.08:
            interp = "Шорты доминируют"
        else:
            interp = "Перегрев шортов"
        return {"rate": round(rate,4), "interp": interp, "ok": True, "source": "Bybit"}
    except:
        return {"ok": False}

async def fetch_funding_okx(symbol):
    try:
        okx_sym = symbol.replace("USDT", "-USDT-SWAP")
        data    = await get(f"https://www.okx.com/api/v5/public/funding-rate?instId={okx_sym}")
        rate    = float(data["data"][0]["fundingRate"]) * 100
        if rate > 0.15:
            interp = "Перегрев лонгов"
        elif rate > 0.05:
            interp = "Лонги доминируют"
        elif rate > -0.02:
            interp = "Нейтрально"
        elif rate > -0.08:
            interp = "Шорты доминируют"
        else:
            interp = "Перегрев шортов"
        return {"rate": round(rate,4), "interp": interp, "ok": True, "source": "OKX"}
    except:
        return {"ok": False}

async def fetch_funding(sym):
    r = await fetch_funding_bybit(sym)
    if r.get("ok"):
        return r
    r = await fetch_funding_okx(sym)
    if r.get("ok"):
        return r
    return {"ok": False}

def calc_rsi(closes, p=14):
    if len(closes) < p + 1:
        return 50.0
    g = []
    l = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0:
            g.append(abs(d))
        else:
            l.append(abs(d))
    ag = sum(g[-p:]) / p if g else 0.0
    al = sum(l[-p:]) / p if l else 0.0
    if al == 0:
        return 100.0
    return round(100 - (100 / (1 + ag/al)), 1)

def calc_macd(closes):
    def ema(d, n):
        k = 2 / (n + 1)
        e = [d[0]]
        for p in d[1:]:
            e.append(p * k + e[-1] * (1 - k))
        return e
    if len(closes) < 26:
        return 0, 0, 0
    m = [a - b for a, b in zip(ema(closes,12), ema(closes,26))]
    s = ema(m, 9)
    return round(m[-1],2), round(s[-1],2), round(m[-1]-s[-1],2)

def bollinger(closes, p=20):
    if len(closes) < p:
        return None, None, None
    w   = closes[-p:]
    mid = sum(w) / p
    std = (sum((x-mid)**2 for x in w) / p) ** 0.5
    return round(mid-2*std,0), round(mid,0), round(mid+2*std,0)

def rsi_label(v):
    if v > 70:
        return "перекуплен"
    elif v < 30:
        return "перепродан"
    elif v < 40 or v > 60:
        return "зона внимания"
    else:
        return "норма"

def generate_signal(change, r12, mh, price, fr=0, fr_ok=False):
    s = 0
    if r12 < 30:      s += 3
    elif r12 < 40:    s += 2
    elif r12 < 50:    s += 1
    elif r12 > 75:    s -= 3
    elif r12 > 65:    s -= 2
    elif r12 > 55:    s -= 1
    if mh > 0:        s += 1
    else:             s -= 1
    if change > 5:    s += 2
    elif change > 2:  s += 1
    elif change < -5: s -= 2
    elif change < -2: s -= 1
    if fr_ok:
        if fr > 0.1:      s -= 1
        elif fr < -0.05:  s += 1
    if s >= 4:
        action = "ПОКУПАТЬ"
        conf   = "Высокая"
        target = round(price * 1.07, 0)
        stop   = round(price * 0.95, 0)
    elif s >= 2:
        action = "НАКАПЛИВАТЬ"
        conf   = "Умеренная"
        target = round(price * 1.04, 0)
        stop   = round(price * 0.97, 0)
    elif s <= -4:
        action = "ПРОДАВАТЬ"
        conf   = "Высокая"
        target = round(price * 0.93, 0)
        stop   = round(price * 1.04, 0)
    elif s <= -2:
        action = "ОСТОРОЖНО"
        conf   = "Умеренная"
        target = round(price * 0.97, 0)
        stop   = round(price * 1.02, 0)
    else:
        action = "НЕЙТРАЛЬНО"
        conf   = "Низкая"
        target = round(price * 1.02, 0)
        stop   = round(price * 0.98, 0)
    return {"action": action, "conf": conf, "target": target, "stop": stop, "score": s}

async def collect_data():
    now_msk = datetime.now(MOSCOW_TZ)
    prices, fg, dom = await asyncio.gather(fetch_prices(), fetch_fear_greed(), fetch_dominance())
    log.info(f"Prices OK: {list(prices.keys()) if isinstance(prices, dict) else 'ERROR'}")
    log.info(f"FG: {fg}")
    log.info(f"DOM: {dom}")
    next_hour = (now_msk.hour + 1) % 24
    result = {
        "time":      now_msk.strftime("%d.%m.%Y %H:%M"),
        "next_hour": f"{next_hour:02d}:00",
        "fg":        fg,
        "dom":       dom,
        "coins":     [],
    }
    for coin_id, meta in COINS.items():
        try:
            d      = prices.get(coin_id, {})
            price  = d.get("usd", 0)
            change = d.get("usd_24h_change", 0)
            vol    = d.get("usd_24h_vol", 0)
            mcap   = d.get("usd_market_cap", 0)
            log.info(f"{meta['symbol']}: price={price} change={change}")
            ohlc   = await fetch_ohlc(coin_id)
            closes = [c[4] for c in ohlc] if ohlc else []
            r6     = calc_rsi(closes, 6)
            r12    = calc_rsi(closes, 12)
            r24    = calc_rsi(closes, 24)
            _, _, mh   = calc_macd(closes)
            bl, bm, bh = bollinger(closes)
            fr     = await fetch_funding(meta["bybit"])
            sig    = generate_signal(change, r12, mh, price, fr=fr.get("rate",0), fr_ok=fr.get("ok",False))
            if bl and bh and price and (bh - bl) > 0:
                pct = int((price-bl)/(bh-bl)*100)
                if price <= bl:
                    bp = "У нижней полосы"
                elif price >= bh:
                    bp = "У верхней полосы"
                else:
                    bp = f"Середина {pct}%"
            else:
                bp = "н/д"
            coin_data = {
                "symbol":         meta["symbol"],
                "price":          price,
                "change":         change,
                "vol":            vol,
                "mcap":           mcap,
                "rsi6":           r6,
                "rsi12":          r12,
                "rsi24":          r24,
                "macd":           mh,
                "bb_pos":         bp,
                "funding_rate":   fr.get("rate") if fr.get("ok") else None,
                "funding_src":    fr.get("source",""),
                "funding_interp": fr.get("interp",""),
                "action":         sig["action"],
                "conf":           sig["conf"],
                "target":         sig["target"],
                "stop":           sig["stop"],
                "score":          sig["score"],
            }
            result["coins"].append(coin_data)
            log.info(f"{meta['symbol']} data collected OK")
        except Exception as e:
            log.error(f"Ошибка {meta['symbol']}: {e}")
    return result

def build_text_report(data):
    L   = []
    fg  = data["fg"]
    dom = data["dom"]
    L.append("*TY SMITH SIGNAL REPORT v3*")
    L.append(f"Время: {data['time']} МСК")
    L.append("")
    if fg.get("ok"):
        v = fg["value"]
        L.append(f"Fear & Greed: *{v}/100* — {fg['label']} (delta {fg['delta']})")
    if dom.get("ok"):
        L.append(f"BTC Dom: *{dom['dom']}%* — {dom['sig']}")
    L.append("")
    for c in data["coins"]:
        sign = "+" if c["change"] >= 0 else ""
        L.append(f"*{c['symbol']}*  ${c['price']:,.0f}  {sign}{c['change']:.2f}%")
        L.append(f"  RSI 6/12/24: {c['rsi6']} / {c['rsi12']} / {c['rsi24']}")
        L.append(f"  MACD: {c['macd']}  |  {c['bb_pos']}")
        if c["funding_rate"] is not None:
            L.append(f"  Funding ({c['funding_src']}): {c['funding_rate']:+.4f}% — {c['funding_interp']}")
        L.append(f"  *{c['action']}* (score {c['score']:+d})")
        L.append(f"  Цель: ${c['target']:,.0f}  Стоп: ${c['stop']:,.0f}")
        L.append("")
    next_hour = (datetime.now(MOSCOW_TZ).hour+1) % 24
    L.append(f"Следующий отчёт: *{next_hour:02d}:00 МСК*")
    L.append("Не является финансовой рекомендацией.")
    return "\n".join(L)

async def send_signals():
    log.info("Генерируем отчёт...")
    try:
        data = await collect_data()
        log.info(f"Coins collected: {len(data['coins'])}")

        cards = generate_all_cards(data)
        log.info(f"Cards generated: {len(cards)}")

        bot = Bot(token=BOT_TOKEN)

        if cards:
            for i, card_bytes in enumerate(cards):
                log.info(f"Sending card {i+1}, size={len(card_bytes)} bytes")
                await bot.send_photo(
                    chat_id=CHAT_ID,
                    photo=io.BytesIO(card_bytes)
                )
                await asyncio.sleep(1)
            log.info("All cards sent")
        else:
            log.error("No cards generated!")

        text = build_text_report(data)
        if len(text) > 4000:
            text = text[:3990] + "\n...обрезано"
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN)
        log.info("Text report sent")

    except Exception as e:
        log.error(f"Ошибка send_signals: {e}", exc_info=True)

async def main():
    log.info("Ty Smith Bot v3 запущен.")
    await send_signals()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_signals, "cron", hour="*", minute=0, timezone=MOSCOW_TZ)
    scheduler.start()
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен.")

if __name__ == "__main__":
    asyncio.run(main())
