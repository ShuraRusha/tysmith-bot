import os, asyncio, logging, io, sys
from datetime import datetime
import pytz
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dashboard import generate_collage

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

async def get(url, retries=5):
    """Fetch JSON with automatic retry on 429 / transient errors."""
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession(headers=HEADERS) as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status == 429:
                        wait = 15 * (attempt + 1)
                        log.warning(f"Rate limit (429) for {url} — waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    r.raise_for_status()
                    return await r.json(content_type=None)
        except asyncio.TimeoutError:
            log.warning(f"Timeout on {url}, attempt {attempt+1}/{retries}")
            if attempt < retries - 1:
                await asyncio.sleep(8)
        except Exception as e:
            log.warning(f"Request error {url}: {e}, attempt {attempt+1}/{retries}")
            if attempt < retries - 1:
                await asyncio.sleep(8)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")

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

async def fetch_nupl():
    """NUPL (Net Unrealized Profit/Loss) — эмоциональное состояние рынка BTC.
    Источник: CoinMetrics community API (NUPLAdj).
    Диапазон: < 0 капитуляция … > 0.75 эйфория."""
    try:
        url = (
            "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
            "?assets=btc&metrics=NUPLAdj&frequency=1d&page_size=3"
        )
        data = await get(url)
        rows = data.get("data", [])
        if not rows:
            return {"ok": False}
        val = float(rows[-1]["NUPLAdj"])
        if val < 0:
            zone   = "Капитуляция"
            interp = "Экстремальная перепроданность — сильный вход"
        elif val < 0.25:
            zone   = "Надежда/Накопление"
            interp = "Рынок восстанавливается — хорошая зона"
        elif val < 0.5:
            zone   = "Оптимизм"
            interp = "Здоровый тренд — продолжение вероятно"
        elif val < 0.75:
            zone   = "Вера/Отрицание"
            interp = "Рынок горячий — сокращай позиции"
        else:
            zone   = "Эйфория"
            interp = "Экстремальная перекупленность — высокий риск"
        log.info(f"NUPL: {val:.3f} ({zone})")
        return {"ok": True, "value": round(val, 3), "zone": zone, "interp": interp}
    except Exception as e:
        log.error(f"NUPL error: {e}")
        return {"ok": False}

async def fetch_puell():
    """Puell Multiple — давление со стороны майнеров BTC.
    = дневная выручка майнеров / 365-дневная скользящая средняя выручки.
    Источник: blockchain.info charts API."""
    try:
        url = "https://api.blockchain.info/charts/miners-revenue?timespan=2years&format=json&cors=true"
        data = await get(url)
        values = data.get("values", [])
        if len(values) < 365:
            return {"ok": False}
        revenues = [v["y"] for v in values]
        today_rev = revenues[-1]
        ma365     = sum(revenues[-365:]) / 365
        if ma365 == 0:
            return {"ok": False}
        puell = today_rev / ma365
        if puell < 0.5:
            zone   = "Зона покупки"
            interp = "Майнеры в стрессе — дно близко"
        elif puell < 0.8:
            zone   = "Недооценка"
            interp = "Хорошая зона накопления"
        elif puell < 1.5:
            zone   = "Справедливая цена"
            interp = "Нейтральная зона"
        elif puell < 2.5:
            zone   = "Переоценка"
            interp = "Майнеры в прибыли — осторожно"
        else:
            zone   = "Зона продажи"
            interp = "Сильная перекупленность — высокий риск"
        log.info(f"Puell Multiple: {puell:.2f} ({zone})")
        return {"ok": True, "value": round(puell, 2), "zone": zone, "interp": interp}
    except Exception as e:
        log.error(f"Puell error: {e}")
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
        return 0, 0, 0, []
    m = [a - b for a, b in zip(ema(closes,12), ema(closes,26))]
    s = ema(m, 9)
    hist = [round(mv - sv, 4) for mv, sv in zip(m, s)]
    return round(m[-1],2), round(s[-1],2), round(m[-1]-s[-1],2), hist[-8:]

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

def generate_signal(change, r6, r12, r24, mh, macd_hist, bb_pct, price,
                    fr=0, fr_ok=False, nupl=None, puell=None):
    """
    Multi-factor signal scoring.

    RSI:          average of 3 timeframes + cross-timeframe trend  (-4 … +4)
    MACD:         histogram value + momentum direction              (-2 … +2)
    Bollinger:    price position within the band                    (-2 … +2)
    Funding:      extreme positioning warning                       (-2 … +1)
    Change:       24h momentum confirmation                         (-1 … +1)
    NUPL:         emotional state of the BTC market                 (-3 … +3)
    Puell:        miner revenue pressure                            (-2 … +2)
    """
    s = 0

    # ── RSI composite (average of 3 timeframes) ──────────────
    rsi_avg = (r6 + r12 + r24) / 3
    if   rsi_avg < 30: s += 3
    elif rsi_avg < 40: s += 2
    elif rsi_avg < 50: s += 1
    elif rsi_avg > 70: s -= 3
    elif rsi_avg > 60: s -= 2
    elif rsi_avg > 55: s -= 1

    # ── RSI trend direction (short-term vs long-term) ─────────
    if   r6 > r24: s += 1
    elif r6 < r24: s -= 1

    # ── MACD histogram: value + momentum direction ────────────
    macd_growing = len(macd_hist) >= 2 and macd_hist[-1] > macd_hist[-2]
    if mh > 0:
        s += 2 if macd_growing else 1
    else:
        s -= 2 if not macd_growing else 1

    # ── Bollinger Bands position ──────────────────────────────
    if   bb_pct < 15: s += 2
    elif bb_pct < 30: s += 1
    elif bb_pct > 85: s -= 2
    elif bb_pct > 70: s -= 1

    # ── Funding rate ──────────────────────────────────────────
    if fr_ok:
        if   fr >  0.10: s -= 2
        elif fr >  0.05: s -= 1
        elif fr < -0.05: s += 1

    # ── 24h price change (momentum confirmation) ──────────────
    if   change >  3: s += 1
    elif change < -3: s -= 1

    # ── NUPL: эмоциональное состояние рынка ──────────────────
    if nupl is not None:
        if   nupl < 0.00: s += 3   # Капитуляция — сильный вход
        elif nupl < 0.25: s += 2   # Надежда/Накопление
        elif nupl < 0.50: s += 1   # Оптимизм
        elif nupl < 0.75: s -= 1   # Вера/Отрицание
        else:             s -= 3   # Эйфория — высокий риск

    # ── Puell Multiple: давление майнеров ────────────────────
    if puell is not None:
        if   puell < 0.50: s += 2   # Майнеры в стрессе — дно близко
        elif puell < 0.80: s += 1   # Недооценка
        elif puell < 1.50: s += 0   # Справедливая цена
        elif puell < 2.50: s -= 1   # Переоценка
        else:              s -= 2   # Зона продажи

    # ── Map score to action (пороги скорректированы под новый диапазон) ──
    if s >= 6:
        action = "ПОКУПАТЬ"
        conf   = "Высокая"
        target = round(price * 1.07, 0)
        stop   = round(price * 0.95, 0)
    elif s >= 4:
        action = "НАКАПЛИВАТЬ"
        conf   = "Умеренная"
        target = round(price * 1.04, 0)
        stop   = round(price * 0.97, 0)
    elif s <= -6:
        action = "ПРОДАВАТЬ"
        conf   = "Высокая"
        target = round(price * 0.93, 0)
        stop   = round(price * 1.05, 0)
    elif s <= -4:
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

# ── OHLC cache (TTL = 4 hours) ────────────────────────────────
_ohlc_cache: dict = {}   # coin_id -> list of candles
_ohlc_ts:    dict = {}   # coin_id -> datetime of last fetch
OHLC_TTL = 4 * 3600      # seconds

async def fetch_ohlc_cached(coin_id: str) -> list:
    """Return cached OHLC if fresh enough, otherwise fetch and update cache."""
    now = datetime.now(MOSCOW_TZ).timestamp()
    if coin_id in _ohlc_cache and now - _ohlc_ts.get(coin_id, 0) < OHLC_TTL:
        log.info(f"OHLC cache hit for {coin_id}")
        return _ohlc_cache[coin_id]
    try:
        data = await fetch_ohlc(coin_id)
        _ohlc_cache[coin_id] = data
        _ohlc_ts[coin_id] = now
        log.info(f"OHLC fetched for {coin_id}: {len(data)} candles")
        return data
    except Exception as e:
        log.warning(f"OHLC fetch failed for {coin_id}: {e}")
        return _ohlc_cache.get(coin_id, [])   # return stale cache on error


async def collect_data():
    now_msk = datetime.now(MOSCOW_TZ)
    # Fetch prices, FG, DOM, NUPL, Puell all in parallel
    prices, fg, dom, nupl, puell = await asyncio.gather(
        fetch_prices(), fetch_fear_greed(), fetch_dominance(), fetch_nupl(), fetch_puell()
    )
    log.info(f"Prices OK: {list(prices.keys()) if isinstance(prices, dict) else 'ERROR'}")
    log.info(f"FG: {fg}")
    log.info(f"DOM: {dom}")
    log.info(f"NUPL: {nupl}")
    log.info(f"Puell: {puell}")

    # Fetch OHLC for coins that need a refresh (sequentially to avoid 429)
    coin_ids = list(COINS.keys())
    for i, coin_id in enumerate(coin_ids):
        now_ts = datetime.now(MOSCOW_TZ).timestamp()
        age = now_ts - _ohlc_ts.get(coin_id, 0)
        if age >= OHLC_TTL:
            if i > 0:
                await asyncio.sleep(5)  # avoid CoinGecko rate limit between fetches
            await fetch_ohlc_cached(coin_id)

    # Fetch all funding rates in parallel (Bybit handles concurrent requests fine)
    funding_results = await asyncio.gather(
        *[fetch_funding(meta["bybit"]) for meta in COINS.values()]
    )
    funding_map = {coin_id: fr for coin_id, fr in zip(COINS.keys(), funding_results)}

    next_hour = (now_msk.hour + 1) % 24
    result = {
        "time":      now_msk.strftime("%d.%m.%Y %H:%M"),
        "next_hour": f"{next_hour:02d}:00",
        "fg":        fg,
        "dom":       dom,
        "nupl":      nupl,
        "puell":     puell,
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
            ohlc   = _ohlc_cache.get(coin_id, [])
            closes = [c[4] for c in ohlc] if ohlc else []
            r6     = calc_rsi(closes, 6)
            r12    = calc_rsi(closes, 12)
            r24    = calc_rsi(closes, 24)
            _, _, mh, macd_hist = calc_macd(closes)
            bl, bm, bh = bollinger(closes)
            fr     = funding_map.get(coin_id, {"ok": False})
            if bl and bh and price and (bh - bl) > 0:
                pct = int((price - bl) / (bh - bl) * 100)
                if price <= bl:
                    bp     = "У нижней полосы"
                    bb_pct = 0
                elif price >= bh:
                    bp     = "У верхней полосы"
                    bb_pct = 100
                else:
                    bp     = f"Середина {pct}%"
                    bb_pct = pct
            else:
                bp     = "н/д"
                bb_pct = 50
            sig = generate_signal(
                change, r6, r12, r24, mh, macd_hist, bb_pct, price,
                fr=fr.get("rate", 0), fr_ok=fr.get("ok", False),
                nupl=nupl.get("value")  if nupl.get("ok")  else None,
                puell=puell.get("value") if puell.get("ok") else None,
            )
            coin_data = {
                "symbol":         meta["symbol"],
                "price":          price,
                "change":         change,
                "vol":            vol,
                "mcap":           mcap,
                "closes":         closes,
                "rsi6":           r6,
                "rsi12":          r12,
                "rsi24":          r24,
                "macd":           mh,
                "macd_hist":      macd_hist,
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
    L     = []
    fg    = data["fg"]
    dom   = data["dom"]
    nupl  = data.get("nupl",  {})
    puell = data.get("puell", {})
    L.append("*TY SMITH SIGNAL REPORT v3*")
    L.append(f"Время: {data['time']} МСК")
    L.append("")
    if fg.get("ok"):
        v = fg["value"]
        L.append(f"Fear & Greed: *{v}/100* — {fg['label']} (delta {fg['delta']})")
    if dom.get("ok"):
        L.append(f"BTC Dom: *{dom['dom']}%* — {dom['sig']}")
    if nupl.get("ok"):
        L.append(f"NUPL: *{nupl['value']:.3f}* — {nupl['zone']}")
    if puell.get("ok"):
        L.append(f"Puell Multiple: *{puell['value']:.2f}x* — {puell['zone']}")
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

        log.info("Генерируем PDF-коллаж...")
        pdf_bytes = generate_collage(data)
        log.info(f"PDF сгенерирован: {len(pdf_bytes):,} байт")

        bot_client = Bot(token=BOT_TOKEN)
        now_str  = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y_%H-%M")
        filename = f"TySmith_{now_str}.pdf"

        await bot_client.send_document(
            chat_id=CHAT_ID,
            document=io.BytesIO(pdf_bytes),
            filename=filename,
            caption=f"*TY SMITH SIGNAL REPORT*  |  {data['time']} МСК",
            parse_mode=ParseMode.MARKDOWN,
        )
        log.info("PDF-коллаж отправлен")

    except Exception as e:
        log.error(f"Ошибка send_signals: {e}", exc_info=True)

PID_FILE = "/tmp/tysmith-bot.pid"

def _acquire_pid_lock():
    """Exit if another instance is already running."""
    if os.path.exists(PID_FILE):
        try:
            old_pid = int(open(PID_FILE).read().strip())
            os.kill(old_pid, 0)   # signal 0 = check if process exists
            log.error(f"Бот уже запущен (PID {old_pid}). Выход.")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass   # stale PID file — overwrite it
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

def _release_pid_lock():
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass

async def main():
    _acquire_pid_lock()
    log.info("Ty Smith Bot v3 запущен.")
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_signals, "cron", hour="*", minute=0, timezone=MOSCOW_TZ)
    scheduler.start()
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен.")
    finally:
        _release_pid_lock()

if __name__ == "__main__":
    asyncio.run(main())
