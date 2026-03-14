import os, asyncio, logging, io
from datetime import datetime
import pytz
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dashboard import generate_dashboard

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
    return await get(f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true")

async def fetch_ohlc(coin_id):
    return await get(f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days=7")

async def fetch_fear_greed():
    try:
        data = await get("https://api.alternative.me/fng/?limit=2")
        t,y = data["data"][0], data["data"][1]
        val = int(t["value"]); delta = val - int(y["value"])
        emoji = "🤯" if val<=20 else "😨" if val<=40 else "😐" if val<=60 else "😊" if val<=80 else "😈"
        sign = "+" if delta>=0 else ""
        return {"value":val,"label":t["value_classification"],"emoji":emoji,"delta":f"{sign}{delta}","ok":True}
    except: return {"ok":False}

async def fetch_dominance():
    try:
        data = await get("https://api.coingecko.com/api/v3/global")
        dom = data["data"]["market_cap_percentage"]["btc"]
        mcap = data["data"]["total_market_cap"]["usd"]
        vol  = data["data"]["total_volume"]["usd"]
        sig = "🔴 Альты под давлением" if dom>58 else "🟡 Выбирай осторожно" if dom>52 else "🟢 Альты могут расти" if dom>46 else "🚀 Альт-сезон!"
        return {"dom":round(dom,1),"sig":sig,"mcap":round(mcap/1e9,0),"vol":round(vol/1e9,0),"ok":True}
    except: return {"ok":False}

async def fetch_funding_bybit(symbol):
    try:
        data = await get(f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1")
        rate = float(data["result"]["list"][0]["fundingRate"])*100
        interp = "🔴 Перегрев лонгов" if rate>0.15 else "🟡 Лонги доминируют" if rate>0.05 else "🟢 Нейтрально" if rate>-0.02 else "🟡 Шорты доминируют" if rate>-0.08 else "🔴 Перегрев шортов"
        return {"rate":round(rate,4),"interp":interp,"ok":True,"source":"Bybit"}
    except: return {"ok":False}

async def fetch_funding_okx(symbol):
    try:
        okx_sym = symbol.replace("USDT","-USDT-SWAP")
        data = await get(f"https://www.okx.com/api/v5/public/funding-rate?instId={okx_sym}")
        rate = float(data["data"][0]["fundingRate"])*100
        interp = "🔴 Перегрев лонгов" if rate>0.15 else "🟡 Лонги доминируют" if rate>0.05 else "🟢 Нейтрально" if rate>-0.02 else "🟡 Шорты доминируют" if rate>-0.08 else "🔴 Перегрев шортов"
        return {"rate":round(rate,4),"interp":interp,"ok":True,"source":"OKX"}
    except: return {"ok":False}

async def fetch_funding(sym):
    r = await fetch_funding_bybit(sym)
    if r.get("ok"): return r
    r = await fetch_funding_okx(sym)
    if r.get("ok"): return r
    return {"ok":False}

def calc_rsi(closes, p=14):
    if len(closes)<p+1: return 50.0
    g,l=[],[]
    for i in range(1,len(closes)):
        d=closes[i]-closes[i-1]
        (g if d>0 else l).append(abs(d))
    ag=sum(g[-p:])/p; al=sum(l[-p:])/p
    return round(100-(100/(1+ag/al)),1) if al else 100.0

def calc_macd(closes):
    def ema(d,n):
        k=2/(n+1); e=[d[0]]
        for p in d[1:]: e.append(p*k+e[-1]*(1-k))
        return e
    if len(closes)<26: return 0,0,0
    m=[a-b for a,b in zip(ema(closes,12),ema(closes,26))]
    s=ema(m,9)
    return round(m[-1],2),round(s[-1],2),round(m[-1]-s[-1],2)

def bollinger(closes,p=20):
    if len(closes)<p: return None,None,None
    w=closes[-p:]; mid=sum(w)/p
    std=(sum((x-mid)**2 for x in w)/p)**0.5
    return round(mid-2*std,0),round(mid,0),round(mid+2*std,0)

def rsi_label(v):
    return "🔴 перекуплен" if v>70 else "🟢 перепродан" if v<30 else "🟡 зона внимания" if v<40 or v>60 else "⚪️ норма"

def generate_signal(change,r12,mh,price,fr=0,fr_ok=False):
    s=0
    s+=3 if r12<30 else 2 if r12<40 else 1 if r12<50 else -3 if r12>75 else -2 if r12>65 else -1 if r12>55 else 0
    s+=1 if mh>0 else -1
    s+=2 if change>5 else 1 if change>2 else -2 if change<-5 else -1 if change<-2 else 0
    if fr_ok: s+=-1 if fr>0.1 else 1 if fr<-0.05 else 0
    if s>=4:   a,c,t,st="🟢 ПОКУПАТЬ","Высокая",round(price*1.07,0),round(price*0.95,0)
    elif s>=2: a,c,t,st="🔵 НАКАПЛИВАТЬ","Умеренная",round(price*1.04,0),round(price*0.97,0)
    elif s<=-4:a,c,t,st="🔴 ПРОДАВАТЬ","Высокая",round(price*0.93,0),round(price*1.04,0)
    elif s<=-2:a,c,t,st="🟠 ОСТОРОЖНО","Умеренная",round(price*0.97,0),round(price*1.02,0)
else:      a,c,t,st="⚪️ НЕЙТРАЛЬНО","Низкая",round(price*1.02,0),round(price*0.98,0)
    return {"action":a,"conf":c,"target":t,"stop":st,"score":s}

async def collect_data():
    now_msk = datetime.now(MOSCOW_TZ)
    prices, fg, dom = await asyncio.gather(fetch_prices(), fetch_fear_greed(), fetch_dominance())
    next_hour = (now_msk.hour+1)%24
    result = {
        "time": now_msk.strftime("%d.%m.%Y %H:%M"),
        "next_hour": f"{next_hour:02d}:00",
        "fg": fg,
        "dom": dom,
        "coins": [],
    }
    for coin_id, meta in COINS.items():
        try:
            d=prices.get(coin_id,{})
            price=d.get("usd",0); change=d.get("usd_24h_change",0)
            vol=d.get("usd_24h_vol",0); mcap=d.get("usd_market_cap",0)
            ohlc=await fetch_ohlc(coin_id)
            closes=[c[4] for c in ohlc] if ohlc else []
            r6=calc_rsi(closes,6); r12=calc_rsi(closes,12); r24=calc_rsi(closes,24)
            _,_,mh=calc_macd(closes)
            bl,bm,bh=bollinger(closes)
            fr=await fetch_funding(meta["bybit"])
            sig=generate_signal(change,r12,mh,price,fr=fr.get("rate",0),fr_ok=fr.get("ok",False))
            if bl and bh:
                bp="🟢 У нижней полосы" if price<=bl else "🔴 У верхней полосы" if price>=bh else f"⚪️ Середина ({int((price-bl)/(bh-bl)*100)}%)"
            else: bp="н/д"
            result["coins"].append({
                "symbol": meta["symbol"],
                "price": price, "change": change,
                "vol": vol, "mcap": mcap,
                "rsi6": r6, "rsi12": r12, "rsi24": r24,
                "macd": mh, "bb_pos": bp,
                "funding_rate": fr.get("rate") if fr.get("ok") else None,
                "funding_src": fr.get("source",""),
                "funding_interp": fr.get("interp",""),
                "action": sig["action"], "conf": sig["conf"],
                "target": sig["target"], "stop": sig["stop"],
                "score": sig["score"],
            })
        except Exception as e:
            log.error(f"Ошибка {meta['symbol']}: {e}")
    return result

def build_text_report(data):
    L = ["📊 *TY SMITH SIGNAL REPORT v3*", f"🕐 {data['time']} МСК", "", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "*🌍 РЫНОЧНЫЙ КОНТЕКСТ*"]
    fg=data["fg"]; dom=data["dom"]
    if fg.get("ok"):
        v=fg["value"]
        L.append(f"  Fear & Greed: {fg['emoji']} *{v}/100* — {fg['label']} (Δ {fg['delta']})")
        L.append("  _→ Экстремальный страх — покупай_" if v<=25 else "  _→ Страх — ищи точки входа_" if v<=45 else "  _→ Нейтрально_" if v<=55 else "  _→ Жадность — осторожно_" if v<=75 else "  _→ Экстремальная жадность — риск_")
    if dom.get("ok"):
        L.append(f"  BTC Dom: *{dom['dom']}%* — {dom['sig']}")
        L.append(f"  Капитализация: `${dom['mcap']:,.0f}B`  Объём: `${dom['vol']:,.0f}B`")
    L += ["", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for c in data["coins"]:
        sign="+" if c["change"]>=0 else ""
        L+=[f"*{c['symbol']}*  `${c['price']:,.0f}`  `{sign}{c['change']:.2f}%`","",
            "  📐 *Технический анализ*",
            f"  RSI-6:  `{c['rsi6']}` — {rsi_label(c['rsi6'])} _[скальпинг]_",
            f"  RSI-12: `{c['rsi12']}` — {rsi_label(c['rsi12'])} _[интрадей]_",
            f"  RSI-24: `{c['rsi24']}` — {rsi_label(c['rsi24'])} _[свинг]_",
            f"  MACD: `{c['macd']}` — {'↗️ бычий' if c['macd']>0 else '↘️ медвежий'}",
            f"  Bollinger: {c['bb_pos']}","","  💹 *Деривативы*"]
        if c["funding_rate"] is not None:
            L+=[f"  Funding ({c['funding_src']}): `{c['funding_rate']:+.4f}%` — {c['funding_interp']}", "  _норма: 0.01% каждые 8ч_"]
        else:
            L+=["  Funding Rate: недоступен"]
        L+=["",f"  🎯 *{c['action']}* ({c['conf']})",
            f"  Score: `{c['score']:+d}`  Цель: `${c['target']:,.0f}`  Стоп: `${c['stop']:,.0f}`",
            f"  Объём: `${c['vol']/1e6:.0f}M`  Кап: `${c['mcap']/1e9:.1f}B`","","━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    next_hour=(datetime.now(MOSCOW_TZ).hour+1)%24
    L+=["*📋 ЛЕГЕНДА RSI*",
        "  RSI-6 <30 краткосрочно | RSI-12 интрадей | RSI-24 свинг",
        "  Все три <35 = сильный сигнал входа",
        "", f"  ⏰ Следующий отчёт в *{next_hour:02d}:00 МСК*",
        "","_Не является финансовой рекомендацией. DYOR._"]
    return "\n".join(L)

async def send_signals():
    log.info("Генерируем отчёт...")
    try:
        data = await collect_data()
        img_bytes = generate_dashboard(data)
        text = build_text_report(data)
        bot = Bot(token=BOT_TOKEN)
        await bot.send_photo(
            chat_id=CHAT_ID,
            photo=io.BytesIO(img_bytes),
            caption="📊 Ty Smith Dashboard"
        )
        if len(text)>4000: text=text[:3990]+"\n_...обрезано_"
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN)
        log.info("Отчёт отправлен.")
    except Exception as e:
        log.error(f"Ошибка: {e}")

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
