import os, asyncio, logging
from datetime import datetime
import pytz
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN = "8557968994:AAGzIC3Hd00UVAr-zliHcovtYAg_WOrSet0"
CHAT_ID   = "7675712715"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

COINS = {
    "bitcoin":   {"symbol": "BTC",  "bybit": "BTCUSDT"},
    "ethereum":  {"symbol": "ETH",  "bybit": "ETHUSDT"},
    "solana":    {"symbol": "SOL",  "bybit": "SOLUSDT"},
    "chainlink": {"symbol": "LINK", "bybit": "LINKUSDT"},
}

async def get(url, headers=None):
    async with aiohttp.ClientSession(headers=headers or HEADERS) as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json(content_type=None)

async def fetch_prices():
    ids = ",".join(COINS.keys())
    return await get(f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true")

async def fetch_ohlc(coin_id):
    return await get(f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days=7")

async def fetch_fear_greed():
    try:
        data = await get("https://api.alternative.me/fng/?limit=2", headers={"Accept": "application/json"})
        t, y = data["data"][0], data["data"][1]
        val = int(t["value"])
        delta = val - int(y["value"])
        emoji = "🤯" if val<=20 else "😨" if val<=40 else "😐" if val<=60 else "😊" if val<=80 else "😈"
        sign = "+" if delta >= 0 else ""
        return {"value": val, "label": t["value_classification"], "emoji": emoji, "delta": f"{sign}{delta}", "ok": True}
    except:
        return {"ok": False}

async def fetch_dominance():
    try:
        data = await get("https://api.coingecko.com/api/v3/global")
        dom = data["data"]["market_cap_percentage"]["btc"]
        mcap = data["data"]["total_market_cap"]["usd"]
        vol  = data["data"]["total_volume"]["usd"]
        sig = "🔴 Альты под давлением" if dom>58 else "🟡 Выбирай осторожно" if dom>52 else "🟢 Альты могут расти" if dom>46 else "🚀 Альт-сезон!"
        return {"dom": round(dom,1), "sig": sig, "mcap": round(mcap/1e9,0), "vol": round(vol/1e9,0), "ok": True}
    except:
        return {"ok": False}

async def fetch_funding_bybit(symbol):
    try:
        url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1"
        data = await get(url)
        rate = float(data["result"]["list"][0]["fundingRate"]) * 100
        interp = "🔴 Перегрев лонгов — риск слива" if rate>0.15 else "🟡 Лонги доминируют" if rate>0.05 else "🟢 Нейтрально" if rate>-0.02 else "🟡 Шорты доминируют — возможен сквиз" if rate>-0.08 else "🔴 Перегрев шортов"
        return {"rate": round(rate,4), "interp": interp, "ok": True, "source": "Bybit"}
    except:
        return {"ok": False}

async def fetch_funding_okx(symbol):
    try:
        okx_sym = symbol.replace("USDT", "-USDT-SWAP")
        url = f"https://www.okx.com/api/v5/public/funding-rate?instId={okx_sym}"
        data = await get(url)
        rate = float(data["data"][0]["fundingRate"]) * 100
        interp = "🔴 Перегрев лонгов — риск слива" if rate>0.15 else "🟡 Лонги доминируют" if rate>0.05 else "🟢 Нейтрально" if rate>-0.02 else "🟡 Шорты доминируют — возможен сквиз" if rate>-0.08 else "🔴 Перегрев шортов"
        return {"rate": round(rate,4), "interp": interp, "ok": True, "source": "OKX"}
    except:
        return {"ok": False}

async def fetch_funding(bybit_sym):
    result = await fetch_funding_bybit(bybit_sym)
    if result.get("ok"):
        return result
    result = await fetch_funding_okx(bybit_sym)
    if result.get("ok"):
        return result
    return {"ok": False}

def calc_rsi(closes, p=14):
    if len(closes)<p+1: return 50.0
    g,l=[],[]
    for i in range(1,len(closes)):
        d=closes[i]-closes[i-1]
        (g if d>0 else l).append(abs(d))
    ag=sum(g[-p:])/p; al=sum(l[-p:])/p
    return round(100-(100/(1+ag/al)),1) if al else 100.0

def multi_rsi(closes):
    return {"rsi6": calc_rsi(closes,6), "rsi12": calc_rsi(closes,12), "rsi24": calc_rsi(closes,24)}

def rsi_label(v):
    return "🔴 перекуплен" if v>70 else "🟢 перепродан" if v<30 else "🟡 зона внимания" if v<40 or v>60 else "⚪️ норма"

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

def generate_signal(change,r12,mh,price,fr=0,fr_ok=False):
    s=0
    s+=3 if r12<30 else 2 if r12<40 else 1 if r12<50 else -3 if r12>75 else -2 if r12>65 else -1 if r12>55 else 0
    s+=1 if mh>0 else -1
    s+=2 if change>5 else 1 if change>2 else -2 if change<-5 else -1 if change<-2 else 0
    if fr_ok:
        s+=-1 if fr>0.1 else 1 if fr<-0.05 else 0
    if s>=4:   a,c,t,st="🟢 ПОКУПАТЬ","Высокая",round(price*1.07,0),round(price*0.95,0)
    elif s>=2: a,c,t,st="🔵 НАКАПЛИВАТЬ","Умеренная",round(price*1.04,0),round(price*0.97,0)
    elif s<=-4:a,c,t,st="🔴 ПРОДАВАТЬ","Высокая",round(price*0.93,0),round(price*1.04,0)
    elif s<=-2:a,c,t,st="🟠 ОСТОРОЖНО","Умеренная",round(price*0.97,0),round(price*1.02,0)
    else:      a,c,t,st="⚪️ НЕЙТРАЛЬНО","Низкая",round(price*1.02,0),round(price*0.98,0)
    return {"action":a,"conf":c,"target":t,"stop":st,"score":s}

async def build_report():
    now = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
    prices, fg, dom = await asyncio.gather(fetch_prices(), fetch_fear_greed(), fetch_dominance())
    L = ["📊 *TY SMITH SIGNAL REPORT v3*", f"🕐 {now} МСК", "", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "*🌍 РЫНОЧНЫЙ КОНТЕКСТ*"]
    if fg.get("ok"):
        v=fg["value"]
        L.append(f"  Fear & Greed: {fg['emoji']} *{v}/100* — {fg['label']} (Δ {fg['delta']} за сутки)")
        L.append("  _→ Экстремальный страх — покупай_" if v<=25 else "  _→ Страх — ищи точки входа_" if v<=45 else "  _→ Нейтрально — жди сигнала_" if v<=55 else "  _→ Жадность — будь осторожен_" if v<=75 else "  _→ Экстремальная жадность — риск коррекции_")
    if dom.get("ok"):
        L.append(f"  BTC Dominance: *{dom['dom']}%* — {dom['sig']}")
        L.append(f"  Капитализация: `${dom['mcap']:,.0f}B`  |  Объём 24h: `${dom['vol']:,.0f}B`")
    L += ["", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for coin_id, meta in COINS.items():
        try:
            d=prices.get(coin_id,{})
            price=d.get("usd",0); change=d.get("usd_24h_change",0)
            vol=d.get("usd_24h_vol",0); mcap=d.get("usd_market_cap",0)
            sign="+" if change>=0 else ""
            ohlc=await fetch_ohlc(coin_id)
            closes=[c[4] for c in ohlc] if ohlc else []
            rsi_vals=multi_rsi(closes)
            _,_,mh=calc_macd(closes)
            bl,bm,bh=bollinger(closes)
            fr=await fetch_funding(meta["bybit"])
            sig=generate_signal(change,rsi_vals["rsi12"],mh,price,fr=fr.get("rate",0),fr_ok=fr.get("ok",False))
            if bl and bh:
                bp="🟢 У нижней полосы (поддержка)" if price<=bl else "🔴 У верхней полосы (сопротивление)" if price>=bh else f"⚪️ В середине ({int((price-bl)/(bh-bl)*100)}%)"
            else: bp="н/д"
            r6,r12,r24=rsi_vals["rsi6"],rsi_vals["rsi12"],rsi_vals["rsi24"]
            macd_tag="↗️ бычий" if mh>0 else "↘️ медвежий"
            rsi_comment="  _→ Все RSI <35 — сильный сигнал входа!_" if r6<35 and r12<35 and r24<35 else "  _→ RSI расходятся — рынок в переходе_" if abs(r6-r24)>20 else "  _→ RSI согласованы_"
            L+=[f"*{meta['symbol']}*  `${price:,.0f}`  `{sign}{change:.2f}%`","",
                "  📐 *Технический анализ*",
                f"  RSI-6:  `{r6}` — {rsi_label(r6)} _[скальпинг]_",
                f"  RSI-12: `{r12}` — {rsi_label(r12)} _[интрадей]_",
                f"  RSI-24: `{r24}` — {rsi_label(r24)} _[свинг]_",
                rsi_comment,
                f"  MACD: `{mh}` — {macd_tag}"]
            if bl: L+=[f"  Bollinger: `${bl:,.0f}` / `${bm:,.0f}` / `${bh:,.0f}`", f"  Позиция: {bp}"]
            L+=["", "  💹 *Деривативы*"]
            if fr.get("ok"):
                L+=[f"  Funding ({fr['source']}): `{fr['rate']:+.4f}%` — {fr['interp']}", "  _норма: 0.01% каждые 8ч_"]
            else:
                L+=["  Funding Rate: недоступен"]
            L+=["", f"  🎯 *СИГНАЛ: {sig['action']}* ({sig['conf']})",
                f"  Score: `{sig['score']:+d}`  Цель: `${sig['target']:,.0f}`  Стоп: `${sig['stop']:,.0f}`",
                f"  Объём: `${vol/1e6:.0f}M`  Кап: `${mcap/1e9:.1f}B`","","━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
        except Exception as e:
            L.append(f"Ошибка {meta['symbol']}: {e}")
    next_hour = (datetime.now(MOSCOW_TZ).hour + 1) % 24
    L+=["*📋 ЛЕГЕНДА RSI*",
        "  RSI-6  <30 = перепродан краткосрочно",
        "  RSI-12 <30 = перепродан интрадей",
        "  RSI-24 <30 = перепродан по тренду",
        "  Все три <35 одновременно = сильный сигнал",
        "", f"  ⏰ Следующий отчёт в *{next_hour:02d}:00 МСК*",
        "", "_Не является финансовой рекомендацией. DYOR._"]
    return "\n".join(L)

async def send_signals():
    log.info("Генерируем отчёт...")
    try:
        text = await build_report()
        bot = Bot(token=BOT_TOKEN)
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
