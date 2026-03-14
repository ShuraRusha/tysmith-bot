from PIL import Image, ImageDraw, ImageFont
import io, os

W, H = 1080, 810

BG_TOP  = (10, 10, 18)
BG_BOT  = (20, 20, 35)
CARD    = (22, 22, 36)
CARD2   = (15, 15, 26)
LINE    = (40, 40, 60)
WHITE   = (255, 255, 255)
LGRAY   = (130, 130, 155)
DGRAY   = (60, 60, 80)
GREEN   = (29, 200, 120)
RED     = (235, 70, 70)
AMBER   = (245, 165, 30)
BLUE    = (80, 150, 255)

BASE = os.path.dirname(os.path.abspath(__file__))

def font(size, bold=False):
    name = "Nunito-Bold.ttf" if bold else "Nunito-Regular.ttf"
    local = os.path.join(BASE, name)
    if os.path.exists(local):
        return ImageFont.truetype(local, size)
    fallbacks = [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for p in fallbacks:
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()

def tw(draw, text, f):
    bb = draw.textbbox((0,0), text, font=f)
    return bb[2]-bb[0]

def rnd(draw, x1,y1,x2,y2, fill, r=14):
    draw.rectangle([x1+r,y1,x2-r,y2], fill=fill)
    draw.rectangle([x1,y1+r,x2,y2-r], fill=fill)
    for cx,cy in [(x1,y1),(x2-2*r,y1),(x1,y2-2*r),(x2-2*r,y2-2*r)]:
        draw.ellipse([cx,cy,cx+2*r,cy+2*r], fill=fill)

def bar(draw, x,y,w,h, pct, color, bg=None):
    bg = bg or DGRAY
    rnd(draw, x,y,x+w,y+h, bg, h//2)
    if pct > 0.01:
        rnd(draw, x,y,x+max(int(w*min(pct,1.0)),h),y+h, color, h//2)

def sig_col(action):
    if action == "ПОКУПАТЬ":    return GREEN
    if action == "НАКАПЛИВАТЬ": return BLUE
    if action == "ПРОДАВАТЬ":   return RED
    if action == "ОСТОРОЖНО":   return AMBER
    return LGRAY

def rsi_col(v):
    if v > 70: return RED
    if v < 30: return GREEN
    if v < 40 or v > 60: return AMBER
    return LGRAY

def generate_coin_card(coin, gdata):
    img  = Image.new("RGB", (W,H), BG_TOP)
    drw  = ImageDraw.Draw(img)

    for y in range(H):
        t = y/H
        r = int(BG_TOP[0]+(BG_BOT[0]-BG_TOP[0])*t)
        g = int(BG_TOP[1]+(BG_BOT[1]-BG_TOP[1])*t)
        b = int(BG_TOP[2]+(BG_BOT[2]-BG_TOP[2])*t)
        drw.line([(0,y),(W,y)], fill=(r,g,b))

    sc = sig_col(coin["action"])
    P  = 42

    f11 = font(22)
    f13 = font(26)
    f15 = font(30)
    f18 = font(36)
    f22 = font(44, bold=True)
    f26 = font(52, bold=True)
    f40 = font(80, bold=True)
    fb  = font(28, bold=True)
    fb2 = font(32, bold=True)

    # accent line top
    drw.rectangle([0,0,W,5], fill=sc)

    # HEADER
    rnd(drw, P, P+4, P+100, P+46, CARD2, 8)
    drw.text((P+14, P+12), coin["symbol"], font=fb, fill=sc)
    drw.text((P+114, P+14), "TY SMITH SIGNALS", font=f11, fill=DGRAY)

    # signal badge top right
    bw = 220
    rnd(drw, W-P-bw, P, W-P, P+90, CARD, 14)
    drw.rectangle([W-P-bw, P, W-P-bw+5, P+90], fill=sc)
    drw.text((W-P-bw+18, P+10), "СИГНАЛ", font=f11, fill=LGRAY)
    drw.text((W-P-bw+18, P+34), coin["action"], font=fb2, fill=sc)
    drw.text((W-P-bw+18, P+70), f"Score {coin['score']:+d}   {coin['conf']}", font=f11, fill=LGRAY)

    # PRICE
    drw.text((P, P+56), f"${coin['price']:,.0f}", font=f40, fill=WHITE)
    chg = coin["change"]
    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
    chg_col = GREEN if chg >= 0 else RED
    pw = tw(drw, f"${coin['price']:,.0f}", f40)
    rnd(drw, P+pw+18, P+92, P+pw+18+tw(drw,chg_str,f15)+20, P+126, CARD2, 8)
    drw.text((P+pw+28, P+94), chg_str, font=f15, fill=chg_col)
    drw.text((P, P+148), f"Vol 24h: ${coin['vol']/1e9:.2f}B     Cap: ${coin['mcap']/1e9:.1f}B", font=f13, fill=LGRAY)

    drw.line([(P,P+178),(W-P,P+178)], fill=LINE, width=1)

    # TARGET / STOP
    mid = W//2 - 10
    rnd(drw, P, P+190, mid-8, P+278, CARD, 12)
    drw.text((P+18, P+198), "ЦЕЛЬ", font=f11, fill=LGRAY)
    drw.text((P+18, P+218), f"${coin['target']:,.0f}", font=f22, fill=GREEN)
    pct_t = (coin["target"]/coin["price"]-1)*100
    drw.text((P+18, P+264), f"+{pct_t:.1f}% от текущей цены", font=f11, fill=DGRAY)

    rnd(drw, mid+8, P+190, W-P, P+278, CARD, 12)
    drw.text((mid+26, P+198), "СТОП-ЛОСС", font=f11, fill=LGRAY)
    drw.text((mid+26, P+218), f"${coin['stop']:,.0f}", font=f22, fill=RED)
    pct_s = (coin["stop"]/coin["price"]-1)*100
    drw.text((mid+26, P+264), f"{pct_s:.1f}% от текущей цены", font=f11, fill=DGRAY)

    # RSI
    y1 = P+300
    drw.text((P, y1), "RSI АНАЛИЗ", font=f11, fill=LGRAY)
    bw2 = (W-P*2-32)//3

    for idx,(lbl,val,tf) in enumerate([("RSI-6",coin["rsi6"],"скальпинг"),("RSI-12",coin["rsi12"],"интрадей"),("RSI-24",coin["rsi24"],"свинг")]):
        bx = P + idx*(bw2+16)
        rc = rsi_col(val)
        rnd(drw, bx, y1+26, bx+bw2, y1+116, CARD, 12)
        drw.text((bx+16, y1+34), lbl, font=fb, fill=WHITE)
        drw.text((bx+bw2-16, y1+36), tf, font=f11, fill=LGRAY)
        drw.text((bx+16, y1+58), str(val), font=f26, fill=rc)
        bar(drw, bx+16, y1+108, bw2-32, 8, val/100, rc)

    r6,r12,r24 = coin["rsi6"],coin["rsi12"],coin["rsi24"]
    cy2 = y1+130
    if r6<35 and r12<35 and r24<35:
        msg,mc = "Все RSI ниже 35 — очень сильный сигнал входа", GREEN
    elif r6>65 and r12>65 and r24>65:
        msg,mc = "Все RSI выше 65 — рынок перегрет, осторожно", RED
    elif abs(r6-r24)>20:
        msg,mc = f"RSI расходятся ({r6} vs {r24}) — рынок в переходе", AMBER
    else:
        msg,mc = "RSI согласованы — тренд стабилен", LGRAY
    drw.rectangle([P, cy2+4, P+5, cy2+32], fill=mc)
    drw.text((P+14, cy2+4), msg, font=f13, fill=mc)

    drw.line([(P,cy2+44),(W-P,cy2+44)], fill=LINE, width=1)

    # INDICATORS
    y2 = cy2+56
    iw = (W-P*2-32)//3

    macd_v = coin.get("macd",0)
    macd_s = f"+{macd_v}" if macd_v>0 else str(macd_v)
    macd_c = GREEN if macd_v>0 else RED

    fr  = coin.get("funding_rate")
    frs = coin.get("funding_src","")
    fri = coin.get("funding_interp","нет данных")
    if fr is not None:
        fr_s = f"{fr:+.4f}%"
        fr_c = RED if fr>0.1 else AMBER if fr>0.05 else GREEN
    else:
        fr_s,fr_c = "N/A", DGRAY
        fri = "нет данных"

    bp  = coin.get("bb_pos","н/д")
    bp_c = GREEN if "нижней" in bp else RED if "верхней" in bp else AMBER

    for idx,(lbl,val,sub,col) in enumerate([
        ("MACD", macd_s, "бычий" if macd_v>0 else "медвежий", macd_c),
        (f"FUNDING {frs}", fr_s, fri[:18], fr_c),
        ("BOLLINGER", bp[:12], "позиция", bp_c),
    ]):
        ix = P + idx*(iw+16)
        rnd(drw, ix, y2, ix+iw, y2+86, CARD, 12)
        drw.text((ix+16, y2+10), lbl, font=f11, fill=LGRAY)
        drw.text((ix+16, y2+32), val, font=fb2, fill=col)
        drw.text((ix+16, y2+68), sub, font=f11, fill=DGRAY)

    drw.line([(P,y2+100),(W-P,y2+100)], fill=LINE, width=1)

    # GLOBAL ROW
    y3 = y2+112
    fg  = gdata.get("fg",{})
    dom = gdata.get("dom",{})

    gw = (W-P*2-48)//4
    gvals = []
    if fg.get("ok"):
        fv = fg["value"]
        fc = RED if fv<=25 else AMBER if fv<=45 else LGRAY if fv<=55 else GREEN
        gvals.append(("FEAR & GREED", f"{fv}/100", fg["label"], fc))
    else:
        gvals.append(("FEAR & GREED","N/A","",DGRAY))
    if dom.get("ok"):
        gvals.append(("BTC DOM", f"{dom['dom']}%", dom["sig"][:14], BLUE))
        gvals.append(("КАП. РЫНКА", f"${dom['mcap']:,.0f}B", "капитализация", LGRAY))
        gvals.append(("ОБЪЁМ 24H", f"${dom['vol']:,.0f}B", "торговый", LGRAY))
    else:
        gvals += [("BTC DOM","N/A","",DGRAY),("КАП.","N/A","",DGRAY),("ОБЪ.","N/A","",DGRAY)]

    for idx,(lbl,val,sub,col) in enumerate(gvals):
        gx = P + idx*(gw+16)
        rnd(drw, gx, y3, gx+gw, y3+76, CARD, 10)
        drw.text((gx+14, y3+10), lbl, font=f11, fill=LGRAY)
        drw.text((gx+14, y3+32), val, font=fb, fill=col)
        drw.text((gx+14, y3+62), sub[:16], font=f11, fill=DGRAY)

    # FOOTER
    drw.line([(P,H-40),(W-P,H-40)], fill=LINE, width=1)
    drw.text((P,H-30), "TY SMITH SIGNALS  •  Не является финансовой рекомендацией", font=f11, fill=DGRAY)
    ts = f"{gdata.get('time','')} МСК  •  след. {gdata.get('next_hour','')}"
    drw.text((W-P-tw(drw,ts,f11), H-30), ts, font=f11, fill=DGRAY)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()

def generate_all_cards(data):
    return [generate_coin_card(c, data) for c in data.get("coins",[])]
