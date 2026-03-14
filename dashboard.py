from PIL import Image, ImageDraw, ImageFont
import io, os

W, H = 960, 720

BG_TOP    = (10, 10, 18)
BG_BOT    = (18, 18, 30)
CARD_BG   = (20, 20, 32)
CARD2     = (14, 14, 22)
LINE_COL  = (42, 42, 58)
WHITE     = (255, 255, 255)
LGRAY     = (100, 100, 120)
DGRAY     = (50, 50, 65)
GREEN     = (29, 158, 117)
GREEN_DIM = (29, 158, 117, 30)
RED       = (226, 75, 74)
AMBER     = (245, 158, 11)
BLUE      = (59, 130, 246)

def load_font(size, bold=False):
    candidates = [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        f"/usr/share/fonts/truetype/liberation/LiberationSans{'-Bold' if bold else '-Regular'}.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except:
                pass
    return ImageFont.load_default()

def rsi_color(v):
    if v > 70: return RED
    if v < 30: return GREEN
    if v < 40 or v > 60: return AMBER
    return LGRAY

def signal_color(action):
    if action == "ПОКУПАТЬ":    return GREEN
    if action == "НАКАПЛИВАТЬ": return BLUE
    if action == "ПРОДАВАТЬ":   return RED
    if action == "ОСТОРОЖНО":   return AMBER
    return LGRAY

def draw_rect(draw, x1, y1, x2, y2, fill, radius=12):
    r = radius
    draw.rectangle([x1+r, y1, x2-r, y2], fill=fill)
    draw.rectangle([x1, y1+r, x2, y2-r], fill=fill)
    for cx, cy in [(x1,y1),(x2-2*r,y1),(x1,y2-2*r),(x2-2*r,y2-2*r)]:
        draw.ellipse([cx, cy, cx+2*r, cy+2*r], fill=fill)

def draw_bar_full(draw, x, y, w, h, pct, color, bg=DGRAY):
    draw_rect(draw, x, y, x+w, y+h, bg, h//2)
    if pct > 0.01:
        fw = max(int(w * min(pct, 1.0)), h)
        draw_rect(draw, x, y, x+fw, y+h, color, h//2)

def text_w(draw, text, font):
    bb = draw.textbbox((0,0), text, font=font)
    return bb[2] - bb[0]

def generate_coin_card(coin: dict, global_data: dict) -> bytes:
    img  = Image.new("RGB", (W, H), BG_TOP)
    draw = ImageDraw.Draw(img)

    for y in range(H):
        t = y / H
        r = int(BG_TOP[0] + (BG_BOT[0]-BG_TOP[0])*t)
        g = int(BG_TOP[1] + (BG_BOT[1]-BG_TOP[1])*t)
        b = int(BG_TOP[2] + (BG_BOT[2]-BG_TOP[2])*t)
        draw.line([(0,y),(W,y)], fill=(r,g,b))

    sig_col = signal_color(coin["action"])

    for i in range(40):
        alpha = int(15 * (1 - i/40))
        draw.ellipse([W-220+i, -80+i, W+80-i, 220-i], fill=(*sig_col, alpha) if len(sig_col)==3 else sig_col)

    fb = load_font(13, bold=True)
    f = load_font(13)
    f12 = load_font(12)
    f11 = load_font(11)
    f36b = load_font(36, bold=True)
    f26b = load_font(26, bold=True)
    f22b = load_font(22, bold=True)
    f18b = load_font(18, bold=True)
    f16b = load_font(16, bold=True)
    f15b = load_font(15, bold=True)

    PAD = 36

    # ── SYMBOL + NAME ────────────────────────────────────────────────────────
    draw_rect(draw, PAD, PAD, PAD+80, PAD+34, (*sig_col, 40) if False else CARD2, 8)
    draw.rectangle([PAD, PAD, PAD+80, PAD+34], fill=CARD2)
    draw.text((PAD+10, PAD+8), coin["symbol"], font=fb, fill=sig_col)

    draw.text((PAD+90, PAD+8), "by TY SMITH SIGNALS", font=f11, fill=DGRAY)

    price_str = f"${coin['price']:,.0f}"
    draw.text((PAD, PAD+50), price_str, font=f36b, fill=WHITE)

    chg = coin["change"]
    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
    chg_col = GREEN if chg >= 0 else RED
    pw = text_w(draw, price_str, f36b)
    draw.rectangle([PAD+pw+16, PAD+62, PAD+pw+16+len(chg_str)*9+16, PAD+84], fill=CARD2)
    draw.text((PAD+pw+24, PAD+64), chg_str, font=fb, fill=chg_col)

    draw.text((PAD, PAD+98), f"Vol 24h: ${coin['vol']/1e9:.2f}B   Cap: ${coin['mcap']/1e9:.1f}B", font=f12, fill=LGRAY)

    draw.line([(PAD, PAD+120), (W-PAD, PAD+120)], fill=LINE_COL, width=1)

    # ── SIGNAL BADGE ─────────────────────────────────────────────────────────
    bx = W - PAD - 180
    draw_rect(draw, bx, PAD+36, bx+180, PAD+110, CARD2, 12)
    draw.rectangle([bx, PAD+36, bx+4, PAD+110], fill=sig_col)
    draw.text((bx+16, PAD+42), "СИГНАЛ", font=f11, fill=LGRAY)
    draw.text((bx+16, PAD+60), coin["action"], font=f18b, fill=sig_col)
    draw.text((bx+16, PAD+86), f"Score: {coin['score']:+d}   {coin['conf']}", font=f11, fill=LGRAY)

    # ── TARGET / STOP ────────────────────────────────────────────────────────
    y0 = PAD + 136
    draw_rect(draw, PAD, y0, (W-PAD*2-16)//2+PAD, y0+70, CARD2, 10)
    draw.text((PAD+16, y0+10), "ЦЕЛЬ", font=f11, fill=LGRAY)
    draw.text((PAD+16, y0+28), f"${coin['target']:,.0f}", font=f22b, fill=GREEN)
    pct_t = (coin["target"]/coin["price"]-1)*100
    draw.text((PAD+16, y0+54), f"+{pct_t:.1f}% от текущей цены", font=f11, fill=DGRAY)

    rx = (W-PAD*2-16)//2+PAD+16
    draw_rect(draw, rx, y0, W-PAD, y0+70, CARD2, 10)
    draw.text((rx+16, y0+10), "СТОП-ЛОСС", font=f11, fill=LGRAY)
    draw.text((rx+16, y0+28), f"${coin['stop']:,.0f}", font=f22b, fill=RED)
    pct_s = (coin["stop"]/coin["price"]-1)*100
    draw.text((rx+16, y0+54), f"{pct_s:.1f}% от текущей цены", font=f11, fill=DGRAY)

    # ── RSI SECTION ──────────────────────────────────────────────────────────
    y1 = y0 + 86
    draw.text((PAD, y1), "RSI АНАЛИЗ", font=f11, fill=LGRAY)

    rsi_data = [
        ("RSI-6",  coin["rsi6"],  "скальпинг"),
        ("RSI-12", coin["rsi12"], "интрадей"),
        ("RSI-24", coin["rsi24"], "свинг"),
    ]
    bar_x  = PAD
    bar_y  = y1 + 22
    bar_w  = (W - PAD*2 - 32) // 3

    for idx, (label, val, timeframe) in enumerate(rsi_data):
        bx2 = bar_x + idx*(bar_w+16)
        draw_rect(draw, bx2, bar_y, bx2+bar_w, bar_y+72, CARD2, 10)
        rc = rsi_color(val)
        draw.text((bx2+14, bar_y+10), label, font=fb, fill=WHITE)
        draw.text((bx2+bar_w-14, bar_y+10), timeframe, font=f11, fill=LGRAY)
        draw.text((bx2+14, bar_y+30), str(val), font=f26b, fill=rc)
        draw_bar_full(draw, bx2+14, bar_y+58, bar_w-28, 6, val/100, rc)

    r6,r12,r24 = coin["rsi6"],coin["rsi12"],coin["rsi24"]
    comment_y = bar_y + 80
    if r6<35 and r12<35 and r24<35:
        comment = "Все RSI ниже 35 — очень сильный сигнал входа"
        cc = GREEN
    elif r6>65 and r12>65 and r24>65:
        comment = "Все RSI выше 65 — рынок перегрет, осторожно"
        cc = RED
    elif abs(r6-r24) > 20:
        comment = f"RSI расходятся ({r6} vs {r24}) — рынок в переходе"
        cc = AMBER
    else:
        comment = "RSI согласованы — тренд стабилен"
        cc = LGRAY
    draw.rectangle([PAD, comment_y, PAD+4, comment_y+24], fill=cc)
    draw.text((PAD+12, comment_y+4), comment, font=f12, fill=cc)

    # ── INDICATORS ROW ───────────────────────────────────────────────────────
    y2 = comment_y + 38
    draw.line([(PAD, y2), (W-PAD, y2)], fill=LINE_COL, width=1)
    y2 += 12

    indicators = []

    macd = coin.get("macd", 0)
    macd_str = f"+{macd}" if macd > 0 else str(macd)
    macd_col = GREEN if macd > 0 else RED
    indicators.append(("MACD", macd_str, "бычий" if macd>0 else "медвежий", macd_col))

    fr = coin.get("funding_rate")
    fr_src = coin.get("funding_src","")
    if fr is not None:
        fr_str = f"{fr:+.4f}%"
        if fr > 0.1: fr_col = RED
        elif fr > 0.05: fr_col = AMBER
        else: fr_col = GREEN
        indicators.append((f"FUNDING ({fr_src})", fr_str, coin.get("funding_interp","")[:14], fr_col))
    else:
        indicators.append(("FUNDING", "N/A", "нет данных", DGRAY))

    bb_pos = coin.get("bb_pos","н/д")
    if "нижней" in bb_pos: bb_col = GREEN
    elif "верхней" in bb_pos: bb_col = RED
    else: bb_col = AMBER
    indicators.append(("BOLLINGER", bb_pos[:12], "позиция", bb_col))

    ind_w = (W - PAD*2 - 32) // 3
    for idx, (lbl, val, sub, col) in enumerate(indicators):
        ix = PAD + idx*(ind_w+16)
        draw_rect(draw, ix, y2, ix+ind_w, y2+68, CARD2, 10)
        draw.text((ix+14, y2+10), lbl, font=f11, fill=LGRAY)
        draw.text((ix+14, y2+28), val, font=f15b, fill=col)
        draw.text((ix+14, y2+50), sub, font=f11, fill=DGRAY)

    # ── GLOBAL ROW ───────────────────────────────────────────────────────────
    y3 = y2 + 84
    draw.line([(PAD, y3), (W-PAD, y3)], fill=LINE_COL, width=1)
    y3 += 12

    fg  = global_data.get("fg", {})
    dom = global_data.get("dom", {})

    gw = (W - PAD*2 - 48) // 4
    globals_data = []

    if fg.get("ok"):
        fv = fg["value"]
        if fv <= 25: fc = RED
        elif fv <= 45: fc = AMBER
        elif fv <= 55: fc = LGRAY
        else: fc = GREEN
        globals_data.append(("FEAR & GREED", str(fv)+"/100", fg["label"], fc))
    else:
        globals_data.append(("FEAR & GREED", "N/A", "", DGRAY))

    if dom.get("ok"):
        globals_data.append(("BTC DOM", f"{dom['dom']}%", dom["sig"][:16], BLUE))
        globals_data.append(("РЫНОК", f"${dom['mcap']:,.0f}B", "капитализация", LGRAY))
        globals_data.append(("ОБЪЁМ 24H", f"${dom['vol']:,.0f}B", "торговый", LGRAY))
    else:
        globals_data += [("BTC DOM","N/A","",DGRAY),("РЫНОК","N/A","",DGRAY),("ОБЪЁМ","N/A","",DGRAY)]

    for idx, (lbl, val, sub, col) in enumerate(globals_data):
        gx = PAD + idx*(gw+16)
        draw_rect(draw, gx, y3, gx+gw, y3+68, CARD2, 10)
        draw.text((gx+14, y3+10), lbl, font=f11, fill=LGRAY)
        draw.text((gx+14, y3+28), val, font=f15b, fill=col)
        draw.text((gx+14, y3+50), sub[:16], font=f11, fill=DGRAY)

    # ── FOOTER ───────────────────────────────────────────────────────────────
    now_str = global_data.get("time","")
    next_h  = global_data.get("next_hour","")
    draw.line([(PAD, H-36), (W-PAD, H-36)], fill=LINE_COL, width=1)
    draw.text((PAD, H-26), "TY SMITH SIGNALS  •  Не является финансовой рекомендацией", font=f11, fill=DGRAY)
    tw = text_w(draw, f"{now_str} МСК  •  след. {next_h}", f11)
    draw.text((W-PAD-tw, H-26), f"{now_str} МСК  •  след. {next_h}", font=f11, fill=DGRAY)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()

def generate_all_cards(data: dict) -> list:
    cards = []
    for coin in data.get("coins", []):
        img_bytes = generate_coin_card(coin, data)
        cards.append(img_bytes)
    return cards
