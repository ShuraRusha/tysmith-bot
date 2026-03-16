from PIL import Image, ImageDraw, ImageFont
import io, os, base64, tempfile, math
from fonts_data import NUNITO_REGULAR, NUNITO_BOLD

W, H = 1080, 810

# ── Palette ──────────────────────────────────────────────────
BG      = (8,  10, 20)
PANEL   = (16, 18, 32)
PANEL2  = (22, 25, 42)
LINE    = (35, 38, 60)
WHITE   = (255, 255, 255)
LGRAY   = (120, 125, 155)
DGRAY   = (50,  53,  75)
GREEN   = (30,  210, 130)
RED     = (240, 75,  75)
AMBER   = (250, 170, 30)
BLUE    = (90,  155, 255)

COIN_ACCENT = {
    "BTC":  (247, 147, 26),
    "ETH":  (98,  126, 234),
    "SOL":  (153, 69,  255),
    "LINK": (42,  90,  218),
}

_FONT_CACHE = {}

def _write_font(name, b64data):
    path = os.path.join(tempfile.gettempdir(), name)
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(base64.b64decode(b64data))
    return path

REG_PATH  = _write_font("Nunito-Regular.ttf", NUNITO_REGULAR)
BOLD_PATH = _write_font("Nunito-Bold.ttf",    NUNITO_BOLD)

def font(size, bold=False):
    key = (size, bold)
    if key not in _FONT_CACHE:
        _FONT_CACHE[key] = ImageFont.truetype(BOLD_PATH if bold else REG_PATH, size)
    return _FONT_CACHE[key]

def tw(draw, text, f):
    bb = draw.textbbox((0, 0), text, font=f)
    return bb[2] - bb[0]

def rnd(draw, x1, y1, x2, y2, fill, r=12):
    if x2 <= x1 or y2 <= y1:
        return
    r = min(r, (x2 - x1) // 2, (y2 - y1) // 2)
    draw.rectangle([x1 + r, y1, x2 - r, y2], fill=fill)
    draw.rectangle([x1, y1 + r, x2, y2 - r], fill=fill)
    for cx, cy in [(x1, y1), (x2 - 2*r, y1), (x1, y2 - 2*r), (x2 - 2*r, y2 - 2*r)]:
        draw.ellipse([cx, cy, cx + 2*r, cy + 2*r], fill=fill)

def sig_col(action):
    a = action.upper()
    if "ПОКУПАТЬ"    in a: return GREEN
    if "НАКАПЛИВАТЬ" in a: return BLUE
    if "ПРОДАВАТЬ"   in a: return RED
    if "ОСТОРОЖНО"   in a: return AMBER
    return LGRAY

def rsi_col(v):
    if v >= 70: return RED
    if v <= 30: return GREEN
    if v <= 40 or v >= 60: return AMBER
    return LGRAY

def _gradient_bg(drw):
    for row in range(H):
        t = row / H
        r = int(BG[0] + (14 - BG[0]) * t)
        g = int(BG[1] + (16 - BG[1]) * t)
        b = int(BG[2] + (28 - BG[2]) * t)
        drw.line([(0, row), (W, row)], fill=(r, g, b))

def _draw_sparkline(img, closes, x, y, w, h, accent):
    """Draw a transparent sparkline layer and paste onto img."""
    if not closes or len(closes) < 2:
        return
    mn, mx = min(closes), max(closes)
    rng = mx - mn if mx > mn else 1

    layer = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    ld = ImageDraw.Draw(layer)

    pad = 8
    sw, sh = w - pad * 2, h - pad * 2
    pts = []
    for i, c in enumerate(closes):
        px = pad + int(i / (len(closes) - 1) * sw)
        py = pad + sh - int((c - mn) / rng * sh)
        pts.append((px, py))

    ar, ag, ab = accent
    # Filled area under line
    poly = [(pts[0][0], pad + sh)] + pts + [(pts[-1][0], pad + sh)]
    ld.polygon(poly, fill=(ar, ag, ab, 35))

    # Line
    for i in range(len(pts) - 1):
        ld.line([pts[i], pts[i + 1]], fill=(ar, ag, ab, 200), width=3)

    # Guide lines
    ld.line([(pad, pad),      (pad + sw, pad)],      fill=(255, 255, 255, 18), width=1)
    ld.line([(pad, pad + sh), (pad + sw, pad + sh)], fill=(255, 255, 255, 18), width=1)

    # Last price dot
    lx, ly = pts[-1]
    ld.ellipse([lx - 7, ly - 7, lx + 7, ly + 7], fill=(ar, ag, ab, 60))
    ld.ellipse([lx - 4, ly - 4, lx + 4, ly + 4], fill=(ar, ag, ab, 255))

    img.paste(layer, (x, y), layer)

def _draw_rsi_gauge(img, cx, cy, r, val, color):
    """Draw a bottom-semicircular arc gauge as RGBA layer, paste onto img."""
    pad = 14
    size = (r + pad) * 2
    layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    ld = ImageDraw.Draw(layer)
    lc = pad + r  # center within layer

    # Background arc (180 → 360)
    ld.arc([lc - r, lc - r, lc + r, lc + r],
           start=180, end=360, fill=(*DGRAY, 200), width=10)

    # Value arc
    end_a = 180 + int(min(max(val, 0), 100) * 1.8)
    rc = rsi_col(val)
    if end_a > 180:
        ld.arc([lc - r, lc - r, lc + r, lc + r],
               start=180, end=end_a, fill=(*rc, 230), width=10)

    # Endpoint dot
    rad = math.radians(end_a)
    dx = int(lc + r * math.cos(rad))
    dy = int(lc + r * math.sin(rad))
    ld.ellipse([dx - 6, dy - 6, dx + 6, dy + 6], fill=(*rc, 255))

    img.paste(layer, (cx - lc, cy - lc), layer)


def generate_coin_card(coin, gdata):
    P = 42  # horizontal padding

    f10  = font(20)
    f12  = font(24)
    f13  = font(26)
    fb14 = font(28, bold=True)
    fb16 = font(32, bold=True)
    fb18 = font(36, bold=True)
    fb24 = font(48, bold=True)
    fb36 = font(72, bold=True)

    symbol = coin.get("symbol", "?")
    accent = COIN_ACCENT.get(symbol, BLUE)
    action = coin.get("action", "НЕЙТРАЛЬНО")
    sc     = sig_col(action)
    price  = coin.get("price", 0)
    chg    = coin.get("change", 0)
    closes = coin.get("closes", [])

    # ── Canvas + gradient ───────────────────────────────────
    img = Image.new("RGBA", (W, H), (*BG, 255))
    drw = ImageDraw.Draw(img)
    _gradient_bg(drw)

    # ── Top accent stripe ────────────────────────────────────
    for i in range(5):
        drw.rectangle([0, i, W, i + 1], fill=accent)

    ar, ag, ab = accent

    # ── Header ───────────────────────────────────────────────
    rnd(drw, P, 14, P + 120, 76, PANEL2, 10)
    drw.rectangle([P, 14, P + 5, 76], fill=accent)
    drw.text((P + 18, 18), symbol, font=fb24, fill=accent)
    drw.text((P + 134, 20), "TY SMITH SIGNALS", font=f10, fill=DGRAY)
    drw.text((P + 134, 46), gdata.get("time", "") + " МСК", font=f10, fill=DGRAY)

    # Signal badge
    badge_w = 270
    bx = W - P - badge_w
    gc = (max(0, int(ar * 0.12)), max(0, int(ag * 0.12)), max(0, int(ab * 0.12)))
    for i in range(3, 0, -1):
        rnd(drw, bx - i*2, 10 - i, W - P + i*2, 82 + i, gc, 16)
    rnd(drw, bx, 12, W - P, 80, PANEL2, 14)
    drw.rectangle([bx, 12, bx + 5, 80], fill=sc)
    drw.text((bx + 18, 18), "СИГНАЛ", font=f10,  fill=LGRAY)
    drw.text((bx + 18, 36), action,   font=fb16, fill=sc)
    drw.text((bx + 18, 64),
             f"Score {coin.get('score', 0):+d}  •  {coin.get('conf', '')}",
             font=f10, fill=LGRAY)

    # ── Price ───────────────────────────────────────────────
    if price >= 1000:
        price_str = f"${price:,.0f}"
    elif price >= 1:
        price_str = f"${price:,.2f}"
    else:
        price_str = f"${price:.4f}"

    drw.text((P, 88), price_str, font=fb36, fill=WHITE)
    pw = tw(drw, price_str, fb36)

    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
    chg_col = GREEN if chg >= 0 else RED
    cbx = P + pw + 20
    cbw = tw(drw, chg_str, fb14) + 24
    rnd(drw, cbx, 130, cbx + cbw, 168,
        (int(chg_col[0]*0.18), int(chg_col[1]*0.18), int(chg_col[2]*0.18)), 8)
    drw.text((cbx + 12, 136), chg_str, font=fb14, fill=chg_col)

    vol  = coin.get("vol",  0)
    mcap = coin.get("mcap", 0)
    drw.text((P, 176),
             f"Vol 24h: ${vol/1e9:.2f}B    Cap: ${mcap/1e9:.1f}B",
             font=f12, fill=LGRAY)

    # ── Divider 1 ────────────────────────────────────────────
    drw.line([(P, 200), (W - P, 200)], fill=LINE, width=1)

    # ── Sparkline ───────────────────────────────────────────
    SP_Y, SP_H = 208, 88
    rnd(drw, P, SP_Y, W - P, SP_Y + SP_H, PANEL, 8)
    if closes and len(closes) >= 2:
        _draw_sparkline(img, closes, P + 6, SP_Y + 4, W - P*2 - 12, SP_H - 8, accent)
        drw = ImageDraw.Draw(img)
        mn_c, mx_c = min(closes), max(closes)
        mn_s = f"${mn_c:,.0f}" if mn_c >= 1 else f"${mn_c:.4f}"
        mx_s = f"${mx_c:,.0f}" if mx_c >= 1 else f"${mx_c:.4f}"
        drw.text((P + 10, SP_Y + SP_H - 22), mn_s, font=f10, fill=LGRAY)
        drw.text((W - P - 10 - tw(drw, mx_s, f10), SP_Y + 6), mx_s, font=f10, fill=LGRAY)
    else:
        drw.text((W//2 - 70, SP_Y + 32), "нет данных графика", font=f12, fill=DGRAY)

    # ── Divider 2 ────────────────────────────────────────────
    Y2 = SP_Y + SP_H + 8
    drw.line([(P, Y2), (W - P, Y2)], fill=LINE, width=1)

    # ── RSI Section ─────────────────────────────────────────
    RSI_LBL_Y = Y2 + 12
    drw.text((P, RSI_LBL_Y), "RSI  АНАЛИЗ", font=fb14, fill=WHITE)

    RSI_Y = RSI_LBL_Y + 36
    RSI_H = 140
    RSI_BW = (W - P*2 - 40) // 3

    for idx, (lbl, val, sub) in enumerate([
        ("RSI-6",  coin.get("rsi6",  50), "краткосрочный"),
        ("RSI-12", coin.get("rsi12", 50), "среднесрочный"),
        ("RSI-24", coin.get("rsi24", 50), "долгосрочный"),
    ]):
        bx  = P + idx * (RSI_BW + 20)
        rc  = rsi_col(val)
        rnd(drw, bx, RSI_Y, bx + RSI_BW, RSI_Y + RSI_H, PANEL, 12)

        lw = tw(drw, lbl, fb14)
        drw.text((bx + RSI_BW//2 - lw//2, RSI_Y + 10), lbl, font=fb14, fill=WHITE)

        g_cx, g_cy, g_r = bx + RSI_BW//2, RSI_Y + 76, 42
        _draw_rsi_gauge(img, g_cx, g_cy, g_r, val, rc)
        drw = ImageDraw.Draw(img)

        val_s = str(val)
        vw = tw(drw, val_s, fb18)
        drw.text((g_cx - vw//2, g_cy - 20), val_s, font=fb18, fill=rc)

        sw2 = tw(drw, sub, f10)
        drw.text((bx + RSI_BW//2 - sw2//2, RSI_Y + RSI_H - 18), sub, font=f10, fill=LGRAY)

    # RSI interpretation line
    r6, r12, r24 = coin.get("rsi6",50), coin.get("rsi12",50), coin.get("rsi24",50)
    IMSG_Y = RSI_Y + RSI_H + 8
    if r6 < 35 and r12 < 35 and r24 < 35:
        imsg, ic = "Все RSI ниже 35 — сильный сигнал входа", GREEN
    elif r6 > 65 and r12 > 65 and r24 > 65:
        imsg, ic = "Все RSI выше 65 — рынок перегрет, осторожно", RED
    elif abs(r6 - r24) > 20:
        imsg, ic = f"RSI расходятся ({r6} vs {r24}) — рынок в переходе", AMBER
    else:
        imsg, ic = "RSI согласованы — тренд стабилен", LGRAY
    drw.rectangle([P, IMSG_Y + 4, P + 5, IMSG_Y + 26], fill=ic)
    drw.text((P + 14, IMSG_Y + 4), imsg, font=f13, fill=ic)

    # ── Divider 3 ────────────────────────────────────────────
    Y3 = IMSG_Y + 36
    drw.line([(P, Y3), (W - P, Y3)], fill=LINE, width=1)

    # ── Indicators ──────────────────────────────────────────
    IND_Y = Y3 + 10
    IND_H = 80
    IND_W = (W - P*2 - 40) // 3

    macd_v = coin.get("macd", 0)
    macd_c = GREEN if macd_v > 0 else RED
    macd_s = f"+{macd_v}" if macd_v >= 0 else str(macd_v)

    fr  = coin.get("funding_rate")
    frs = coin.get("funding_src", "")
    fri = coin.get("funding_interp", "нет данных")
    if fr is not None:
        fr_s = f"{fr:+.4f}%"
        fr_c = RED if fr > 0.1 else AMBER if fr > 0.05 else LGRAY if fr > -0.02 else GREEN
    else:
        fr_s, fr_c, fri, frs = "N/A", DGRAY, "нет данных", ""

    bp   = coin.get("bb_pos", "н/д")
    bp_c = GREEN if "нижней" in bp else RED if "верхней" in bp else AMBER if "%" in bp else LGRAY

    for idx, (lbl, val, sub, col) in enumerate([
        ("MACD",              macd_s,    "бычий" if macd_v > 0 else "медвежий", macd_c),
        (f"FUNDING {frs}",    fr_s,      fri[:20],                               fr_c),
        ("BOLLINGER",         bp[:16],   "позиция",                              bp_c),
    ]):
        ix = P + idx * (IND_W + 20)
        rnd(drw, ix, IND_Y, ix + IND_W, IND_Y + IND_H, PANEL, 10)
        drw.rectangle([ix, IND_Y, ix + 5, IND_Y + IND_H], fill=col)
        drw.text((ix + 18, IND_Y + 8),  lbl, font=f10,  fill=LGRAY)
        drw.text((ix + 18, IND_Y + 28), val, font=fb18, fill=col)
        drw.text((ix + 18, IND_Y + 62), sub, font=f10,  fill=DGRAY)

    # ── Divider 4 ────────────────────────────────────────────
    Y4 = IND_Y + IND_H + 10
    drw.line([(P, Y4), (W - P, Y4)], fill=LINE, width=1)

    # ── Target / Stop ────────────────────────────────────────
    TS_Y = Y4 + 10
    TS_H = 82
    TS_W = (W - P*2 - 24) // 2

    target = coin.get("target", price)
    stop   = coin.get("stop",   price)
    pct_t  = (target / price - 1) * 100 if price > 0 else 0
    pct_s  = (stop   / price - 1) * 100 if price > 0 else 0

    rnd(drw, P, TS_Y, P + TS_W, TS_Y + TS_H, PANEL, 10)
    drw.rectangle([P, TS_Y, P + 5, TS_Y + TS_H], fill=GREEN)
    drw.text((P + 18, TS_Y + 8),  "ЦЕЛЬ",             font=f10,  fill=LGRAY)
    drw.text((P + 18, TS_Y + 28), f"${target:,.0f}",  font=fb18, fill=GREEN)
    drw.text((P + 18, TS_Y + 64), f"{pct_t:+.1f}% от цены", font=f10, fill=DGRAY)

    sx = P + TS_W + 24
    rnd(drw, sx, TS_Y, sx + TS_W, TS_Y + TS_H, PANEL, 10)
    drw.rectangle([sx, TS_Y, sx + 5, TS_Y + TS_H], fill=RED)
    drw.text((sx + 18, TS_Y + 8),  "СТОП-ЛОСС",       font=f10,  fill=LGRAY)
    drw.text((sx + 18, TS_Y + 28), f"${stop:,.0f}",   font=fb18, fill=RED)
    drw.text((sx + 18, TS_Y + 64), f"{pct_s:.1f}% от цены", font=f10, fill=DGRAY)

    # ── Divider 5 ────────────────────────────────────────────
    Y5 = TS_Y + TS_H + 10
    drw.line([(P, Y5), (W - P, Y5)], fill=LINE, width=1)

    # ── Global market ────────────────────────────────────────
    GM_Y = Y5 + 10
    GM_H = 50
    GM_W = (W - P*2 - 60) // 4

    fg  = gdata.get("fg",  {})
    dom = gdata.get("dom", {})
    gm  = []

    if fg.get("ok"):
        fv = fg["value"]
        fc = RED if fv <= 25 else AMBER if fv <= 45 else LGRAY if fv <= 55 else GREEN
        gm.append(("FEAR & GREED", f"{fv}/100", fg.get("label","")[:14], fc))
    else:
        gm.append(("FEAR & GREED", "N/A", "недоступно", DGRAY))

    if dom.get("ok"):
        gm.append(("BTC DOM",    f"{dom['dom']}%",       dom["sig"][:16], BLUE))
        gm.append(("MARKET CAP", f"${dom['mcap']:.0f}B", "глобальный",    LGRAY))
        gm.append(("ОБЪ. 24H",   f"${dom['vol']:.0f}B",  "торговый",      LGRAY))
    else:
        for lbl in ("BTC DOM", "MARKET CAP", "ОБЪ. 24H"):
            gm.append((lbl, "N/A", "", DGRAY))

    for idx, (lbl, val, sub, col) in enumerate(gm):
        gx = P + idx * (GM_W + 20)
        rnd(drw, gx, GM_Y, gx + GM_W, GM_Y + GM_H, PANEL, 10)
        drw.text((gx + 14, GM_Y + 4),  lbl,      font=f10,  fill=LGRAY)
        drw.text((gx + 14, GM_Y + 22), val,      font=fb14, fill=col)
        drw.text((gx + 14, GM_Y + 44), sub[:18], font=f10,  fill=DGRAY)

    # ── Footer ───────────────────────────────────────────────
    Y_FOOT = GM_Y + GM_H + 2
    drw.line([(P, Y_FOOT), (W - P, Y_FOOT)], fill=LINE, width=1)
    drw.text((P, Y_FOOT + 8),
             "Не является финансовой рекомендацией  •  DYOR",
             font=f10, fill=DGRAY)
    next_s = f"Следующий отчёт: {gdata.get('next_hour', '')} МСК"
    drw.text((W - P - tw(drw, next_s, f10), Y_FOOT + 8), next_s, font=f10, fill=DGRAY)

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.read()


def generate_all_cards(data):
    cards = []
    for coin in data.get("coins", []):
        try:
            cards.append(generate_coin_card(coin, data))
        except Exception as e:
            print(f"Card error {coin.get('symbol','?')}: {e}", flush=True)
    return cards
