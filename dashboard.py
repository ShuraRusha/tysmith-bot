from PIL import Image, ImageDraw, ImageFont
import io, os, base64, tempfile
from fonts_data import NUNITO_REGULAR, NUNITO_BOLD

W, H = 1080, 810

# ── Palette ──────────────────────────────────────────────────
BG      = (8,  10, 20)
PANEL   = (16, 18, 32)
PANEL2  = (22, 25, 42)
LINE    = (38, 40, 62)
WHITE   = (255, 255, 255)
LGRAY   = (120, 125, 155)
DGRAY   = (52,  55,  78)
GREEN   = (34,  210, 130)
RED     = (235, 70,  70)
AMBER   = (248, 168, 28)
BLUE    = (88,  152, 255)

COIN_ACCENT = {
    "BTC":  (247, 147, 26),
    "ETH":  (98,  126, 234),
    "SOL":  (153, 69,  255),
    "LINK": (42,  90,  218),
}

# ── Layout constants (all Y-positions pre-calculated) ────────
# Each section sits below the previous with explicit gaps.
# This prevents any math errors causing overflow past H=810.
_STRIPE_H   = 5
_HEADER_Y   = 14   ; _HEADER_H  = 68   # 14-82
_PRICE_Y    = 90                        # price text
_VOLC_Y     = 174                       # vol/cap text
_DIV1       = 204                       # after vol/cap (174 + ~26px font + 4px gap)
_SPARK_Y    = 212  ; _SPARK_H   = 78   # 212-290
_DIV2       = 298
_RSI_LBL_Y  = 306                       # "RSI АНАЛИЗ"
_RSI_Y      = 338  ; _RSI_H    = 130   # 338-468
_INTERP_Y   = 472                       # RSI interpretation
_DIV3       = 504
_IND_Y      = 512  ; _IND_H    = 88   # 512-600
_DIV4       = 608
_TS_Y       = 616  ; _TS_H     = 70   # 616-686
_DIV5       = 694
_GM_Y       = 702  ; _GM_H     = 56   # 702-758
_FOOT_DIV   = 766
_FOOT_TXT_Y = 774                       # footer text, font 20 → ends ~796 < 810

# ── Font cache ────────────────────────────────────────────────
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

# ── Drawing helpers ───────────────────────────────────────────
def rnd(draw, x1, y1, x2, y2, fill, r=12):
    """Filled rounded rectangle."""
    if x2 <= x1 or y2 <= y1:
        return
    r = min(r, (x2 - x1) // 2, (y2 - y1) // 2)
    draw.rectangle([x1 + r, y1, x2 - r, y2], fill=fill)
    draw.rectangle([x1, y1 + r, x2, y2 - r], fill=fill)
    for cx, cy in [(x1, y1), (x2 - 2*r, y1), (x1, y2 - 2*r), (x2 - 2*r, y2 - 2*r)]:
        draw.ellipse([cx, cy, cx + 2*r, cy + 2*r], fill=fill)

def bar(draw, x, y, w, h, pct, color):
    """Colored progress bar with rounded ends."""
    rnd(draw, x, y, x + w, y + h, DGRAY, h // 2)
    fw = max(int(w * min(max(pct, 0.0), 1.0)), h)
    rnd(draw, x, y, x + fw, y + h, color, h // 2)

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
    if v <= 42 or v >= 58: return AMBER
    return LGRAY

def rsi_label(v):
    if v >= 70: return "перекуплен"
    if v <= 30: return "перепродан"
    if v <= 42 or v >= 58: return "внимание"
    return "норма"

def _gradient_bg(drw):
    for row in range(H):
        t = row / H
        r = int(BG[0] + (14 - BG[0]) * t)
        g = int(BG[1] + (16 - BG[1]) * t)
        b = int(BG[2] + (30 - BG[2]) * t)
        drw.line([(0, row), (W, row)], fill=(r, g, b))

def _draw_sparkline(img, closes, x, y, w, h, accent):
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
    poly = [(pts[0][0], pad + sh)] + pts + [(pts[-1][0], pad + sh)]
    ld.polygon(poly, fill=(ar, ag, ab, 32))
    for i in range(len(pts) - 1):
        ld.line([pts[i], pts[i + 1]], fill=(ar, ag, ab, 210), width=3)
    lx, ly = pts[-1]
    ld.ellipse([lx - 5, ly - 5, lx + 5, ly + 5], fill=(ar, ag, ab, 255))

    img.paste(layer, (x, y), layer)


# ── Main card generator ───────────────────────────────────────
def generate_coin_card(coin, gdata):
    P = 40  # horizontal padding

    # Fonts
    f10  = font(20)
    f12  = font(24)
    f13  = font(26)
    fb13 = font(26, bold=True)
    fb14 = font(28, bold=True)
    fb16 = font(32, bold=True)
    fb22 = font(44, bold=True)
    fb32 = font(64, bold=True)

    symbol = coin.get("symbol", "?")
    accent = COIN_ACCENT.get(symbol, BLUE)
    action = coin.get("action", "НЕЙТРАЛЬНО")
    sc     = sig_col(action)
    price  = coin.get("price", 0)
    chg    = coin.get("change", 0)
    closes = coin.get("closes", [])
    ar, ag, ab = accent

    # ── Canvas + gradient ────────────────────────────────────
    img = Image.new("RGBA", (W, H), (*BG, 255))
    drw = ImageDraw.Draw(img)
    _gradient_bg(drw)

    # ── Top accent stripe ────────────────────────────────────
    for i in range(_STRIPE_H):
        drw.rectangle([0, i, W, i + 1], fill=accent)

    # ── HEADER ──────────────────────────────────────────────
    # Symbol badge (left)
    rnd(drw, P, _HEADER_Y, P + 130, _HEADER_Y + _HEADER_H, PANEL2, 10)
    drw.rectangle([P, _HEADER_Y, P + 5, _HEADER_Y + _HEADER_H], fill=accent)
    drw.text((P + 18, _HEADER_Y + 10), symbol, font=fb22, fill=accent)

    drw.text((P + 148, _HEADER_Y + 10), "TY SMITH SIGNALS", font=f10, fill=DGRAY)
    drw.text((P + 148, _HEADER_Y + 34), gdata.get("time", "") + " МСК",
             font=f10, fill=DGRAY)

    # Signal badge (right) — width 260 so "НАКАПЛИВАТЬ" fits at 32pt bold
    BADGE_W = 260
    bx = W - P - BADGE_W
    gc = (max(0, ar // 8), max(0, ag // 8), max(0, ab // 8))
    for i in range(3, 0, -1):
        rnd(drw, bx - i*2, _HEADER_Y - i, W - P + i*2, _HEADER_Y + _HEADER_H + i, gc, 16)
    rnd(drw, bx, _HEADER_Y, W - P, _HEADER_Y + _HEADER_H, PANEL2, 14)
    drw.rectangle([bx, _HEADER_Y, bx + 5, _HEADER_Y + _HEADER_H], fill=sc)
    drw.text((bx + 18, _HEADER_Y + 6),  "СИГНАЛ", font=f10,  fill=LGRAY)
    drw.text((bx + 18, _HEADER_Y + 24), action,   font=fb16, fill=sc)
    drw.text((bx + 18, _HEADER_Y + 54),
             f"Score {coin.get('score', 0):+d}  •  {coin.get('conf', '')}",
             font=f10, fill=LGRAY)

    # ── PRICE ────────────────────────────────────────────────
    if price >= 1000:
        price_str = f"${price:,.0f}"
    elif price >= 1:
        price_str = f"${price:,.2f}"
    else:
        price_str = f"${price:.4f}"

    drw.text((P, _PRICE_Y), price_str, font=fb32, fill=WHITE)
    pw = tw(drw, price_str, fb32)

    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
    chg_col = GREEN if chg >= 0 else RED
    cbx = P + pw + 18
    cbw = tw(drw, chg_str, fb14) + 22
    cbh = 36
    cby = _PRICE_Y + 30  # vertically centered relative to price text
    rnd(drw, cbx, cby, cbx + cbw, cby + cbh,
        (int(chg_col[0]*0.16), int(chg_col[1]*0.16), int(chg_col[2]*0.16)), 8)
    drw.text((cbx + 11, cby + 6), chg_str, font=fb14, fill=chg_col)

    vol  = coin.get("vol",  0)
    mcap = coin.get("mcap", 0)
    drw.text((P, _VOLC_Y),
             f"Vol 24h: ${vol/1e9:.2f}B    Cap: ${mcap/1e9:.1f}B",
             font=f12, fill=LGRAY)

    # ── DIVIDER 1 ────────────────────────────────────────────
    drw.line([(P, _DIV1), (W - P, _DIV1)], fill=LINE, width=1)

    # ── SPARKLINE ────────────────────────────────────────────
    rnd(drw, P, _SPARK_Y, W - P, _SPARK_Y + _SPARK_H, PANEL, 8)
    if closes and len(closes) >= 2:
        _draw_sparkline(img, closes,
                        P + 6, _SPARK_Y + 4,
                        W - P * 2 - 12, _SPARK_H - 8, accent)
        drw = ImageDraw.Draw(img)
        mn_c, mx_c = min(closes), max(closes)
        mn_s = f"${mn_c:,.0f}" if mn_c >= 1 else f"${mn_c:.4f}"
        mx_s = f"${mx_c:,.0f}" if mx_c >= 1 else f"${mx_c:.4f}"
        drw.text((P + 10, _SPARK_Y + _SPARK_H - 22), mn_s, font=f10, fill=LGRAY)
        drw.text((W - P - 10 - tw(drw, mx_s, f10), _SPARK_Y + 6),
                 mx_s, font=f10, fill=LGRAY)
    else:
        drw.text((W // 2 - 80, _SPARK_Y + 30), "нет данных графика",
                 font=f12, fill=DGRAY)

    # ── DIVIDER 2 ────────────────────────────────────────────
    drw.line([(P, _DIV2), (W - P, _DIV2)], fill=LINE, width=1)

    # ── RSI SECTION ──────────────────────────────────────────
    drw.text((P, _RSI_LBL_Y), "RSI  АНАЛИЗ", font=fb14, fill=WHITE)

    RSI_BW = (W - P * 2 - 40) // 3  # width of each RSI block

    for idx, (lbl, val, sub) in enumerate([
        ("RSI-6",  coin.get("rsi6",  50), "краткосрочный"),
        ("RSI-12", coin.get("rsi12", 50), "среднесрочный"),
        ("RSI-24", coin.get("rsi24", 50), "долгосрочный"),
    ]):
        bx  = P + idx * (RSI_BW + 20)
        rc  = rsi_col(val)
        rl  = rsi_label(val)

        rnd(drw, bx, _RSI_Y, bx + RSI_BW, _RSI_Y + _RSI_H, PANEL, 12)

        # Top row: label (left) + timeframe (right)
        drw.text((bx + 16, _RSI_Y + 10), lbl, font=fb13, fill=WHITE)
        sw = tw(drw, sub, f10)
        drw.text((bx + RSI_BW - 16 - sw, _RSI_Y + 14), sub, font=f10, fill=LGRAY)

        # Large value — left-aligned, with status label to the right
        val_s = str(val)
        drw.text((bx + 16, _RSI_Y + 40), val_s, font=fb22, fill=rc)
        vw = tw(drw, val_s, fb22)
        drw.text((bx + 16 + vw + 12, _RSI_Y + 60), rl, font=f10, fill=rc)

        # Progress bar with zone markers
        BAR_X = bx + 16
        BAR_Y = _RSI_Y + 92
        BAR_W = RSI_BW - 32
        BAR_H = 8

        # Background bar
        rnd(drw, BAR_X, BAR_Y, BAR_X + BAR_W, BAR_Y + BAR_H, DGRAY, BAR_H // 2)

        # Zone fill: 0-30 green tint, 30-70 neutral, 70-100 red tint
        z30 = BAR_X + int(0.30 * BAR_W)
        z70 = BAR_X + int(0.70 * BAR_W)
        rnd(drw, BAR_X, BAR_Y, z30, BAR_Y + BAR_H,
            (int(GREEN[0]*0.35), int(GREEN[1]*0.35), int(GREEN[2]*0.35)), BAR_H // 2)
        rnd(drw, z70, BAR_Y, BAR_X + BAR_W, BAR_Y + BAR_H,
            (int(RED[0]*0.35), int(RED[1]*0.35), int(RED[2]*0.35)), BAR_H // 2)

        # Colored fill up to current value
        dot_x = BAR_X + int(min(max(val, 0), 100) / 100 * BAR_W)
        fill_w = max(dot_x - BAR_X, BAR_H)
        rnd(drw, BAR_X, BAR_Y, BAR_X + fill_w, BAR_Y + BAR_H, rc, BAR_H // 2)

        # Zone marker lines
        drw.line([(z30, BAR_Y - 4), (z30, BAR_Y + BAR_H + 4)],
                 fill=(60, 64, 90), width=1)
        drw.line([(z70, BAR_Y - 4), (z70, BAR_Y + BAR_H + 4)],
                 fill=(60, 64, 90), width=1)

        # Current value dot
        drw.ellipse([dot_x - 6, BAR_Y - 3, dot_x + 6, BAR_Y + BAR_H + 3], fill=rc)

        # Scale labels
        drw.text((BAR_X, BAR_Y + 14),       "30",  font=f10, fill=DGRAY)
        drw.text((z70 - 8, BAR_Y + 14),     "70",  font=f10, fill=DGRAY)

    # RSI interpretation
    r6, r12, r24 = coin.get("rsi6",50), coin.get("rsi12",50), coin.get("rsi24",50)
    if r6 < 35 and r12 < 35 and r24 < 35:
        imsg, ic = "Все RSI ниже 35 — сильный сигнал входа", GREEN
    elif r6 > 65 and r12 > 65 and r24 > 65:
        imsg, ic = "Все RSI выше 65 — рынок перегрет, осторожно", RED
    elif abs(r6 - r24) > 18:
        imsg, ic = f"RSI расходятся ({r6} vs {r24}) — рынок в переходе", AMBER
    else:
        imsg, ic = "RSI согласованы — тренд стабилен", LGRAY

    drw.rectangle([P, _INTERP_Y + 4, P + 5, _INTERP_Y + 26], fill=ic)
    drw.text((P + 14, _INTERP_Y + 4), imsg, font=f13, fill=ic)

    # ── DIVIDER 3 ────────────────────────────────────────────
    drw.line([(P, _DIV3), (W - P, _DIV3)], fill=LINE, width=1)

    # ── INDICATORS ───────────────────────────────────────────
    IND_W = (W - P * 2 - 40) // 3

    macd_v = coin.get("macd", 0)
    macd_c = GREEN if macd_v > 0 else RED
    macd_s = f"+{macd_v}" if macd_v >= 0 else str(macd_v)
    macd_l = "бычий" if macd_v > 0 else "медвежий"

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

    for idx, (lbl, val_s, sub, col) in enumerate([
        ("MACD",           macd_s,    macd_l,    macd_c),
        (f"FUNDING {frs}", fr_s,      fri[:22],  fr_c),
        ("BOLLINGER",      bp[:18],   "позиция", bp_c),
    ]):
        ix = P + idx * (IND_W + 20)
        rnd(drw, ix, _IND_Y, ix + IND_W, _IND_Y + _IND_H, PANEL, 10)
        drw.rectangle([ix, _IND_Y, ix + 5, _IND_Y + _IND_H], fill=col)
        drw.text((ix + 18, _IND_Y + 10), lbl,   font=f10,  fill=LGRAY)
        drw.text((ix + 18, _IND_Y + 32), val_s, font=fb16, fill=col)
        drw.text((ix + 18, _IND_Y + 70), sub,   font=f10,  fill=DGRAY)

    # ── DIVIDER 4 ────────────────────────────────────────────
    drw.line([(P, _DIV4), (W - P, _DIV4)], fill=LINE, width=1)

    # ── TARGET / STOP ────────────────────────────────────────
    TS_W = (W - P * 2 - 24) // 2
    target = coin.get("target", price)
    stop   = coin.get("stop",   price)
    pct_t  = (target / price - 1) * 100 if price > 0 else 0
    pct_s  = (stop   / price - 1) * 100 if price > 0 else 0

    # Target
    rnd(drw, P, _TS_Y, P + TS_W, _TS_Y + _TS_H, PANEL, 10)
    drw.rectangle([P, _TS_Y, P + 5, _TS_Y + _TS_H], fill=GREEN)
    lbl_t = f"ЦЕЛЬ  {pct_t:+.1f}% от цены"
    drw.text((P + 18, _TS_Y + 8), lbl_t, font=f10, fill=LGRAY)
    drw.text((P + 18, _TS_Y + 26), f"${target:,.2f}" if target < 10 else f"${target:,.0f}",
             font=fb22, fill=GREEN)

    # Stop
    sx = P + TS_W + 24
    rnd(drw, sx, _TS_Y, sx + TS_W, _TS_Y + _TS_H, PANEL, 10)
    drw.rectangle([sx, _TS_Y, sx + 5, _TS_Y + _TS_H], fill=RED)
    lbl_s = f"СТОП-ЛОСС  {pct_s:+.1f}% от цены"
    drw.text((sx + 18, _TS_Y + 8), lbl_s, font=f10, fill=LGRAY)
    drw.text((sx + 18, _TS_Y + 26), f"${stop:,.2f}" if stop < 10 else f"${stop:,.0f}",
             font=fb22, fill=RED)

    # ── DIVIDER 5 ────────────────────────────────────────────
    drw.line([(P, _DIV5), (W - P, _DIV5)], fill=LINE, width=1)

    # ── GLOBAL MARKET ────────────────────────────────────────
    GM_W = (W - P * 2 - 60) // 4

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
        gm.append(("BTC DOM",    f"{dom['dom']}%",       dom["sig"][:15], BLUE))
        gm.append(("MARKET CAP", f"${dom['mcap']:.0f}B", "глобальный",    LGRAY))
        gm.append(("ОБЪ. 24H",   f"${dom['vol']:.0f}B",  "торговый",      LGRAY))
    else:
        for lbl in ("BTC DOM", "MARKET CAP", "ОБЪ. 24H"):
            gm.append((lbl, "N/A", "", DGRAY))

    for idx, (lbl, val_s, sub, col) in enumerate(gm):
        gx = P + idx * (GM_W + 20)
        rnd(drw, gx, _GM_Y, gx + GM_W, _GM_Y + _GM_H, PANEL, 10)
        drw.text((gx + 14, _GM_Y + 5),  lbl,      font=f10,  fill=LGRAY)
        drw.text((gx + 14, _GM_Y + 22), val_s,    font=fb14, fill=col)
        drw.text((gx + 14, _GM_Y + 46), sub[:17], font=f10,  fill=DGRAY)

    # ── FOOTER ───────────────────────────────────────────────
    drw.line([(P, _FOOT_DIV), (W - P, _FOOT_DIV)], fill=LINE, width=1)
    drw.text((P, _FOOT_TXT_Y),
             "Не является финансовой рекомендацией  •  DYOR",
             font=f10, fill=DGRAY)
    next_s = f"Следующий отчёт: {gdata.get('next_hour', '')} МСК"
    drw.text((W - P - tw(drw, next_s, f10), _FOOT_TXT_Y),
             next_s, font=f10, fill=DGRAY)

    # ── Export ───────────────────────────────────────────────
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
