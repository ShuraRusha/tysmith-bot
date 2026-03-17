from PIL import Image, ImageDraw, ImageFont
import io, os, base64, tempfile, re
from fonts_data import NUNITO_REGULAR, NUNITO_BOLD

# Portrait format to match design mockup
W, H = 900, 1120

# ── Palette ──────────────────────────────────────────────────
BG_OUT  = (13,  15,  24)   # outer background
BG_CARD = (20,  23,  36)   # main card
BG_BLOK = (28,  32,  48)   # inner block
BG_BLK2 = (34,  38,  56)   # slightly lighter inner block
LINE    = (44,  48,  70)
WHITE   = (255, 255, 255)
LGRAY   = (108, 114, 145)
DGRAY   = (58,  62,  88)
GREEN   = (42,  196, 140)
RED     = (228, 72,  72)
AMBER   = (244, 164, 28)
BLUE    = (82,  146, 250)

COIN_ACCENT = {
    "BTC":  (42,  185, 145),
    "ETH":  (98,  126, 234),
    "SOL":  (153, 69,  255),
    "LINK": (58,  110, 230),
}

COIN_NAMES = {
    "BTC":  "Bitcoin",
    "ETH":  "Ethereum",
    "SOL":  "Solana",
    "LINK": "Chainlink",
}

RSI_TIMEFRAMES = {
    "RSI-6":  "скальпинг",
    "RSI-12": "интрадей",
    "RSI-24": "свинг",
}

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

def th(draw, text, f):
    bb = draw.textbbox((0, 0), text, font=f)
    return bb[3] - bb[1]

# ── Drawing helpers ───────────────────────────────────────────
def rnd(draw, x1, y1, x2, y2, fill, r=12):
    if x2 <= x1 or y2 <= y1:
        return
    r = min(r, (x2 - x1) // 2, (y2 - y1) // 2)
    draw.rectangle([x1 + r, y1, x2 - r, y2], fill=fill)
    draw.rectangle([x1, y1 + r, x2, y2 - r], fill=fill)
    for cx, cy in [(x1, y1), (x2 - 2*r, y1), (x1, y2 - 2*r), (x2 - 2*r, y2 - 2*r)]:
        draw.ellipse([cx, cy, cx + 2*r, cy + 2*r], fill=fill)

def rnd_outline(draw, x1, y1, x2, y2, color, r=12, lw=1):
    """Draw only the outline of a rounded rectangle."""
    if x2 <= x1 or y2 <= y1:
        return
    r = min(r, (x2 - x1) // 2, (y2 - y1) // 2)
    draw.arc([x1, y1, x1 + 2*r, y1 + 2*r], 180, 270, fill=color, width=lw)
    draw.arc([x2 - 2*r, y1, x2, y1 + 2*r], 270, 360, fill=color, width=lw)
    draw.arc([x1, y2 - 2*r, x1 + 2*r, y2], 90, 180, fill=color, width=lw)
    draw.arc([x2 - 2*r, y2 - 2*r, x2, y2], 0, 90, fill=color, width=lw)
    draw.line([(x1 + r, y1), (x2 - r, y1)], fill=color, width=lw)
    draw.line([(x1 + r, y2), (x2 - r, y2)], fill=color, width=lw)
    draw.line([(x1, y1 + r), (x1, y2 - r)], fill=color, width=lw)
    draw.line([(x2, y1 + r), (x2, y2 - r)], fill=color, width=lw)

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

def bb_pct_from_pos(bb_pos):
    """Extract numeric % from bb_pos string."""
    m = re.search(r'(\d+)%', bb_pos)
    if m:
        return int(m.group(1))
    if "нижней" in bb_pos:
        return 2
    if "верхней" in bb_pos:
        return 98
    return 50

def bb_label(pct):
    if pct <= 20: return "нижняя"
    if pct >= 80: return "верхняя"
    return "середина"


# ── Card generator ────────────────────────────────────────────
def generate_coin_card(coin, gdata):
    OP  = 26   # outer padding (image edge → card)
    IP  = 24   # inner padding (card edge → content)
    CX  = OP + IP           # content X start = 50
    CW  = W - 2 * (OP + IP) # content width  = 800

    symbol  = coin.get("symbol", "?")
    accent  = COIN_ACCENT.get(symbol, BLUE)
    action  = coin.get("action", "НЕЙТРАЛЬНО")
    sc      = sig_col(action)
    price   = coin.get("price", 0)
    chg     = coin.get("change", 0)
    score   = coin.get("score", 0)
    conf    = coin.get("conf", "")
    r6, r12, r24 = coin.get("rsi6",50), coin.get("rsi12",50), coin.get("rsi24",50)
    macd_v  = coin.get("macd", 0)
    fr      = coin.get("funding_rate")
    frs     = coin.get("funding_src", "")
    bb_pos  = coin.get("bb_pos", "н/д")
    bb_pct  = bb_pct_from_pos(bb_pos)
    target  = coin.get("target", price)
    stop    = coin.get("stop",   price)
    pct_t   = (target / price - 1) * 100 if price > 0 else 0
    pct_s   = (stop   / price - 1) * 100 if price > 0 else 0
    vol     = coin.get("vol",  0)
    fg      = gdata.get("fg",  {})
    dom     = gdata.get("dom", {})

    # ── Fonts ────────────────────────────────────────────────
    f9   = font(18)
    f10  = font(20)
    f11  = font(22)
    f12  = font(24)
    fb12 = font(24, bold=True)
    fb14 = font(28, bold=True)
    fb18 = font(36, bold=True)
    fb26 = font(52, bold=True)
    fb36 = font(72, bold=True)

    # ── Canvas ───────────────────────────────────────────────
    img = Image.new("RGB", (W, H), BG_OUT)
    drw = ImageDraw.Draw(img)

    # Main card
    rnd(drw, OP, OP, W - OP, H - OP, BG_CARD, 24)

    # Thin accent stripe at top of card
    ar, ag, ab = accent
    for i in range(4):
        drw.rectangle([OP + 24, OP + i, W - OP - 24, OP + i + 1], fill=accent)

    # ── Y layout ─────────────────────────────────────────────
    # These are Y positions relative to image top.
    Y = OP + IP + 10  # start of content

    # ── HEADER ───────────────────────────────────────────────
    # Coin badge (left)
    badge_txt = symbol
    bw = tw(drw, badge_txt, fb14) + 32
    rnd(drw, CX, Y, CX + bw, Y + 44, accent, 10)
    drw.text((CX + 16, Y + 8), badge_txt, font=fb14, fill=BG_CARD)

    coin_name = COIN_NAMES.get(symbol, symbol)
    drw.text((CX + bw + 16, Y + 12), coin_name, font=f12, fill=LGRAY)

    # Signal box (right)
    SB_W, SB_H = 210, 90
    sbx = CX + CW - SB_W
    rnd(drw, sbx, Y - 4, sbx + SB_W, Y - 4 + SB_H, BG_BLOK, 16)
    rnd_outline(drw, sbx, Y - 4, sbx + SB_W, Y - 4 + SB_H, accent, 16, lw=1)
    drw.text((sbx + 18, Y + 2),  "СИГНАЛ",  font=f9,  fill=LGRAY)
    drw.text((sbx + 18, Y + 20), action,    font=fb14, fill=sc)
    drw.text((sbx + 18, Y + 60), f"Score {score:+d}", font=f10, fill=LGRAY)

    Y += SB_H + 10

    # ── PRICE ────────────────────────────────────────────────
    if price >= 1000:
        price_str = f"${price:,.0f}"
    elif price >= 1:
        price_str = f"${price:,.2f}"
    else:
        price_str = f"${price:.4f}"

    drw.text((CX, Y), price_str, font=fb36, fill=WHITE)

    Y += 78  # price text height

    # Change badge + vol
    chg_str = f"+{chg:.2f}% 24h" if chg >= 0 else f"{chg:.2f}% 24h"
    chg_col = GREEN if chg >= 0 else RED
    cbg = (int(chg_col[0]*0.18), int(chg_col[1]*0.18), int(chg_col[2]*0.18))
    cbw = tw(drw, chg_str, fb12) + 22
    rnd(drw, CX, Y, CX + cbw, Y + 36, cbg, 8)
    drw.text((CX + 11, Y + 6), chg_str, font=fb12, fill=chg_col)

    vol_s = f"Vol: ${vol/1e9:.1f}B" if vol > 0 else ""
    if vol_s:
        drw.text((CX + cbw + 20, Y + 8), vol_s, font=f12, fill=LGRAY)

    Y += 52

    # ── TARGET / STOP ────────────────────────────────────────
    TS_H = 130
    rnd(drw, CX, Y, CX + CW, Y + TS_H, BG_BLOK, 16)
    mid = CX + CW // 2

    # Vertical divider
    drw.line([(mid, Y + 24), (mid, Y + TS_H - 24)], fill=LINE, width=1)

    # Target (left)
    drw.text((CX + 22, Y + 18), "ЦЕЛЬ", font=f9, fill=LGRAY)
    drw.text((CX + 22, Y + 40), f"${target:,.0f}", font=fb26, fill=GREEN)
    drw.text((CX + 22, Y + 98), f"{pct_t:+.1f}% от цены", font=f10, fill=DGRAY)

    # Stop (right)
    drw.text((mid + 22, Y + 18), "СТОП-ЛОСС", font=f9, fill=LGRAY)
    drw.text((mid + 22, Y + 40), f"${stop:,.0f}", font=fb26, fill=RED)
    drw.text((mid + 22, Y + 98), f"{pct_s:+.1f}% от цены", font=f10, fill=DGRAY)

    Y += TS_H + 20

    # ── RSI SECTION ──────────────────────────────────────────
    drw.text((CX, Y), "RSI АНАЛИЗ", font=f10, fill=LGRAY)
    Y += 26

    LBL_W  = 76    # label column width
    VAL_W  = 64    # value column width
    TF_W   = 90    # timeframe column width
    BAR_X  = CX + LBL_W + 12
    BAR_W  = CW - LBL_W - 12 - 12 - VAL_W - 12 - TF_W
    ROW_H  = 50

    for lbl, val, tf in [
        ("RSI-6",  r6,  "скальпинг"),
        ("RSI-12", r12, "интрадей"),
        ("RSI-24", r24, "свинг"),
    ]:
        rc  = rsi_col(val)

        # Label (left)
        drw.text((CX, Y + 14), lbl, font=f11, fill=LGRAY)

        # Bar (center)
        bh = 10
        by = Y + (ROW_H - bh) // 2
        rnd(drw, BAR_X, by, BAR_X + BAR_W, by + bh, BG_BLK2, bh // 2)

        # Zone tints
        z30 = BAR_X + int(0.30 * BAR_W)
        z70 = BAR_X + int(0.70 * BAR_W)
        rnd(drw, BAR_X, by, z30, by + bh,
            (int(GREEN[0]*0.25), int(GREEN[1]*0.25), int(GREEN[2]*0.25)), bh//2)
        rnd(drw, z70, by, BAR_X + BAR_W, by + bh,
            (int(RED[0]*0.25), int(RED[1]*0.25), int(RED[2]*0.25)), bh//2)

        # Colored fill
        fill_end = BAR_X + int(min(max(val, 0), 100) / 100 * BAR_W)
        fill_w   = max(fill_end - BAR_X, bh)
        rnd(drw, BAR_X, by, BAR_X + fill_w, by + bh, rc, bh // 2)

        # Value (right of bar)
        val_s = str(val)
        vx = BAR_X + BAR_W + 14
        drw.text((vx, Y + 10), val_s, font=fb14, fill=rc)
        vw = tw(drw, val_s, fb14)

        # Timeframe (far right)
        drw.text((vx + vw + 12, Y + 14), tf, font=f9, fill=DGRAY)

        Y += ROW_H

    # RSI interpretation
    if r6 < 35 and r12 < 35 and r24 < 35:
        imsg, ic = "Все RSI ниже 35 — сильный сигнал входа", GREEN
    elif r6 > 65 and r12 > 65 and r24 > 65:
        imsg, ic = "Все RSI выше 65 — рынок перегрет, осторожно", RED
    elif abs(r6 - r24) > 18:
        imsg, ic = f"RSI расходятся ({r6} vs {r24}) — рынок в переходе", AMBER
    else:
        imsg, ic = "RSI согласованы — тренд стабилен", LGRAY

    Y += 6
    rnd(drw, CX, Y, CX + CW, Y + 56, BG_BLOK, 12)
    drw.rectangle([CX, Y + 12, CX + 4, Y + 44], fill=ic)
    drw.text((CX + 18, Y + 16), imsg, font=f11, fill=ic)

    Y += 72

    # ── INDICATORS ───────────────────────────────────────────
    IND_W = (CW - 28) // 3
    IND_H = 112

    macd_c = GREEN if macd_v > 0 else RED
    macd_s = f"+{macd_v}" if macd_v >= 0 else str(macd_v)
    macd_l = "бычий" if macd_v > 0 else "медвежий"

    if fr is not None:
        fr_s = f"{fr:+.4f}%"
        fr_c = RED if fr > 0.1 else AMBER if fr > 0.05 else LGRAY if fr > -0.02 else GREEN
        fr_l = coin.get("funding_interp", "")[:16].lower()
    else:
        fr_s, fr_c, fr_l = "N/A", DGRAY, "нет данных"

    bb_c = GREEN if bb_pct <= 25 else RED if bb_pct >= 75 else AMBER
    bb_s = f"{bb_pct}%"
    bb_l = bb_label(bb_pct)

    for idx, (lbl, val_s, sub, col) in enumerate([
        ("MACD",    macd_s, macd_l, macd_c),
        ("FUNDING", fr_s,   fr_l,   fr_c),
        ("BOLLINGER", bb_s, bb_l,   bb_c),
    ]):
        ix = CX + idx * (IND_W + 14)
        rnd(drw, ix, Y, ix + IND_W, Y + IND_H, BG_BLOK, 14)
        drw.text((ix + 16, Y + 14), lbl,   font=f9,   fill=LGRAY)
        drw.text((ix + 16, Y + 36), val_s, font=fb18, fill=col)
        drw.text((ix + 16, Y + 80), sub,   font=f10,  fill=DGRAY)

    Y += IND_H + 20

    # ── GLOBAL MARKET ────────────────────────────────────────
    GM_H = 118
    rnd(drw, CX, Y, CX + CW, Y + GM_H, BG_BLOK, 14)

    gm_cols = []
    if fg.get("ok"):
        fv = fg["value"]
        fc = RED if fv <= 25 else AMBER if fv <= 45 else LGRAY if fv <= 55 else GREEN
        gm_cols.append(("FEAR & GREED", f"{fv}", fg.get("label","")[:12], fc))
    else:
        gm_cols.append(("FEAR & GREED", "N/A", "", DGRAY))

    if dom.get("ok"):
        gm_cols.append(("BTC DOM",    f"{dom['dom']}%",       dom["sig"][:18], BLUE))
        gm_cols.append(("КАП. РЫНКА", f"${dom['mcap']:.0f}B", "",             LGRAY))
    else:
        gm_cols += [("BTC DOM","N/A","",DGRAY), ("КАП. РЫНКА","N/A","",DGRAY)]

    col_w = CW // len(gm_cols)
    for idx, (lbl, val_s, sub, col) in enumerate(gm_cols):
        gx = CX + idx * col_w
        if idx > 0:
            drw.line([(gx, Y + 24), (gx, Y + GM_H - 24)], fill=LINE, width=1)
        drw.text((gx + 22, Y + 14), lbl,   font=f9,   fill=LGRAY)
        drw.text((gx + 22, Y + 38), val_s, font=fb18, fill=col)
        if sub:
            drw.text((gx + 22, Y + 82), sub, font=f10, fill=DGRAY)

    Y += GM_H + 18

    # ── FOOTER ───────────────────────────────────────────────
    drw.text((CX, Y + 4), "TY SMITH SIGNALS", font=f9, fill=DGRAY)
    ts = f"{gdata.get('time', '')} МСК"
    drw.text((CX + CW - tw(drw, ts, f9), Y + 4), ts, font=f9, fill=DGRAY)

    # ── Export ───────────────────────────────────────────────
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
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
