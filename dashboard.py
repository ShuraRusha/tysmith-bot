from PIL import Image, ImageDraw, ImageFont, ImageFilter
import io, os, base64, tempfile, re
from fonts_data import NUNITO_REGULAR, NUNITO_BOLD

# Output size
W, H = 900, 1260
# Supersampling scale — draw at 2× then downscale for crisp anti-aliasing
S = 2
WS, HS = W * S, H * S

# ── Palette ──────────────────────────────────────────────────
BG_OUT  = (10,  12,  22)
BG_CARD = (19,  22,  35)
BG_BLOK = (27,  31,  47)
BG_BLK2 = (33,  37,  55)
LINE    = (46,  50,  74)
WHITE   = (255, 255, 255)
LGRAY   = (110, 116, 148)
DGRAY   = (60,  64,  90)
GREEN   = (46,  204, 148)
RED     = (232, 74,  74)
AMBER   = (246, 168, 30)
BLUE    = (84,  150, 252)

COIN_ACCENT = {
    "BTC":  (46,  194, 152),
    "ETH":  (100, 128, 238),
    "SOL":  (156, 72,  255),
    "LINK": (60,  114, 234),
}
COIN_NAMES = {
    "BTC": "Bitcoin", "ETH": "Ethereum",
    "SOL": "Solana",  "LINK": "Chainlink",
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
        _FONT_CACHE[key] = ImageFont.truetype(BOLD_PATH if bold else REG_PATH, size * S)
    return _FONT_CACHE[key]

def tw(draw, text, f):
    bb = draw.textbbox((0, 0), text, font=f)
    return bb[2] - bb[0]

def th(draw, text, f):
    bb = draw.textbbox((0, 0), text, font=f)
    return bb[3] - bb[1]

def s(v):
    """Scale a logical pixel value to supersampled space."""
    return v * S

# ── Drawing helpers ───────────────────────────────────────────
def rnd(draw, x1, y1, x2, y2, fill, r=12):
    x1, y1, x2, y2, r = s(x1), s(y1), s(x2), s(y2), s(r)
    if x2 <= x1 or y2 <= y1:
        return
    r = min(r, (x2 - x1) // 2, (y2 - y1) // 2)
    draw.rectangle([x1 + r, y1, x2 - r, y2], fill=fill)
    draw.rectangle([x1, y1 + r, x2, y2 - r], fill=fill)
    for cx, cy in [(x1, y1), (x2 - 2*r, y1), (x1, y2 - 2*r), (x2 - 2*r, y2 - 2*r)]:
        draw.ellipse([cx, cy, cx + 2*r, cy + 2*r], fill=fill)

def rnd_outline(draw, x1, y1, x2, y2, color, r=12, lw=1):
    x1, y1, x2, y2, r, lw = s(x1), s(y1), s(x2), s(y2), s(r), s(lw)
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

def line(draw, x1, y1, x2, y2, fill, width=1):
    draw.line([(s(x1), s(y1)), (s(x2), s(y2))], fill=fill, width=s(width))

def rect(draw, x1, y1, x2, y2, fill):
    draw.rectangle([s(x1), s(y1), s(x2), s(y2)], fill=fill)

def text(draw, x, y, txt, f, fill):
    draw.text((s(x), s(y)), txt, font=f, fill=fill)

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

def bb_pct_from_pos(bb_pos):
    m = re.search(r'(\d+)%', bb_pos)
    if m:
        return int(m.group(1))
    if "нижней" in bb_pos: return 2
    if "верхней" in bb_pos: return 98
    return 50

def bb_label(pct):
    if pct <= 20: return "нижняя"
    if pct >= 80: return "верхняя"
    return "середина"

def tint(color, alpha):
    """Return darkened/tinted version of color for backgrounds."""
    return tuple(int(c * alpha) for c in color)


# ── Card generator ────────────────────────────────────────────
def generate_coin_card(coin, gdata):
    OP  = 26
    IP  = 24
    CX  = OP + IP
    CW  = W - 2 * (OP + IP)

    symbol = coin.get("symbol", "?")
    accent = COIN_ACCENT.get(symbol, BLUE)
    action = coin.get("action", "НЕЙТРАЛЬНО")
    sc     = sig_col(action)
    price  = coin.get("price", 0)
    chg    = coin.get("change", 0)
    score  = coin.get("score", 0)
    r6, r12, r24 = coin.get("rsi6", 50), coin.get("rsi12", 50), coin.get("rsi24", 50)
    macd_v    = coin.get("macd", 0)
    macd_hist = coin.get("macd_hist", [])
    fr        = coin.get("funding_rate")
    bb_pos = coin.get("bb_pos", "н/д")
    bb_pct = bb_pct_from_pos(bb_pos)
    target = coin.get("target", price)
    stop   = coin.get("stop",   price)
    pct_t  = (target / price - 1) * 100 if price > 0 else 0
    pct_s  = (stop   / price - 1) * 100 if price > 0 else 0
    vol    = coin.get("vol", 0)
    fg     = gdata.get("fg",  {})
    dom    = gdata.get("dom", {})

    # ── Fonts (logical sizes — will be rendered at 2×) ────────
    f9   = font(18)
    f10  = font(20)
    f11  = font(22)
    f12  = font(24)
    fb12 = font(24, bold=True)
    fb14 = font(28, bold=True)
    fb18 = font(36, bold=True)
    fb26 = font(52, bold=True)
    fb36 = font(72, bold=True)

    # ── Canvas at 2× ─────────────────────────────────────────
    img = Image.new("RGB", (WS, HS), BG_OUT)
    drw = ImageDraw.Draw(img)

    # Subtle vertical gradient on outer background
    for yi in range(HS):
        t = yi / HS
        c = tuple(int(BG_OUT[i] + (BG_CARD[i] - BG_OUT[i]) * t * 0.4) for i in range(3))
        drw.line([(0, yi), (WS, yi)], fill=c)

    # Drop shadow under main card (offset 4px down, clipped to image)
    shadow_col = (6, 7, 14)
    rnd(drw, OP + 4, OP + 6, W - OP - 2, H - OP + 4, shadow_col, 24)

    # Main card
    rnd(drw, OP, OP, W - OP, H - OP, BG_CARD, 24)

    # Accent glow strip at top of card (3 layers: glow then solid)
    ar, ag, ab = accent
    for i in range(6):
        alpha = 0.15 + i * 0.14
        col = (int(ar * alpha + BG_CARD[0] * (1 - alpha)),
               int(ag * alpha + BG_CARD[1] * (1 - alpha)),
               int(ab * alpha + BG_CARD[2] * (1 - alpha)))
        drw.rectangle([s(OP + 24), s(OP) + i * S,
                        s(W - OP - 24), s(OP) + (i + 1) * S], fill=col)
    # Solid stripe top 3px
    drw.rectangle([s(OP + 24), s(OP),
                    s(W - OP - 24), s(OP) + 3 * S], fill=accent)

    # ── Y layout ─────────────────────────────────────────────
    Y = OP + IP + 12

    # ── HEADER ───────────────────────────────────────────────
    badge_txt = symbol
    bw = tw(drw, badge_txt, fb14) // S + 36
    rnd(drw, CX, Y, CX + bw, Y + 46, accent, 10)
    text(drw, CX + 18, Y + 9, badge_txt, fb14, BG_CARD)

    coin_name = COIN_NAMES.get(symbol, symbol)
    text(drw, CX + bw + 16, Y + 13, coin_name, f12, LGRAY)

    # Signal box (right) — auto-width so long actions fit
    SB_H   = 92
    act_w  = tw(drw, action, fb14) // S
    SB_W   = max(act_w + 44, 210)
    sbx    = CX + CW - SB_W
    sby    = Y - 4
    rnd(drw, sbx, sby, sbx + SB_W, sby + SB_H, BG_BLOK, 16)
    rnd_outline(drw, sbx, sby, sbx + SB_W, sby + SB_H, accent, 16, lw=1)
    text(drw, sbx + 18, sby + 8,  "СИГНАЛ", f9,   LGRAY)
    text(drw, sbx + 18, sby + 26, action,   fb14, sc)
    text(drw, sbx + 18, sby + 66, f"Score {score:+d}", f10, LGRAY)

    Y += SB_H + 10

    # ── PRICE ────────────────────────────────────────────────
    if price >= 1000:
        price_str = f"${price:,.0f}"
    elif price >= 1:
        price_str = f"${price:,.2f}"
    else:
        price_str = f"${price:.4f}"

    text(drw, CX, Y, price_str, fb36, WHITE)
    Y += 80

    # Change badge
    chg_str = f"+{chg:.2f}% 24h" if chg >= 0 else f"{chg:.2f}% 24h"
    chg_col = GREEN if chg >= 0 else RED
    cbg = tint(chg_col, 0.16)
    cbw = tw(drw, chg_str, fb12) // S + 24
    rnd(drw, CX, Y, CX + cbw, Y + 36, cbg, 8)
    text(drw, CX + 12, Y + 7, chg_str, fb12, chg_col)

    vol_s = f"Vol: ${vol/1e9:.1f}B" if vol > 0 else ""
    if vol_s:
        text(drw, CX + cbw + 20, Y + 9, vol_s, f12, LGRAY)

    Y += 52

    # ── TARGET / STOP ────────────────────────────────────────
    TS_H = 132
    rnd(drw, CX, Y, CX + CW, Y + TS_H, BG_BLOK, 16)
    mid = CX + CW // 2

    line(drw, mid, Y + 22, mid, Y + TS_H - 22, LINE, 1)

    text(drw, CX + 22, Y + 16, "ЦЕЛЬ",      f9,   LGRAY)
    text(drw, CX + 22, Y + 38, f"${target:,.0f}", fb26, GREEN)
    text(drw, CX + 22, Y + 100, f"{pct_t:+.1f}% от цены", f10, DGRAY)

    text(drw, mid + 22, Y + 16, "СТОП-ЛОСС", f9,   LGRAY)
    text(drw, mid + 22, Y + 38, f"${stop:,.0f}", fb26, RED)
    text(drw, mid + 22, Y + 100, f"{pct_s:+.1f}% от цены", f10, DGRAY)

    Y += TS_H + 18

    # ── RSI SECTION ──────────────────────────────────────────
    text(drw, CX, Y, "RSI АНАЛИЗ", f10, LGRAY)
    Y += 28

    LBL_W = 78
    VAL_W = 66
    TF_W  = 96
    BAR_X = CX + LBL_W + 12
    BAR_W = CW - LBL_W - 12 - 12 - VAL_W - 12 - TF_W
    ROW_H = 48
    BAR_H = 12   # bar height in logical px

    for lbl, val, tf in [
        ("RSI-6",  r6,  "скальпинг"),
        ("RSI-12", r12, "интрадей"),
        ("RSI-24", r24, "свинг"),
    ]:
        rc = rsi_col(val)
        by = Y + (ROW_H - BAR_H) // 2

        text(drw, CX, Y + 13, lbl, f11, LGRAY)

        # Track background
        rnd(drw, BAR_X, by, BAR_X + BAR_W, by + BAR_H, BG_BLK2, BAR_H // 2)

        # Danger zones (tinted)
        z30 = BAR_X + int(0.30 * BAR_W)
        z70 = BAR_X + int(0.70 * BAR_W)
        rnd(drw, BAR_X, by, z30, by + BAR_H, tint(GREEN, 0.22), BAR_H // 2)
        rnd(drw, z70, by, BAR_X + BAR_W, by + BAR_H, tint(RED, 0.22), BAR_H // 2)

        # Colored fill
        fill_w = max(int(min(max(val, 0), 100) / 100 * BAR_W), BAR_H)
        rnd(drw, BAR_X, by, BAR_X + fill_w, by + BAR_H, rc, BAR_H // 2)

        # Zone tick marks at 30 and 70
        line(drw, BAR_X + int(0.30 * BAR_W), by - 3,
                  BAR_X + int(0.30 * BAR_W), by + BAR_H + 3, DGRAY, 1)
        line(drw, BAR_X + int(0.70 * BAR_W), by - 3,
                  BAR_X + int(0.70 * BAR_W), by + BAR_H + 3, DGRAY, 1)

        # Value
        val_s = str(val)
        vx = BAR_X + BAR_W + 14
        text(drw, vx, Y + 9, val_s, fb14, rc)
        vw = tw(drw, val_s, fb14) // S
        text(drw, vx + vw + 10, Y + 14, tf, f9, DGRAY)

        Y += ROW_H

    # RSI interpretation box
    if r6 < 35 and r12 < 35 and r24 < 35:
        imsg, ic = "Все RSI ниже 35 — сильный сигнал входа", GREEN
    elif r6 > 65 and r12 > 65 and r24 > 65:
        imsg, ic = "Все RSI выше 65 — рынок перегрет, осторожно", RED
    elif abs(r6 - r24) > 18:
        imsg, ic = f"RSI расходятся ({r6} vs {r24}) — переходная зона", AMBER
    else:
        imsg, ic = "RSI согласованы — тренд стабилен", LGRAY

    Y += 6
    rnd(drw, CX, Y, CX + CW, Y + 52, BG_BLOK, 12)
    rect(drw, CX, Y + 10, CX + 4, Y + 42, ic)
    text(drw, CX + 18, Y + 15, imsg, f11, ic)

    Y += 66

    # ── INDICATORS ───────────────────────────────────────────
    IND_W = (CW - 28) // 3
    IND_H = 144

    # MACD: direction label + momentum + mini histogram bars
    macd_c = GREEN if macd_v > 0 else RED
    if len(macd_hist) >= 2:
        growing = macd_hist[-1] > macd_hist[-2]
    else:
        growing = macd_v > 0
    if macd_v > 0:
        macd_dir = "Бычий"
        macd_mom = "усиливается" if growing else "ослабевает"
    else:
        macd_dir = "Медвежий"
        macd_mom = "усиливается" if not growing else "ослабевает"

    if fr is not None:
        fr_s = f"{fr:+.4f}%"
        fr_c = RED if fr > 0.1 else AMBER if fr > 0.05 else LGRAY if fr > -0.02 else GREEN
        fr_l = coin.get("funding_interp", "")[:18].lower()
    else:
        fr_s, fr_c, fr_l = "N/A", DGRAY, "нет данных"

    bb_c = GREEN if bb_pct <= 25 else RED if bb_pct >= 75 else AMBER
    bb_s = f"{bb_pct}%"
    bb_l = bb_label(bb_pct)

    # Draw FUNDING and BOLLINGER cards (standard layout)
    for idx, (lbl, val_s, sub, col) in enumerate([
        ("FUNDING",   fr_s, fr_l, fr_c),
        ("BOLLINGER", bb_s, bb_l, bb_c),
    ]):
        ix = CX + (idx + 1) * (IND_W + 14)
        rnd(drw, ix, Y, ix + IND_W, Y + IND_H, BG_BLOK, 14)
        rnd(drw, ix, Y, ix + IND_W, Y + 3, col, 2)
        text(drw, ix + 16, Y + 14, lbl,   f9,   LGRAY)
        text(drw, ix + 16, Y + 36, val_s, fb18, col)
        text(drw, ix + 16, Y + 108, sub,  f10,  DGRAY)

    # Draw MACD card: number | direction + momentum | histogram
    # Layout (logical px from card top):
    #  +13 : "MACD" label
    #  +32 : number  (+420.5)          fb14 ~30px tall → ends at ~+62
    #  +70 : "Бычий  усиливается"      f9   ~18px tall → ends at ~+88
    #  +94…+130 : mini histogram bars
    mx       = CX
    macd_num = f"+{macd_v}" if macd_v >= 0 else str(macd_v)
    macd_sub = f"{macd_dir}  {macd_mom}"
    rnd(drw, mx, Y, mx + IND_W, Y + IND_H, BG_BLOK, 14)
    rnd(drw, mx, Y, mx + IND_W, Y + 3, macd_c, 2)
    text(drw, mx + 16, Y + 13, "MACD",    f9,   LGRAY)
    text(drw, mx + 16, Y + 32, macd_num,  fb14, macd_c)
    text(drw, mx + 16, Y + 70, macd_sub,  f9,   macd_c)

    # Mini histogram bars
    if macd_hist:
        bars_n       = len(macd_hist)
        bar_w        = 7
        bar_gap      = 3
        bars_total_w = bars_n * (bar_w + bar_gap) - bar_gap
        bx_start     = mx + 16
        max_abs      = max(abs(v) for v in macd_hist) or 1
        bar_max_h    = 24
        base_y       = Y + IND_H - 10

        for bi, hv in enumerate(macd_hist):
            bx  = bx_start + bi * (bar_w + bar_gap)
            bh  = max(int(abs(hv) / max_abs * bar_max_h), 2)
            col = GREEN if hv >= 0 else RED
            rnd(drw, bx, base_y - bh, bx + bar_w, base_y, col, 2)
        line(drw, bx_start - 2, base_y, bx_start + bars_total_w + 2, base_y, DGRAY, 1)

    Y += IND_H + 18

    # ── GLOBAL MARKET ────────────────────────────────────────
    GM_H = 120
    rnd(drw, CX, Y, CX + CW, Y + GM_H, BG_BLOK, 14)

    gm_cols = []
    if fg.get("ok"):
        fv = fg["value"]
        fc = RED if fv <= 25 else AMBER if fv <= 45 else LGRAY if fv <= 55 else GREEN
        gm_cols.append(("FEAR & GREED", f"{fv}", fg.get("label", "")[:14], fc))
    else:
        gm_cols.append(("FEAR & GREED", "N/A", "", DGRAY))

    if dom.get("ok"):
        gm_cols.append(("BTC DOM",    f"{dom['dom']}%",        dom["sig"][:20], BLUE))
        gm_cols.append(("КАП. РЫНКА", f"${dom['mcap']:.0f}B", "",              LGRAY))
    else:
        gm_cols += [("BTC DOM", "N/A", "", DGRAY), ("КАП. РЫНКА", "N/A", "", DGRAY)]

    col_w = CW // len(gm_cols)
    for idx, (lbl, val_s, sub, col) in enumerate(gm_cols):
        gx = CX + idx * col_w
        if idx > 0:
            line(drw, gx, Y + 22, gx, Y + GM_H - 22, LINE, 1)
        text(drw, gx + 22, Y + 14, lbl,   f9,   LGRAY)
        text(drw, gx + 22, Y + 38, val_s, fb18, col)
        if sub:
            text(drw, gx + 22, Y + 88, sub, f10, DGRAY)

    Y += GM_H + 16

    # ── ON-CHAIN METRICS ─────────────────────────────────────
    OC_H  = 130
    nupl  = gdata.get("nupl",  {})
    puell = gdata.get("puell", {})

    rnd(drw, CX, Y, CX + CW, Y + OC_H, BG_BLOK, 14)
    text(drw, CX + 22, Y + 12, "ON-CHAIN МЕТРИКИ", f9, LGRAY)

    oc_items = []
    if nupl.get("ok"):
        nv  = nupl["value"]
        nc  = RED if nv > 0.75 else AMBER if nv > 0.5 else LGRAY if nv > 0.25 else GREEN
        oc_items.append(("NUPL", f"{nv:.3f}", nupl.get("zone", "")[:22], nc))
    else:
        oc_items.append(("NUPL", "N/A", "нет данных", DGRAY))

    if puell.get("ok"):
        pv  = puell["value"]
        pc  = RED if pv > 2.5 else AMBER if pv > 1.5 else LGRAY if pv > 0.8 else GREEN
        oc_items.append(("PUELL MULTIPLE", f"{pv:.2f}x", puell.get("zone", "")[:22], pc))
    else:
        oc_items.append(("PUELL MULTIPLE", "N/A", "нет данных", DGRAY))

    oc_col_w = CW // len(oc_items)
    for idx, (lbl, val_s, sub, col) in enumerate(oc_items):
        ox = CX + idx * oc_col_w
        if idx > 0:
            line(drw, ox, Y + 28, ox, Y + OC_H - 18, LINE, 1)
        text(drw, ox + 22, Y + 30, lbl,   f9,   LGRAY)
        text(drw, ox + 22, Y + 52, val_s, fb18, col)
        if sub:
            text(drw, ox + 22, Y + 102, sub, f10, DGRAY)

    Y += OC_H + 16

    # ── FOOTER ───────────────────────────────────────────────
    line(drw, CX, Y, CX + CW, Y, LINE, 1)
    Y += 8
    text(drw, CX, Y, "TY SMITH SIGNALS", f9, DGRAY)
    ts = f"{gdata.get('time', '')} МСК"
    ts_w = tw(drw, ts, f9) // S
    text(drw, CX + CW - ts_w, Y, ts, f9, DGRAY)

    # ── Downscale 2× → 1× for crisp output ──────────────────
    img = img.resize((W, H), Image.LANCZOS)

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
            import traceback
            print(f"Card error {coin.get('symbol','?')}: {e}", flush=True)
            traceback.print_exc()
    return cards
