from PIL import Image, ImageDraw, ImageFont
import io, os, asyncio, logging, pytz, aiohttp
from datetime import datetime
from telegram import Bot

log = logging.getLogger(__name__)

BG        = "#0d0d0d"
CARD_BG   = "#1a1a1a"
CARD2_BG  = "#141414"
WHITE     = "#ffffff"
GRAY      = "#888888"
LGRAY     = "#555555"
GREEN     = "#1d9e75"
RED       = "#e24b4a"
AMBER     = "#f59e0b"
BLUE      = "#3b82f6"

W, H = 900, 1080

def load_font(size, bold=False):
    paths = [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        f"/usr/share/fonts/truetype/liberation/LiberationSans{'-Bold' if bold else '-Regular'}.ttf",
        f"/System/Library/Fonts/Helvetica.ttc",
        f"/System/Library/Fonts/Arial.ttf",
    ]
    for p in paths:
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()

def rsi_color(v):
    if v > 70: return RED
    if v < 30: return GREEN
    if v < 40 or v > 60: return AMBER
    return GRAY

def signal_color(action):
    if "ПОКУПАТЬ" in action:    return GREEN
    if "НАКАПЛИВАТЬ" in action: return BLUE
    if "ПРОДАВАТЬ" in action:   return RED
    if "ОСТОРОЖНО" in action:   return AMBER
    return GRAY

def fg_color(v):
    if v <= 25: return RED
    if v <= 45: return AMBER
    if v <= 55: return GRAY
    if v <= 75: return GREEN
    return GREEN

def draw_rounded_rect(draw, xy, radius, fill):
    x1,y1,x2,y2 = xy
    draw.rectangle([x1+radius,y1,x2-radius,y2], fill=fill)
    draw.rectangle([x1,y1+radius,x2,y2-radius], fill=fill)
    draw.ellipse([x1,y1,x1+2*radius,y1+2*radius], fill=fill)
    draw.ellipse([x2-2*radius,y1,x2,y1+2*radius], fill=fill)
    draw.ellipse([x1,y2-2*radius,x1+2*radius,y2], fill=fill)
    draw.ellipse([x2-2*radius,y2-2*radius,x2,y2], fill=fill)

def draw_bar(draw, x, y, w, h, pct, color, bg="#2a2a2a"):
    draw_rounded_rect(draw, [x,y,x+w,y+h], h//2, bg)
    if pct > 0:
        fw = max(int(w * min(pct,1.0)), h)
        draw_rounded_rect(draw, [x,y,x+fw,y+h], h//2, color)

def generate_dashboard(data: dict) -> bytes:
    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    f32b = load_font(32, bold=True)
    f28b = load_font(28, bold=True)
    f24b = load_font(24, bold=True)
    f22  = load_font(22)
    f20b = load_font(20, bold=True)
    f18  = load_font(18)
    f16  = load_font(16)
    f14  = load_font(14)

    # ── HEADER ────────────────────────────────────────────────────────────────
    draw.text((40, 28), "TY SMITH", font=f32b, fill=WHITE)
    draw.text((40, 66), "SIGNAL REPORT", font=f22, fill=GRAY)
    now = data.get("time", "")
    draw.text((W-40, 28), now, font=f18, fill=GRAY, anchor="ra")
    draw.text((W-40, 54), "МСК", font=f14, fill=LGRAY, anchor="ra")
    draw.line([(40, 100), (W-40, 100)], fill="#2a2a2a", width=1)

    # ── GLOBAL BLOCK ──────────────────────────────────────────────────────────
    draw_rounded_rect(draw, [40, 112, W-40, 210], 10, CARD_BG)

    fg    = data.get("fg", {})
    dom   = data.get("dom", {})
    fg_v  = fg.get("value", 50)
    fg_l  = fg.get("label", "Neutral")
    fg_d  = fg.get("delta", "0")
    dom_v = dom.get("dom", 50.0)
    dom_s = dom.get("sig", "")

    draw.text((60, 125), "FEAR & GREED", font=f14, fill=GRAY)
    draw.text((60, 145), str(fg_v), font=f28b, fill=fg_color(fg_v))
    draw.text((60+60, 155), f"/100  {fg_l}  Δ{fg_d}", font=f16, fill=GRAY)
    draw_bar(draw, 60, 182, 340, 8, fg_v/100, fg_color(fg_v))

    draw.line([(W//2, 118), (W//2, 202)], fill="#2a2a2a", width=1)

    draw.text((W//2+20, 125), "BTC DOMINANCE", font=f14, fill=GRAY)
    draw.text((W//2+20, 145), f"{dom_v}%", font=f28b, fill=BLUE)
    short_sig = dom_s.split("—")[-1].strip() if "—" in dom_s else dom_s
    draw.text((W//2+20, 178), short_sig[:32], font=f14, fill=GRAY)
    draw_bar(draw, W//2+20, 195, 340, 8, dom_v/100, BLUE)

    # ── COIN CARDS ────────────────────────────────────────────────────────────
    coins = data.get("coins", [])
    cols, rows = 2, 2
    cw = (W - 40 - 40 - 16) // 2
    ch = 190
    start_y = 226

    for i, coin in enumerate(coins[:4]):
        col = i % cols
        row = i // cols
        cx = 40 + col * (cw + 16)
        cy = start_y + row * (ch + 14)

        sig_col = signal_color(coin.get("action",""))
        draw_rounded_rect(draw, [cx, cy, cx+cw, cy+ch], 10, CARD2_BG)
        draw.rectangle([cx, cy, cx+4, cy+ch], fill=sig_col)

        # Symbol + price
        draw.text((cx+18, cy+14), coin["symbol"], font=f24b, fill=WHITE)
        chg = coin.get("change", 0)
        chg_col = GREEN if chg >= 0 else RED
        chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
        draw.text((cx+18, cy+44), f"${coin['price']:,.0f}", font=f20b, fill=WHITE)
        draw.text((cx+cw-16, cy+48), chg_str, font=f16, fill=chg_col, anchor="ra")

        draw.line([(cx+14, cy+72), (cx+cw-14, cy+72)], fill="#2a2a2a", width=1)

        # RSI bars
        rsi_y = cy + 82
        for label, val in [("RSI-6", coin.get("rsi6",50)), ("RSI-12", coin.get("rsi12",50)), ("RSI-24", coin.get("rsi24",50))]:
            rc = rsi_color(val)
            draw.text((cx+18, rsi_y), label, font=f14, fill=GRAY)
            draw.text((cx+80, rsi_y), str(val), font=f14, fill=rc)
            draw_bar(draw, cx+115, rsi_y+2, cw-145, 10, val/100, rc)
            rsi_y += 22

        draw.line([(cx+14, cy+154), (cx+cw-14, cy+154)], fill="#2a2a2a", width=1)

        # Funding + Signal
        fr = coin.get("funding_rate", None)
        fr_src = coin.get("funding_src", "")
        if fr is not None:
            fr_col = RED if fr>0.1 else AMBER if fr>0.05 else GREEN
            draw.text((cx+18, cy+160), f"Funding ({fr_src}): {fr:+.4f}%", font=f14, fill=fr_col)
        else:
            draw.text((cx+18, cy+160), "Funding: недоступен", font=f14, fill=LGRAY)

        # Signal badge
        action = coin.get("action", "НЕЙТРАЛЬНО")
        short_action = action.split(" ",1)[1] if " " in action else action
        badge_w = len(short_action)*9 + 20
        bx = cx + cw - badge_w - 14
        draw_rounded_rect(draw, [bx, cy+14, bx+badge_w, cy+36], 8, sig_col+"33")
        draw.text((bx+badge_w//2, cy+25), short_action, font=f14, fill=sig_col, anchor="mm")

        # Target / Stop
        t_str = f"🎯 ${coin['target']:,.0f}"
        s_str = f"🛡 ${coin['stop']:,.0f}"
        draw.text((cx+18, cy+ch-22), t_str, font=f14, fill=GREEN)
        draw.text((cx+cw//2, cy+ch-22), s_str, font=f14, fill=RED)

    # ── FOOTER ────────────────────────────────────────────────────────────────
    fy = start_y + rows*(ch+14) + 10
    draw.line([(40, fy), (W-40, fy)], fill="#2a2a2a", width=1)
    next_h = data.get("next_hour", "")
    draw.text((40, fy+12), f"Следующий отчёт: {next_h} МСК", font=f16, fill=GRAY)
    draw.text((W-40, fy+12), "Не является финансовой рекомендацией", font=f14, fill=LGRAY, anchor="ra")

    buf = io.BytesIO()
    img.save(buf, format="PNG", quality=95)
    buf.seek(0)
    return buf.read()
