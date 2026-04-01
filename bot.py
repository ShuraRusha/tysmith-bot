import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from functools import wraps

import pytz
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from web3 import Web3
from web3.middleware import geth_poa_middleware

import config
from analyzer import check_token, get_bnb_price
from position import Position, PositionManager
from trader import Trader
from watcher import watch_pairs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

MOSCOW_TZ     = pytz.timezone("Europe/Moscow")
TRADE_LOG_FILE = "/tmp/tysmith_trades.json"


# ── Web3 setup ────────────────────────────────────────────────────────────────

def _make_w3(url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(url))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3

w3 = _make_w3(config.BSC_HTTP_RPC)
if not w3.is_connected():
    log.warning("Primary RPC unavailable — switching to backup")
    w3 = _make_w3(config.BSC_HTTP_RPC_BACKUP)

trader = Trader(w3, config.PRIVATE_KEY, config.GAS_MULTIPLIER)

# callback_id (first 10 chars of token address) → token info
# cleaned up after user action (buy or skip)
pending: dict[str, dict] = {}

# ── Bot state ─────────────────────────────────────────────────────────────────

is_paused: bool = False
trade_history: list[dict] = []


def _load_history():
    global trade_history
    try:
        if os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE) as f:
                trade_history = json.load(f)
            log.info(f"Loaded {len(trade_history)} trades from history")
    except Exception as e:
        log.warning(f"Could not load trade history: {e}")
        trade_history = []


def _save_history():
    try:
        with open(TRADE_LOG_FILE, "w") as f:
            json.dump(trade_history[-500:], f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Failed to save trade history: {e}")


def _record_trade(pos: Position, pnl_pct: float, reason: str, sell_price: float = 0.0):
    pnl_bnb = round(pos.buy_bnb * pnl_pct / 100, 6)
    trade_history.append({
        "symbol":         pos.symbol,
        "token_address":  pos.token_address,
        "buy_price_bnb":  pos.buy_price_bnb,
        "sell_price_bnb": sell_price,
        "buy_bnb":        pos.buy_bnb,
        "pnl_pct":        round(pnl_pct, 2),
        "pnl_bnb":        pnl_bnb,
        "reason":         reason,
        "closed_at":      datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M"),
    })
    _save_history()


# ── Telegram helper ───────────────────────────────────────────────────────────

async def tg_send(text: str, reply_markup=None):
    bot = Bot(token=config.BOT_TOKEN)
    await bot.send_message(
        chat_id=config.CHAT_ID,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )

pos_manager = PositionManager(trader, tg_send)
pos_manager.on_close = _record_trade


# ── Owner-only guard ──────────────────────────────────────────────────────────

def owner_only(fn):
    @wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if str(update.effective_chat.id) != str(config.CHAT_ID):
            return
        return await fn(update, context)
    return wrapper


# ── New pair handler ──────────────────────────────────────────────────────────

async def on_pair_found(token_address: str, base_token: str, pair_address: str):
    if is_paused:
        log.info(f"Bot paused — skipping {token_address}")
        return

    log.info(f"Analyzing: {token_address}")

    result = await check_token(
        token_address, pair_address, base_token, w3,
        config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
    )

    if not result["ok"]:
        log.info(f"Rejected {token_address}: {result['reason']}")
        return

    info      = result["info"]
    bnb_price = info["bnb_price"]

    warnings = []
    if info["is_mintable"]:   warnings.append("⚠️ Mintable — могут допечатать токены")
    if info["hidden_owner"]:  warnings.append("⚠️ Hidden owner")
    if info["is_proxy"]:      warnings.append("⚠️ Proxy контракт")
    if info["external_call"]: warnings.append("⚠️ External call в коде")
    warn_block = "\n".join(warnings) if warnings else "✅ Дополнительных угроз нет"

    text = (
        f"🎯 *Новый токен прошёл все проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💧 Ликвидность: *${info['liquidity_usd']:,.0f}*\n"
        f"💸 Buy tax: *{info['buy_tax']:.1f}%*  |  Sell tax: *{info['sell_tax']:.1f}%*\n"
        f"👥 Холдеры: {info['holder_count']}\n\n"
        f"{warn_block}\n\n"
        f"📊 TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции  "
        f"| Trailing: -{config.TRAILING_STOP_PCT}% от пика  "
        f"| SL: -{config.STOP_LOSS}%\n"
        f"💰 Покупка: *{config.BUY_AMOUNT_BNB} BNB* "
        f"(~${config.BUY_AMOUNT_BNB * bnb_price:.0f})\n"
        f"⏰ {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')} МСК"
    )

    cb_id = token_address[:10]
    pending[cb_id] = {
        "token_address": token_address,
        "base_token":    base_token,
        "pair_address":  pair_address,
        "info":          info,
        "ts":            time.time(),
    }

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ КУПИТЬ",     callback_data=f"buy_{cb_id}"),
        InlineKeyboardButton("❌ Пропустить", callback_data=f"skip_{cb_id}"),
    ]])
    await tg_send(text, reply_markup=keyboard)


# ── Callback handler ──────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    # ── BUY ──────────────────────────────────────────────────────────────────
    if data.startswith("buy_"):
        cb_id      = data[4:]
        token_info = pending.pop(cb_id, None)
        if not token_info:
            await query.edit_message_text("⚠️ Время ожидания истекло — пара уже устарела.")
            return

        sym           = token_info["info"]["symbol"]
        token_address = token_info["token_address"]

        if time.time() - token_info["ts"] > config.PENDING_TTL:
            await query.edit_message_text(
                f"⏰ Время истекло — {sym} уже {config.PENDING_TTL // 60} мин назад.\n"
                f"Цена могла сильно измениться."
            )
            return

        if token_address in pos_manager.positions:
            await query.edit_message_text(f"⚠️ Позиция по {sym} уже открыта.")
            return

        if len(pos_manager.positions) >= config.MAX_POSITIONS:
            await query.edit_message_text(
                f"🚫 Достигнут лимит позиций ({config.MAX_POSITIONS}).\n"
                f"Закрой одну из текущих перед новой покупкой."
            )
            return

        if not trader.has_enough_bnb(config.BUY_AMOUNT_BNB):
            await query.edit_message_text(
                f"💸 Недостаточно BNB для покупки {sym}.\n"
                f"Нужно: {config.BUY_AMOUNT_BNB} BNB + ~0.005 BNB на газ."
            )
            return

        await query.edit_message_text(
            f"⏳ Одобряю и покупаю *{sym}*...", parse_mode=ParseMode.MARKDOWN
        )

        approved = await asyncio.to_thread(trader.approve_token, token_address)
        if not approved:
            await query.edit_message_text(
                f"❌ Не удалось одобрить контракт для {sym}. Пропускаем.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        price_before = await asyncio.to_thread(
            trader.get_price, token_address, token_info["base_token"]
        )

        result = await asyncio.to_thread(
            trader.buy, token_address, config.BUY_AMOUNT_BNB
        )

        if result["ok"]:
            entry_price = price_before if price_before > 0 else (
                config.BUY_AMOUNT_BNB / (result["tokens_received"] / 10 ** result["decimals"])
            )
            pos = Position(
                token_address      = token_address,
                symbol             = sym,
                name               = token_info["info"]["name"],
                pair_address       = token_info["pair_address"],
                buy_price_bnb      = entry_price,
                tokens_amount      = result["tokens_received"],
                decimals           = result["decimals"],
                buy_bnb            = config.BUY_AMOUNT_BNB,
                take_profit_1      = config.TAKE_PROFIT_1,
                take_profit_1_pct  = config.TAKE_PROFIT_1_PCT,
                trailing_stop_pct  = config.TRAILING_STOP_PCT,
                stop_loss          = config.STOP_LOSS,
            )
            pos_manager.add(pos)

            amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
            await query.edit_message_text(
                f"✅ *Куплено!* — {sym}\n\n"
                f"Получено: {amount_fmt:.4f} {sym}\n"
                f"Цена входа: {entry_price:.8f} BNB\n"
                f"Tx: `{result['tx_hash']}`\n\n"
                f"TP1: +{config.TAKE_PROFIT_1}% → продать {config.TAKE_PROFIT_1_PCT:.0f}%\n"
                f"Далее: trailing stop -{config.TRAILING_STOP_PCT}% от пика\n"
                f"SL: -{config.STOP_LOSS}%",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await query.edit_message_text(
                f"❌ *Ошибка покупки* — {sym}\n{result['reason']}",
                parse_mode=ParseMode.MARKDOWN,
            )

    # ── SKIP ─────────────────────────────────────────────────────────────────
    elif data.startswith("skip_"):
        cb_id = data[5:]
        info  = pending.pop(cb_id, {}).get("info", {})
        sym   = info.get("symbol", "токен")
        await query.edit_message_text(f"❌ Пропущено — {sym}")

    # ── SELL (manual from /positions) ─────────────────────────────────────────
    elif data.startswith("sell_"):
        token_address = data[5:]
        pos = pos_manager.positions.get(token_address)
        if not pos:
            await query.edit_message_text("⚠️ Позиция не найдена или уже закрыта.")
            return

        await query.edit_message_text(
            f"⏳ Продаю *{pos.symbol}*...", parse_mode=ParseMode.MARKDOWN
        )
        result = await asyncio.to_thread(
            trader.sell, token_address, pos.tokens_amount
        )

        if result["ok"]:
            sell_price = await asyncio.to_thread(trader.get_price, token_address)
            pnl_pct = (
                (sell_price - pos.buy_price_bnb) / pos.buy_price_bnb * 100
                if pos.buy_price_bnb and sell_price else 0
            )
            _record_trade(pos, pnl_pct, "Manual", sell_price)
            pos_manager.remove(token_address)
            await query.edit_message_text(
                f"✅ *Продано вручную* — {pos.symbol}\n"
                f"P&L: {pnl_pct:+.1f}%\n"
                f"Tx: `{result['tx_hash']}`",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await query.edit_message_text(
                f"❌ Ошибка продажи — {pos.symbol}: {result['reason']}",
                parse_mode=ParseMode.MARKDOWN,
            )


# ── Command handlers ──────────────────────────────────────────────────────────

@owner_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Sniper Bot активен*\n\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC).\n"
        "Каждый токен проходит автоматическую проверку:\n"
        "honeypot · налог · ликвидность · опасные функции\n\n"
        "*/help* — все команды\n"
        "*/status* — баланс и настройки\n"
        "*/positions* — открытые позиции\n"
        "*/stats* — статистика сделок\n"
        "*/history* — последние 10 сделок",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Команды бота*\n\n"
        "*Мониторинг*\n"
        "/status — баланс кошелька и текущие настройки\n"
        "/positions — открытые позиции с P&L\n\n"
        "*Статистика*\n"
        "/stats — общая статистика (win rate, PnL)\n"
        "/history — последние 10 закрытых сделок\n\n"
        "*Управление*\n"
        "/pause — приостановить снайпинг\n"
        "/resume — возобновить снайпинг\n\n"
        "*Настройки* (изменить без перезапуска)\n"
        "`/set buy 0.05` — сумма покупки в BNB\n"
        "`/set sl 20` — стоп-лосс в %\n"
        "`/set tp1 60` — TP1 в %\n"
        "`/set trail 15` — trailing stop в %\n"
        "`/set liq 5000` — мин. ликвидность в USD\n"
        "`/set tax 8` — макс. налог на покупку и продажу\n"
        "`/set max 5` — макс. кол-во позиций",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_paused
    if is_paused:
        await update.message.reply_text("⏸ Бот уже на паузе. Используй /resume для возобновления.")
        return
    is_paused = True
    await update.message.reply_text(
        "⏸ *Снайпинг приостановлен*\n\n"
        "Новые токены не будут отправляться.\n"
        "Открытые позиции продолжают мониториться.\n\n"
        "Используй /resume для возобновления.",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_paused
    if not is_paused:
        await update.message.reply_text("▶️ Бот уже активен.")
        return
    is_paused = False
    await update.message.reply_text(
        "▶️ *Снайпинг возобновлён*\n\n"
        "Слежу за новыми парами на PancakeSwap V2.",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not trade_history:
        await update.message.reply_text("📊 Нет закрытых сделок.")
        return

    total   = len(trade_history)
    wins    = [t for t in trade_history if t["pnl_pct"] > 0]
    losses  = [t for t in trade_history if t["pnl_pct"] <= 0]
    win_rate = len(wins) / total * 100

    total_pnl_bnb = sum(t["pnl_bnb"] for t in trade_history)
    avg_pnl_pct   = sum(t["pnl_pct"] for t in trade_history) / total

    bnb_price = await get_bnb_price(w3)
    total_pnl_usd = total_pnl_bnb * bnb_price

    best  = max(trade_history, key=lambda t: t["pnl_pct"])
    worst = min(trade_history, key=lambda t: t["pnl_pct"])

    await update.message.reply_text(
        f"📊 *Статистика сделок*\n\n"
        f"Всего сделок: *{total}*\n"
        f"Прибыльных: *{len(wins)}*  |  Убыточных: *{len(losses)}*\n"
        f"Win rate: *{win_rate:.1f}%*\n\n"
        f"Общий P&L: *{total_pnl_bnb:+.4f} BNB* (~${total_pnl_usd:+.0f})\n"
        f"Средний P&L: *{avg_pnl_pct:+.1f}%* за сделку\n\n"
        f"Лучшая: *{best['symbol']}* {best['pnl_pct']:+.1f}%\n"
        f"Худшая: *{worst['symbol']}* {worst['pnl_pct']:+.1f}%",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not trade_history:
        await update.message.reply_text("📜 История пустая — нет закрытых сделок.")
        return

    last10 = trade_history[-10:][::-1]
    lines = ["📜 *Последние сделки*\n"]
    for t in last10:
        emoji = "✅" if t["pnl_pct"] > 0 else "🔴"
        lines.append(
            f"{emoji} *{t['symbol']}* {t['pnl_pct']:+.1f}% "
            f"({t['pnl_bnb']:+.4f} BNB) — {t['reason']}\n"
            f"    `{t['closed_at']}`"
        )
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            "❌ Формат: `/set <параметр> <значение>`\n\n"
            "Параметры: `buy`, `sl`, `tp1`, `trail`, `liq`, `tax`, `max`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    param, raw = args[0].lower(), args[1]
    try:
        value = float(raw)
    except ValueError:
        await update.message.reply_text("❌ Значение должно быть числом.")
        return

    PARAMS = {
        "buy":   ("BUY_AMOUNT_BNB",    0.001, 10.0,   "BNB на покупку"),
        "sl":    ("STOP_LOSS",          1.0,   90.0,   "Стоп-лосс %"),
        "tp1":   ("TAKE_PROFIT_1",      5.0,   500.0,  "TP1 %"),
        "trail": ("TRAILING_STOP_PCT",  1.0,   90.0,   "Trailing stop %"),
        "liq":   ("MIN_LIQUIDITY_USD",  500.0, 1e7,    "Мин. ликвидность USD"),
        "tax":   ("MAX_BUY_TAX",        1.0,   50.0,   "Макс. налог %"),
        "max":   ("MAX_POSITIONS",      1,     20,     "Макс. позиций"),
    }

    if param not in PARAMS:
        await update.message.reply_text(
            f"❌ Неизвестный параметр `{param}`.\n"
            f"Доступные: `{', '.join(PARAMS.keys())}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    attr, min_val, max_val, label = PARAMS[param]
    if not (min_val <= value <= max_val):
        await update.message.reply_text(
            f"❌ Значение вне диапазона: {min_val} — {max_val}"
        )
        return

    old_value = getattr(config, attr)
    # For MAX_POSITIONS store as int
    if attr == "MAX_POSITIONS":
        value = int(value)
    setattr(config, attr, value)

    # tax changes both buy and sell
    if param == "tax":
        setattr(config, "MAX_SELL_TAX", value)

    await update.message.reply_text(
        f"✅ *{label}* обновлён\n"
        f"{old_value} → *{value}*",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    positions = pos_manager.get_all()
    if not positions:
        await update.message.reply_text("Нет открытых позиций.")
        return

    for pos in positions:
        price   = await asyncio.to_thread(trader.get_price, pos.token_address)
        pnl_pct = (
            (price - pos.buy_price_bnb) / pos.buy_price_bnb * 100
            if pos.buy_price_bnb and price else 0
        )
        if pos.tp1_done:
            phase = f"Trailing stop: -{pos.trailing_stop_pct}% от пика ({pos.peak_price:.8f})"
        else:
            phase = f"TP1: +{pos.take_profit_1}%  |  SL: -{pos.stop_loss}%"
        text = (
            f"*{pos.name}* ({pos.symbol})\n"
            f"Вход: {pos.buy_price_bnb:.8f} BNB\n"
            f"Сейчас: {price:.8f} BNB\n"
            f"P&L: {pnl_pct:+.1f}%\n"
            f"{phase}"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                f"🔴 Продать {pos.symbol}",
                callback_data=f"sell_{pos.token_address}",
            )
        ]])
        await update.message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard
        )


@owner_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    connected = w3.is_connected()
    balance   = w3.eth.get_balance(trader.wallet) / 1e18
    bnb_price = await get_bnb_price(w3)
    status_icon = "⏸ ПАУЗА" if is_paused else "▶️ активен"
    await update.message.reply_text(
        f"*Статус Sniper Bot* — {status_icon}\n\n"
        f"RPC: {'✅ подключён' if connected else '❌ нет соединения'}\n"
        f"Кошелёк: `{trader.wallet}`\n"
        f"Баланс: *{balance:.4f} BNB* (~${balance * bnb_price:.0f})\n"
        f"Позиций открыто: {len(pos_manager.positions)}\n"
        f"Сделок в истории: {len(trade_history)}\n\n"
        f"*Настройки:*\n"
        f"Buy: {config.BUY_AMOUNT_BNB} BNB  |  Slip buy/sell: {config.SLIPPAGE_BUY}%/{config.SLIPPAGE_SELL}%\n"
        f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции\n"
        f"Trailing stop: -{config.TRAILING_STOP_PCT}% от пика  |  SL: -{config.STOP_LOSS}%\n"
        f"Min ликвидность: ${config.MIN_LIQUIDITY_USD:,.0f}\n"
        f"Max tax: {config.MAX_BUY_TAX}% buy / {config.MAX_SELL_TAX}% sell",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Background: remove expired pending alerts ────────────────────────────────

async def _cleanup_pending():
    """Remove pending tokens older than PENDING_TTL every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        now     = time.time()
        expired = [k for k, v in pending.items() if now - v["ts"] > config.PENDING_TTL]
        for k in expired:
            pending.pop(k, None)
        if expired:
            log.info(f"Cleaned up {len(expired)} expired pending token(s)")


# ── Entry point ───────────────────────────────────────────────────────────────

PID_FILE = "/tmp/tysmith-bot.pid"

def _acquire_pid_lock():
    """Exit if another instance is already running."""
    if os.path.exists(PID_FILE):
        try:
            old_pid = int(open(PID_FILE).read().strip())
            os.kill(old_pid, 0)
            log.error(f"Бот уже запущен (PID {old_pid}). Выход.")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

def _release_pid_lock():
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass


async def main():
    _load_history()
    log.info("Sniper Bot starting...")

    app = Application.builder().token(config.BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("pause",     cmd_pause))
    app.add_handler(CommandHandler("resume",    cmd_resume))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("history",   cmd_history))
    app.add_handler(CommandHandler("set",       cmd_set))
    app.add_handler(CallbackQueryHandler(handle_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    asyncio.create_task(pos_manager.monitor())
    asyncio.create_task(watch_pairs(config.BSC_WS_RPC, on_pair_found))
    asyncio.create_task(_cleanup_pending())

    log.info(f"Ready. Wallet: {trader.wallet}")
    await tg_send(
        "🚀 *Sniper Bot запущен*\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC)...\n\n"
        "/help — все команды"
    )

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        _release_pid_lock()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    _acquire_pid_lock()
    asyncio.run(main())
