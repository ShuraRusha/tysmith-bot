import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime

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
from analyzer import check_token_security, get_bnb_price
from execution_engine import Candidate, ExecutionEngine
from position import Position, PositionManager
from trader import Trader
from watcher import watch_pairs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

MOSCOW_TZ = pytz.timezone("Europe/Moscow")


# ── Web3 setup ────────────────────────────────────────────────────────────────

def _make_w3(url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(url))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3

w3 = _make_w3(config.BSC_HTTP_RPC)
if not w3.is_connected():
    log.warning("Primary RPC unavailable — switching to backup")
    w3 = _make_w3(config.BSC_HTTP_RPC_BACKUP)

trader = Trader(w3, config.PRIVATE_KEY, config.SLIPPAGE, config.GAS_MULTIPLIER)

# cb_id (first 10 chars of token address) → pending token info
# TTL-controlled; user must click BUY before PENDING_TTL seconds
pending: dict[str, dict] = {}


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

pos_manager    = PositionManager(trader, tg_send)
execution_engine: ExecutionEngine   # initialized in main()


# ── Discovery: new pair handler ───────────────────────────────────────────────

async def on_pair_found(token_address: str, base_token: str, pair_address: str):
    """
    Called by watcher for every PairCreated event.

    Responsibility: security screening only.
    Buying happens later — ExecutionEngine waits for liquidity first.
    """
    log.info(f"Analyzing security: {token_address}")

    result = await check_token_security(
        token_address,
        config.MAX_BUY_TAX,
        config.MAX_SELL_TAX,
        w3=w3,
        pair_address=pair_address,
    )

    if not result["ok"]:
        log.info(f"Rejected {token_address}: {result['reason']}")
        return

    info      = result["info"]
    bnb_price = await get_bnb_price(w3)

    warnings = []
    if info["is_mintable"]:   warnings.append("⚠️ Mintable — могут допечатать токены")
    if info["hidden_owner"]:  warnings.append("⚠️ Hidden owner")
    if info["is_proxy"]:      warnings.append("⚠️ Proxy контракт")
    if info["external_call"]: warnings.append("⚠️ External call в коде")
    warn_block = "\n".join(warnings) if warnings else "✅ Дополнительных угроз нет"

    creation_block = await asyncio.to_thread(lambda: w3.eth.block_number)
    cb_id = token_address[:10]

    text = (
        f"🎯 *Новый токен прошёл проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💸 Buy tax: *{info['buy_tax']:.1f}%*  |  Sell tax: *{info['sell_tax']:.1f}%*\n"
        f"👥 Холдеры: {info['holder_count']}\n\n"
        f"{warn_block}\n\n"
        f"📊 TP1: +{config.TAKE_PROFIT_1}% ({config.TAKE_PROFIT_1_PCT:.0f}% позиции)  "
        f"TP2: +{config.TAKE_PROFIT_2}%  |  SL: -{config.STOP_LOSS}%\n"
        f"💰 Покупка: *{config.BUY_AMOUNT_BNB} BNB* "
        f"(~${config.BUY_AMOUNT_BNB * bnb_price:.0f})\n"
        f"⏳ Нажмите КУПИТЬ — войду в первый блок ликвидности\n"
        f"⏰ {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')} МСК"
    )

    pending[cb_id] = {
        "token_address":  token_address,
        "base_token":     base_token,
        "pair_address":   pair_address,
        "info":           info,
        "creation_block": creation_block,
        "ts":             time.time(),
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

    # ── BUY → hand off to ExecutionEngine ────────────────────────────────────
    if data.startswith("buy_"):
        cb_id      = data[4:]
        token_info = pending.pop(cb_id, None)
        if not token_info:
            await query.edit_message_text("⚠️ Время ожидания истекло — пара уже устарела.")
            return

        if time.time() - token_info["ts"] > config.PENDING_TTL:
            sym = token_info["info"]["symbol"]
            await query.edit_message_text(
                f"⏰ Время истекло — {sym} уже {config.PENDING_TTL // 60} мин назад.\n"
                f"Цена могла сильно измениться."
            )
            return

        sym  = token_info["info"]["symbol"]
        addr = token_info["token_address"]

        if addr in pos_manager.positions:
            await query.edit_message_text(f"⚠️ Позиция по {sym} уже открыта.")
            return

        if len(pos_manager.positions) >= config.MAX_POSITIONS:
            await query.edit_message_text(
                f"🚫 Достигнут лимит позиций ({config.MAX_POSITIONS})."
            )
            return

        if not trader.has_enough_bnb(config.BUY_AMOUNT_BNB):
            await query.edit_message_text(
                f"💸 Недостаточно BNB для покупки {sym}.\n"
                f"Нужно: {config.BUY_AMOUNT_BNB} BNB + ~0.005 BNB на газ."
            )
            return

        candidate = Candidate(
            token_address  = addr,
            base_token     = token_info["base_token"],
            pair_address   = token_info["pair_address"],
            info           = token_info["info"],
            creation_block = token_info["creation_block"],
        )
        await execution_engine.enqueue(candidate)

        await query.edit_message_text(
            f"⏳ *{sym}* — жду ликвидность ≥ ${config.MIN_LIQUIDITY_USD:,.0f}...\n"
            f"Куплю автоматически как только появится.",
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
            price   = await asyncio.to_thread(trader.get_price, token_address)
            pnl_pct = (
                (price - pos.buy_price_bnb) / pos.buy_price_bnb * 100
                if pos.buy_price_bnb and price else 0
            )
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

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Sniper Bot активен*\n\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC).\n\n"
        "Каждый токен проходит проверку:\n"
        "honeypot · налог · опасные функции\n\n"
        "При появлении чистого токена — пришлю уведомление.\n"
        "Нажмите КУПИТЬ — и бот войдёт автоматически в первый блок ликвидности.\n\n"
        "*/positions* — открытые позиции\n"
        "*/status* — состояние бота и баланс кошелька",
        parse_mode=ParseMode.MARKDOWN,
    )


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
        tp1_status = "✅" if pos.tp1_done else f"+{pos.take_profit_1}%"
        text = (
            f"*{pos.name}* ({pos.symbol})\n"
            f"Вход: {pos.buy_price_bnb:.8f} BNB\n"
            f"Сейчас: {price:.8f} BNB\n"
            f"P&L: {pnl_pct:+.1f}%\n"
            f"TP1: {tp1_status}  TP2: +{pos.take_profit_2}%  SL: -{pos.stop_loss}%"
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


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    connected = await asyncio.to_thread(w3.is_connected)
    bnb_price = await get_bnb_price(w3)

    mode_line = "🎭 *DEMO режим* — реальные деньги не тратятся\n" if config.DEMO_MODE else ""

    if config.DEMO_MODE:
        balance_line = f"Виртуальный баланс: {config.BUY_AMOUNT_BNB} BNB/сделка\n"
    else:
        balance = await asyncio.to_thread(lambda: w3.eth.get_balance(trader.wallet) / 1e18)
        balance_line = f"Баланс: {balance:.4f} BNB (~${balance * bnb_price:.0f})\n"

    await update.message.reply_text(
        f"{mode_line}"
        f"Статус Sniper Bot\n\n"
        f"RPC: {'✅ подключён' if connected else '❌ нет соединения'}\n"
        f"Кошелёк: {trader.wallet}\n"
        f"{balance_line}"
        f"Позиций открыто: {len(pos_manager.positions)}\n\n"
        f"Настройки:\n"
        f"Buy: {config.BUY_AMOUNT_BNB} BNB | Slippage: {config.SLIPPAGE}%\n"
        f"TP1: +{config.TAKE_PROFIT_1}% -> {config.TAKE_PROFIT_1_PCT:.0f}% позиции\n"
        f"TP2: +{config.TAKE_PROFIT_2}% -> остаток | SL: -{config.STOP_LOSS}%\n"
        f"Min ликвидность: ${config.MIN_LIQUIDITY_USD:,.0f}\n"
        f"Max tax: {config.MAX_BUY_TAX}% buy / {config.MAX_SELL_TAX}% sell",
    )


# ── Background: clean up expired pending alerts ───────────────────────────────

async def _cleanup_pending():
    while True:
        await asyncio.sleep(60)
        now     = time.time()
        expired = [k for k, v in pending.items() if now - v["ts"] > config.PENDING_TTL]
        for k in expired:
            pending.pop(k, None)
        if expired:
            log.info(f"Cleaned up {len(expired)} expired pending token(s)")


# ── PID lock ──────────────────────────────────────────────────────────────────

PID_FILE = "/tmp/tysmith-bot.pid"

def _acquire_pid_lock():
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


async def cmd_demostats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args and context.args[0].lower() == "reset":
        pos_manager.reset_demo_stats()
        await update.message.reply_text("🎭 Demo статистика сброшена.")
        return

    stats = pos_manager.get_demo_stats()
    if stats["total"] == 0:
        await update.message.reply_text(
            "🎭 *Demo статистика*\n\nСделок пока нет.",
            parse_mode="Markdown",
        )
        return

    bnb_price = await get_bnb_price(w3)
    pnl_usd   = stats["total_pnl_bnb"] * bnb_price

    open_demo = [p for p in pos_manager.get_all() if p.demo]
    open_line = f"\nОткрытых demo позиций: *{len(open_demo)}*" if open_demo else ""

    honeypot_line = f"Honeypot при закрытии: *{stats['honeypots']}* ⚠️\n" if stats["honeypots"] else ""
    best_line  = f"Лучшая: *+{stats['best']['pnl_pct']:.1f}%* ({stats['best']['symbol']})\n" if stats.get("best") else ""
    worst_line = f"Худшая: *{stats['worst']['pnl_pct']:+.1f}%* ({stats['worst']['symbol']})\n" if stats.get("worst") else ""

    await update.message.reply_text(
        f"🎭 *Demo статистика*\n\n"
        f"Всего сделок: *{stats['total']}*\n"
        f"Прибыльных: *{stats['wins']}* | Убыточных: *{stats['losses']}*\n"
        f"{honeypot_line}"
        f"Winrate: *{stats['win_rate']:.0f}%* (без honeypot)\n\n"
        f"Средний P&L: *{stats['avg_pnl']:+.1f}%*\n"
        f"{best_line}"
        f"{worst_line}\n"
        f"Вложено виртуально: *{stats['total_invested']:.3f} BNB*\n"
        f"P&L: *{stats['total_pnl_bnb']:+.4f} BNB* (~${pnl_usd:+.0f})"
        f"{open_line}\n\n"
        f"/demostats reset — сбросить статистику",
        parse_mode="Markdown",
    )


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    global execution_engine

    _acquire_pid_lock()
    log.info("Sniper Bot starting...")

    execution_engine = ExecutionEngine(w3, trader, pos_manager, tg_send)

    app = Application.builder().token(config.BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("positions",  cmd_positions))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("demostats",  cmd_demostats))
    app.add_handler(CallbackQueryHandler(handle_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    asyncio.create_task(pos_manager.monitor())
    asyncio.create_task(watch_pairs(config.BSC_WS_RPC, on_pair_found))
    asyncio.create_task(execution_engine.run())
    asyncio.create_task(_cleanup_pending())

    log.info(f"Ready. Wallet: {trader.wallet}")
    await tg_send(
        "🚀 *Sniper Bot запущен*\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC)...\n\n"
        "/status — баланс и настройки"
    )

    # Graceful shutdown on SIGTERM (Railway zero-downtime deploys) and SIGINT (Ctrl-C).
    # Without this handler Railway sends SIGTERM → bot ignores it → waits for SIGKILL →
    # old instance keeps polling Telegram → HTTP 409 Conflict with the new instance.
    _shutdown_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for _sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(_sig, _shutdown_event.set)
        except (NotImplementedError, RuntimeError):
            pass  # Windows / test env

    try:
        await _shutdown_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("Shutdown signal received — stopping bot gracefully…")
        _release_pid_lock()
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception as e:
            log.error(f"Shutdown error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
