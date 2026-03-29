import asyncio
import logging
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
from analyzer import check_token, get_bnb_price
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

# callback_id (first 10 chars of token address) → token info
# cleaned up after user action (buy or skip)
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

pos_manager = PositionManager(trader, tg_send)


# ── New pair handler ──────────────────────────────────────────────────────────

async def on_pair_found(token_address: str, base_token: str, pair_address: str):
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

    # Build warning lines for non-critical flags
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
        f"📊 TP: +{config.TAKE_PROFIT}%  |  SL: -{config.STOP_LOSS}%\n"
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

        sym = token_info["info"]["symbol"]
        await query.edit_message_text(
            f"⏳ Покупаю *{sym}*...", parse_mode=ParseMode.MARKDOWN
        )

        result = await asyncio.to_thread(
            trader.buy, token_info["token_address"], config.BUY_AMOUNT_BNB
        )

        if result["ok"]:
            price = await asyncio.to_thread(
                trader.get_price,
                token_info["token_address"],
                token_info["base_token"],
            )
            pos = Position(
                token_address = token_info["token_address"],
                symbol        = sym,
                name          = token_info["info"]["name"],
                pair_address  = token_info["pair_address"],
                buy_price_bnb = price,
                tokens_amount = result["tokens_received"],
                decimals      = result["decimals"],
                buy_bnb       = config.BUY_AMOUNT_BNB,
                take_profit   = config.TAKE_PROFIT,
                stop_loss     = config.STOP_LOSS,
            )
            pos_manager.add(pos)
            amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
            await query.edit_message_text(
                f"✅ *Куплено!* — {sym}\n\n"
                f"Получено: {amount_fmt:.4f} {sym}\n"
                f"Цена входа: {price:.8f} BNB\n"
                f"Tx: `{result['tx_hash']}`\n\n"
                f"TP: +{config.TAKE_PROFIT}%  |  SL: -{config.STOP_LOSS}%",
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
        "Слежу за новыми парами на PancakeSwap V2 (BSC).\n"
        "Каждый токен проходит автоматическую проверку:\n"
        "honeypot · налог · ликвидность · опасные функции\n\n"
        "При появлении чистого токена — пришлю уведомление с кнопками.\n\n"
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
        text = (
            f"*{pos.name}* ({pos.symbol})\n"
            f"Вход: {pos.buy_price_bnb:.8f} BNB\n"
            f"Сейчас: {price:.8f} BNB\n"
            f"P&L: {pnl_pct:+.1f}%\n"
            f"TP: +{pos.take_profit}%  |  SL: -{pos.stop_loss}%"
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
    connected = w3.is_connected()
    balance   = w3.eth.get_balance(trader.wallet) / 1e18
    bnb_price = await get_bnb_price(w3)
    await update.message.reply_text(
        f"*Статус Sniper Bot*\n\n"
        f"RPC: {'✅ подключён' if connected else '❌ нет соединения'}\n"
        f"Кошелёк: `{trader.wallet}`\n"
        f"Баланс: *{balance:.4f} BNB* (~${balance * bnb_price:.0f})\n"
        f"Позиций открыто: {len(pos_manager.positions)}\n\n"
        f"*Настройки:*\n"
        f"Buy: {config.BUY_AMOUNT_BNB} BNB  |  Slippage: {config.SLIPPAGE}%\n"
        f"TP: +{config.TAKE_PROFIT}%  |  SL: -{config.STOP_LOSS}%\n"
        f"Min ликвидность: ${config.MIN_LIQUIDITY_USD:,.0f}\n"
        f"Max tax: {config.MAX_BUY_TAX}% buy / {config.MAX_SELL_TAX}% sell",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    log.info("Sniper Bot starting...")

    app = Application.builder().token(config.BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CallbackQueryHandler(handle_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    asyncio.create_task(pos_manager.monitor())
    asyncio.create_task(watch_pairs(config.BSC_WS_RPC, on_pair_found))

    log.info(f"Ready. Wallet: {trader.wallet}")
    await tg_send(
        "🚀 *Sniper Bot запущен*\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC)...\n\n"
        "/status — баланс и настройки"
    )

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
