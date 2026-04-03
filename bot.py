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
from demo import DemoManager
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

# ── Dynamic position sizing ───────────────────────────────────────────────────

def calculate_max_positions(balance_bnb: float) -> int:
    """
    Max concurrent positions in auto mode = floor(15% of balance / % per trade).
    Keeps ~15% of capital deployed at any time.
      ≤1 BNB  (5%/trade) → 3 positions
      1-5 BNB (3%/trade) → 5 positions
      >5 BNB  (2%/trade) → 7 positions
    Manual override: set MAX_AUTO_POSITIONS > 0 in config.
    """
    if config.MAX_AUTO_POSITIONS > 0:
        return config.MAX_AUTO_POSITIONS
    if balance_bnb <= 1.0:
        return 3
    elif balance_bnb <= 5.0:
        return 5
    else:
        return 7


def calculate_buy_amount(balance_bnb: float) -> float:
    """
    Returns BNB amount to spend on a single trade based on current balance.
    Returns 0.0 if trade should be skipped (balance too low or gas would dominate).

    Auto-tier logic (when BUY_PCT_OF_BALANCE == 0):
      balance ≤ 1 BNB  → 5%  (small account, grow faster)
      balance 1–5 BNB  → 3%  (balanced)
      balance > 5 BNB  → 2%  (conservative, protect capital)

    Manual override: set BUY_PCT_OF_BALANCE > 0 to bypass tiers.
    """
    available = balance_bnb - config.GAS_RESERVE_BNB
    if available <= 0:
        return 0.0

    if config.BUY_PCT_OF_BALANCE > 0:
        pct = config.BUY_PCT_OF_BALANCE
    elif balance_bnb <= 1.0:
        pct = 5.0
    elif balance_bnb <= 5.0:
        pct = 3.0
    else:
        pct = 2.0

    amount = available * pct / 100.0

    if amount < config.BUY_MIN_BNB:
        return 0.0  # too small — gas would eat most of any profit

    return round(min(amount, config.BUY_MAX_BNB), 4)


# ── Bot state ─────────────────────────────────────────────────────────────────

is_paused: bool = False
is_auto:   bool = config.AUTO_BUY
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

# Demo manager — initialised with placeholder BNB; reset at /demo on
demo_manager: DemoManager | None = None


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
        wallet_address=trader.wallet,
    )

    if not result["ok"]:
        log.info(f"Rejected {token_address}: {result['reason']}")
        return

    info      = result["info"]
    bnb_price = info["bnb_price"]

    balance   = w3.eth.get_balance(trader.wallet) / 1e18
    buy_amount = calculate_buy_amount(balance)
    if buy_amount == 0.0:
        log.info(f"Skipping {token_address}: balance too low for min trade size")
        return

    warnings = info.get("extra_warnings", [])
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
        f"💰 Покупка: *{buy_amount} BNB* (~${buy_amount * bnb_price:.0f}) "
        f"| Баланс: {balance:.3f} BNB\n"
        f"⏰ {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')} МСК"
    )

    cb_id = token_address[:10]

    # ── DEMO MODE — virtual trade, no real money ──────────────────────────────
    if demo_manager and demo_manager.enabled:
        max_pos = calculate_max_positions(demo_manager.balance_bnb)
        if len(demo_manager.positions) >= max_pos:
            log.info(f"Demo: max positions ({max_pos}) reached, skipping {info['symbol']}")
            return
        if token_address in demo_manager.positions:
            return
        demo_buy = calculate_buy_amount(demo_manager.balance_bnb)
        if demo_buy == 0.0:
            log.info(f"Demo: virtual balance too low, skipping {info['symbol']}")
            return

        price = await asyncio.to_thread(trader.get_price, token_address, base_token)
        if price <= 0:
            return

        ok = demo_manager.buy(
            token_address     = token_address,
            symbol            = info["symbol"],
            name              = info["name"],
            buy_price_bnb     = price,
            buy_bnb           = demo_buy,
            take_profit_1     = config.TAKE_PROFIT_1,
            take_profit_1_pct = config.TAKE_PROFIT_1_PCT,
            trailing_stop_pct = config.TRAILING_STOP_PCT,
            stop_loss         = config.STOP_LOSS,
        )
        if ok:
            await tg_send(
                f"📊 *\[ДЕМО\] Куплено — {info['name']} ({info['symbol']})*\n\n"
                f"Цена входа: `{price:.8f}` BNB\n"
                f"Сумма: *{demo_buy} BNB* виртуальных\n"
                f"Ликвидность: ${info['liquidity_usd']:,.0f}\n\n"
                f"TP1: +{config.TAKE_PROFIT_1}%  "
                f"| Trailing: -{config.TRAILING_STOP_PCT}%  "
                f"| SL: -{config.STOP_LOSS}%\n"
                f"Виртуальный баланс: {demo_manager.balance_bnb:.4f} BNB"
            )
        return

    # ── AUTO-BUY MODE ─────────────────────────────────────────────────────────
    if is_auto:
        max_pos = calculate_max_positions(balance)
        if len(pos_manager.positions) >= max_pos:
            log.info(f"Auto: max positions ({max_pos}) reached, skipping {info['symbol']}")
            return
        if token_address in pos_manager.positions:
            return

        await tg_send(
            f"⚡ *Авто-покупка* — {info['name']} (`{info['symbol']}`)\n"
            f"💰 {buy_amount} BNB (~${buy_amount * bnb_price:.0f}) | "
            f"Ликвидность: ${info['liquidity_usd']:,.0f}\n"
            f"{warn_block}"
        )

        approved = await asyncio.to_thread(trader.approve_token, token_address)
        if not approved:
            await tg_send(f"❌ Авто: не удалось одобрить *{info['symbol']}*")
            return

        price_before = await asyncio.to_thread(trader.get_price, token_address, base_token)
        result       = await asyncio.to_thread(trader.buy, token_address, buy_amount)

        if result["ok"]:
            entry_price = price_before if price_before > 0 else (
                buy_amount / (result["tokens_received"] / 10 ** result["decimals"])
            )
            pos = Position(
                token_address     = token_address,
                symbol            = info["symbol"],
                name              = info["name"],
                pair_address      = pair_address,
                buy_price_bnb     = entry_price,
                tokens_amount     = result["tokens_received"],
                decimals          = result["decimals"],
                buy_bnb           = buy_amount,
                take_profit_1     = config.TAKE_PROFIT_1,
                take_profit_1_pct = config.TAKE_PROFIT_1_PCT,
                trailing_stop_pct = config.TRAILING_STOP_PCT,
                stop_loss         = config.STOP_LOSS,
            )
            pos_manager.add(pos)
            amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
            await tg_send(
                f"✅ *Куплено авто* — {info['symbol']}\n\n"
                f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                f"Цена входа: {entry_price:.8f} BNB\n"
                f"Tx: `{result['tx_hash']}`\n\n"
                f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%  "
                f"| Trailing: -{config.TRAILING_STOP_PCT}%  "
                f"| SL: -{config.STOP_LOSS}%\n"
                f"Позиций открыто: {len(pos_manager.positions)}/{max_pos}"
            )
        else:
            await tg_send(f"❌ Авто: ошибка покупки *{info['symbol']}*: {result['reason']}")
        return

    # ── MANUAL MODE: send notification with buttons ───────────────────────────
    pending[cb_id] = {
        "token_address": token_address,
        "base_token":    base_token,
        "pair_address":  pair_address,
        "info":          info,
        "ts":            time.time(),
        "buy_amount":    buy_amount,
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

        # Recalculate at buy time — balance may have changed since notification
        current_balance = w3.eth.get_balance(trader.wallet) / 1e18
        buy_amount = calculate_buy_amount(current_balance)
        if buy_amount == 0.0:
            await query.edit_message_text(
                f"💸 Баланс слишком мал для покупки {sym}.\n"
                f"Нужно минимум {config.BUY_MIN_BNB + config.GAS_RESERVE_BNB:.3f} BNB "
                f"(торговля + газ). Текущий баланс: {current_balance:.4f} BNB."
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
            trader.buy, token_address, buy_amount
        )

        if result["ok"]:
            entry_price = price_before if price_before > 0 else (
                buy_amount / (result["tokens_received"] / 10 ** result["decimals"])
            )
            pos = Position(
                token_address      = token_address,
                symbol             = sym,
                name               = token_info["info"]["name"],
                pair_address       = token_info["pair_address"],
                buy_price_bnb      = entry_price,
                tokens_amount      = result["tokens_received"],
                decimals           = result["decimals"],
                buy_bnb            = buy_amount,
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
        "/auto on|off — авто-режим (покупает сам без подтверждения)\n"
        "/pause — приостановить снайпинг\n"
        "/resume — возобновить снайпинг\n\n"
        "*Настройки* (изменить без перезапуска)\n"
        "`/set pct 3` — % баланса на сделку (0 = авто-тир)\n"
        "`/set minbuy 0.03` — мин. сумма сделки BNB\n"
        "`/set maxbuy 0.5` — макс. сумма сделки BNB\n"
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
async def cmd_demo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global demo_manager
    args    = context.args
    subcmd  = args[0].lower() if args else "status"

    if subcmd == "on":
        bnb_price = await get_bnb_price(w3)
        if bnb_price <= 0:
            await update.message.reply_text("❌ Не удалось получить цену BNB.")
            return
        initial_bnb = round(1000.0 / bnb_price, 4)
        fresh = demo_manager is None
        if fresh:
            demo_manager = DemoManager(trader, tg_send, initial_bnb)
        else:
            demo_manager.reset(initial_bnb)
        demo_manager.enabled = True
        demo_manager._save()
        if fresh:
            asyncio.create_task(demo_manager.monitor())
        await update.message.reply_text(
            f"📊 *Демо-режим запущен*\n\n"
            f"Виртуальный баланс: *{initial_bnb:.4f} BNB* (~$1000)\n"
            f"Курс BNB: ${bnb_price:.0f}\n\n"
            f"Бот анализирует реальные токены и совершает виртуальные сделки.\n"
            f"Реальные деньги не тратятся.\n\n"
            f"Следи за статистикой: /demo\n"
            f"Остановить: /demo off",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if subcmd == "off":
        if demo_manager:
            demo_manager.enabled = False
            demo_manager._save()
        await update.message.reply_text("📊 Демо-режим остановлен. Статистика сохранена.")
        return

    if subcmd == "reset":
        bnb_price = await get_bnb_price(w3)
        initial_bnb = round(1000.0 / bnb_price, 4) if bnb_price > 0 else 1.67
        if demo_manager is None:
            demo_manager = DemoManager(trader, tg_send, initial_bnb)
        else:
            demo_manager.reset(initial_bnb)
        demo_manager.enabled = True
        demo_manager._save()
        await update.message.reply_text(
            f"🔄 Демо сброшен. Новый баланс: *{initial_bnb:.4f} BNB* (~$1000)",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if subcmd == "history":
        if not demo_manager or not demo_manager.trades:
            await update.message.reply_text("📊 История демо-сделок пустая.")
            return
        last10 = demo_manager.trades[-10:][::-1]
        lines  = ["📊 *Последние демо-сделки*\n"]
        for t in last10:
            emoji = "✅" if t["pnl_pct"] > 0 else "🔴"
            lines.append(
                f"{emoji} *{t['symbol']}* {t['pnl_pct']:+.1f}% "
                f"({t['pnl_bnb']:+.4f} BNB) — {t['reason']}\n"
                f"    {t['opened_at']} → {t['closed_at']}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
        return

    # Default: show stats
    if not demo_manager:
        await update.message.reply_text(
            "📊 Демо не запущен.\n\n"
            "Запустить: /demo on\n"
            "Бот начнёт виртуальные сделки с $1000 на реальных токенах."
        )
        return

    bnb_price = await get_bnb_price(w3)
    s         = demo_manager.get_stats(bnb_price)
    status    = "▶️ активен" if demo_manager.enabled else "⏸ остановлен"

    open_lines = []
    for pos in demo_manager.positions.values():
        price   = await asyncio.to_thread(trader.get_price, pos.token_address)
        pnl_pct = (price - pos.buy_price_bnb) / pos.buy_price_bnb * 100 if price and pos.buy_price_bnb else 0
        phase   = "trailing" if pos.tp1_done else "phase1"
        open_lines.append(f"  • {pos.symbol}: {pnl_pct:+.1f}% [{phase}]")

    open_text = "\n".join(open_lines) if open_lines else "  нет открытых позиций"
    pnl_emoji = "📈" if s["pnl_pct"] >= 0 else "📉"

    await update.message.reply_text(
        f"📊 *Демо-счёт* — {status}\n\n"
        f"Стартовый баланс: *${s['initial_usd']:.0f}* ({s['initial_bnb']:.4f} BNB)\n"
        f"Текущий баланс:   *${s['balance_usd']:.0f}* ({s['balance_bnb']:.4f} BNB)\n"
        f"{pnl_emoji} Итого P&L: *{s['pnl_pct']:+.1f}%* ({s['pnl_bnb']:+.4f} BNB / ${s['pnl_usd']:+.0f})\n\n"
        f"Закрытых сделок: *{s['total_trades']}*\n"
        f"Прибыльных: {s['wins']}  |  Убыточных: {s['losses']}\n"
        f"Win rate: *{s['win_rate']:.1f}%*\n\n"
        f"Открытых позиций: {s['open']}\n"
        f"{open_text}\n\n"
        + (f"Лучшая: *{s['best']['symbol']}* {s['best']['pnl_pct']:+.1f}%\n" if s["best"] else "")
        + (f"Худшая: *{s['worst']['symbol']}* {s['worst']['pnl_pct']:+.1f}%\n" if s["worst"] else "")
        + "\n`/demo history` — история сделок\n`/demo reset` — начать заново",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_auto
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        status = "включён ✅" if is_auto else "выключен ❌"
        balance = w3.eth.get_balance(trader.wallet) / 1e18
        max_pos = calculate_max_positions(balance)
        await update.message.reply_text(
            f"⚡ *Авто-режим*: {status}\n\n"
            f"Текущий лимит позиций: *{max_pos}* "
            f"(баланс {balance:.3f} BNB)\n\n"
            f"`/auto on` — включить\n"
            f"`/auto off` — выключить",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if args[0].lower() == "on":
        if is_auto:
            await update.message.reply_text("⚡ Авто-режим уже включён.")
            return
        is_auto = True
        balance = w3.eth.get_balance(trader.wallet) / 1e18
        max_pos = calculate_max_positions(balance)
        buy_amt = calculate_buy_amount(balance)
        await update.message.reply_text(
            f"⚡ *Авто-режим включён*\n\n"
            f"Бот будет покупать самостоятельно без твоего подтверждения.\n\n"
            f"Баланс: {balance:.4f} BNB\n"
            f"Сумма на сделку: *{buy_amt} BNB*\n"
            f"Макс. позиций: *{max_pos}* (~15% капитала в работе)\n\n"
            f"⚠️ Убедись что фильтры настроены правильно (/status).\n"
            f"Отключить: /auto off",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        if not is_auto:
            await update.message.reply_text("⚡ Авто-режим уже выключен.")
            return
        is_auto = False
        await update.message.reply_text(
            "🔵 *Авто-режим выключен*\n\n"
            "Бот снова будет присылать уведомления с кнопками для ручного подтверждения.",
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
        "pct":    ("BUY_PCT_OF_BALANCE", 0.0,  20.0,  "% баланса на сделку (0 = авто-тир)"),
        "minbuy": ("BUY_MIN_BNB",        0.01, 1.0,   "Мин. сумма сделки BNB"),
        "maxbuy": ("BUY_MAX_BNB",        0.05, 10.0,  "Макс. сумма сделки BNB"),
        "sl":     ("STOP_LOSS",          1.0,  90.0,  "Стоп-лосс %"),
        "tp1":    ("TAKE_PROFIT_1",      5.0,  500.0, "TP1 %"),
        "trail":  ("TRAILING_STOP_PCT",  1.0,  90.0,  "Trailing stop %"),
        "liq":    ("MIN_LIQUIDITY_USD",  500.0, 1e7,  "Мин. ликвидность USD"),
        "tax":    ("MAX_BUY_TAX",        1.0,  50.0,  "Макс. налог %"),
        "max":    ("MAX_POSITIONS",      1,    20,    "Макс. позиций"),
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
    connected  = w3.is_connected()
    balance    = w3.eth.get_balance(trader.wallet) / 1e18
    bnb_price  = await get_bnb_price(w3)
    buy_amount = calculate_buy_amount(balance)
    auto_icon   = "⚡ авто" if is_auto else "👆 ручной"
    status_icon = "⏸ ПАУЗА" if is_paused else f"▶️ активен | {auto_icon}"

    if config.BUY_PCT_OF_BALANCE > 0:
        size_mode = f"{config.BUY_PCT_OF_BALANCE}% (ручной)"
    elif balance <= 1.0:
        size_mode = f"5% авто (баланс ≤ 1 BNB)"
    elif balance <= 5.0:
        size_mode = f"3% авто (баланс 1–5 BNB)"
    else:
        size_mode = f"2% авто (баланс > 5 BNB)"

    await update.message.reply_text(
        f"*Статус Sniper Bot* — {status_icon}\n\n"
        f"RPC: {'✅ подключён' if connected else '❌ нет соединения'}\n"
        f"Кошелёк: `{trader.wallet}`\n"
        f"Баланс: *{balance:.4f} BNB* (~${balance * bnb_price:.0f})\n"
        f"Позиций открыто: {len(pos_manager.positions)}/{calculate_max_positions(balance) if is_auto else config.MAX_POSITIONS}\n"
        f"Сделок в истории: {len(trade_history)}\n\n"
        f"*Размер позиции:*\n"
        f"Режим: {size_mode}\n"
        f"Следующая сделка: *{buy_amount} BNB* (~${buy_amount * bnb_price:.0f})\n"
        f"Мин: {config.BUY_MIN_BNB} BNB  |  Макс: {config.BUY_MAX_BNB} BNB  "
        f"|  Газ-резерв: {config.GAS_RESERVE_BNB} BNB\n\n"
        f"*Настройки:*\n"
        f"Slip buy/sell: {config.SLIPPAGE_BUY}%/{config.SLIPPAGE_SELL}%\n"
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
    app.add_handler(CommandHandler("demo",      cmd_demo))
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("auto",      cmd_auto))
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

    # Restore demo manager if it was running before restart
    global demo_manager
    _dm = DemoManager(trader, tg_send, initial_bnb=1.67)  # placeholder
    if _dm.enabled:
        demo_manager = _dm
        asyncio.create_task(demo_manager.monitor())
        log.info("Demo mode restored from saved state")

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
