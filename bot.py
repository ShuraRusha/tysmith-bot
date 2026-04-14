import asyncio
import json
import logging
import os
import socket
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
DATA_DIR       = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)   # ensure /data exists (Railway Volume or fallback)
TRADE_LOG_FILE = os.path.join(DATA_DIR, "tysmith_trades.json")
SETTINGS_FILE  = os.path.join(DATA_DIR, "tysmith_settings.json")


# ── Web3 setup (try all configured RPCs until one connects) ──────────────────

def _make_w3(url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3

w3 = None
for rpc_url in config.BSC_HTTP_RPCS:
    _w3 = _make_w3(rpc_url)
    if _w3.is_connected():
        w3 = _w3
        log.info(f"Connected to RPC: {rpc_url[:50]}...")
        break
    log.warning(f"RPC unavailable: {rpc_url[:50]}...")

if w3 is None:
    log.error("All RPCs failed — using primary as fallback")
    w3 = _make_w3(config.BSC_HTTP_RPC)

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


# ── Settings persistence ──────────────────────────────────────────────────────

_PERSISTENT_SETTINGS = [
    "BUY_PCT_OF_BALANCE", "BUY_MIN_BNB", "BUY_MAX_BNB",
    "STOP_LOSS", "TAKE_PROFIT_1", "TRAILING_STOP_PCT",
    "MIN_LIQUIDITY_USD", "MAX_BUY_TAX", "MAX_SELL_TAX",
    "MAX_POSITIONS", "GAS_BUY_GWEI",
]


def _save_settings():
    try:
        data = {k: getattr(config, k) for k in _PERSISTENT_SETTINGS}
        # Also persist bot mode state so it survives restarts/redeploys
        data["__is_auto"]   = is_auto
        data["__is_paused"] = is_paused
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.error(f"Failed to save settings: {e}")


def _load_settings():
    global is_auto, is_paused
    try:
        if not os.path.exists(SETTINGS_FILE):
            return
        with open(SETTINGS_FILE) as f:
            data = json.load(f)
        for key, val in data.items():
            if key.startswith("__"):
                continue  # handled separately below
            if key in _PERSISTENT_SETTINGS and hasattr(config, key):
                setattr(config, key, val)
        # Restore bot mode
        if "__is_auto" in data:
            is_auto = bool(data["__is_auto"])
        if "__is_paused" in data:
            is_paused = bool(data["__is_paused"])
        log.info(
            f"Loaded persisted settings ({len(data)} params) | "
            f"auto={'on' if is_auto else 'off'} | "
            f"paused={'yes' if is_paused else 'no'}"
        )
    except Exception as e:
        log.warning(f"Could not load settings: {e}")


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


def _record_buy(pos: Position, tx_hash: str):
    """Record a buy event immediately when position opens."""
    trade_history.append({
        "status":        "open",
        "symbol":        pos.symbol,
        "token_address": pos.token_address,
        "buy_price_bnb": pos.buy_price_bnb,
        "buy_bnb":       pos.buy_bnb,
        "liquidity_usd": pos.liquidity_usd,
        "buy_tax":       pos.buy_tax,
        "sell_tax":      pos.sell_tax,
        "tx_hash_buy":   tx_hash,
        "opened_at":     datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M"),
    })
    _save_history()


def _record_trade(pos: Position, pnl_pct: float, reason: str, sell_price: float = 0.0):
    """Close a trade record. Updates the matching open entry if it exists."""
    pnl_bnb  = round(pos.buy_bnb * pnl_pct / 100, 6)
    hold_sec = int(time.time() - pos.opened_at)
    hold_min = hold_sec // 60
    hold_str = f"{hold_min}м {hold_sec % 60}с" if hold_min < 60 else f"{hold_min // 60}ч {hold_min % 60}м"

    # Try to find and update existing open entry for this token
    for entry in reversed(trade_history):
        if entry.get("status") == "open" and entry.get("token_address") == pos.token_address:
            entry.update({
                "status":         "closed",
                "sell_price_bnb": sell_price,
                "pnl_pct":        round(pnl_pct, 2),
                "pnl_bnb":        pnl_bnb,
                "reason":         reason,
                "hold_sec":       hold_sec,
                "hold_str":       hold_str,
                "closed_at":      datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M"),
            })
            _save_history()
            return

    # Fallback: no open entry found (e.g. position restored from disk before fix)
    trade_history.append({
        "status":         "closed",
        "symbol":         pos.symbol,
        "token_address":  pos.token_address,
        "buy_price_bnb":  pos.buy_price_bnb,
        "sell_price_bnb": sell_price,
        "buy_bnb":        pos.buy_bnb,
        "pnl_pct":        round(pnl_pct, 2),
        "pnl_bnb":        pnl_bnb,
        "reason":         reason,
        "hold_sec":       hold_sec,
        "hold_str":       hold_str,
        "liquidity_usd":  pos.liquidity_usd,
        "buy_tax":        pos.buy_tax,
        "sell_tax":       pos.sell_tax,
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
        wallet_address=trader.wallet,
        require_goplus=is_auto,   # auto mode requires GoPlus for LP-lock check
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

        # Run approve + price fetch in parallel to save ~200ms
        approve_task     = asyncio.to_thread(trader.approve_token, token_address)
        price_task       = asyncio.to_thread(trader.get_price, token_address, base_token)
        approve_result, price_before = await asyncio.gather(approve_task, price_task)

        if not approve_result["ok"]:
            await tg_send(
                f"❌ Авто: не удалось одобрить *{info['symbol']}*\n"
                f"`{approve_result['reason']}`"
            )
            return

        result = await asyncio.to_thread(trader.buy, token_address, buy_amount)

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
                liquidity_usd     = info["liquidity_usd"],
                buy_tax           = info["buy_tax"],
                sell_tax          = info["sell_tax"],
            )
            pos_manager.add(pos)
            _record_buy(pos, result["tx_hash"])
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

        approve_result = await asyncio.to_thread(trader.approve_token, token_address)
        if not approve_result["ok"]:
            await query.edit_message_text(
                f"❌ Не удалось одобрить *{sym}*\n`{approve_result['reason']}`",
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
                liquidity_usd      = token_info["info"]["liquidity_usd"],
                buy_tax            = token_info["info"]["buy_tax"],
                sell_tax           = token_info["info"]["sell_tax"],
            )
            pos_manager.add(pos)
            _record_buy(pos, result["tx_hash"])

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

    # ── WRITE-OFF (remove to history without selling) ─────────────────────────
    elif data.startswith("writeoff_"):
        token_address = data[9:]
        pos = pos_manager.positions.get(token_address)
        if not pos:
            await query.edit_message_text("⚠️ Позиция не найдена или уже закрыта.")
            return

        current_price = await asyncio.to_thread(trader.get_price, pos.token_address)
        pnl_pct = (
            (current_price - pos.buy_price_bnb) / pos.buy_price_bnb * 100
            if pos.buy_price_bnb > 0 and current_price > 0 else -100.0
        )
        _record_trade(pos, pnl_pct, "Списана вручную", current_price)
        pos_manager.remove(token_address)
        await query.edit_message_text(
            f"🗑 *{pos.symbol}* убрана в историю\n"
            f"P&L: {pnl_pct:+.1f}% | Потрачено: {pos.buy_bnb} BNB\n"
            f"Слот позиции освобождён.",
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
        "/stats — полная статистика (win rate, PnL, breakdown)\n"
        "/stats today — только сегодня\n"
        "/stats week — за последние 7 дней\n"
        "/history — последние 10 сделок с временем удержания\n\n"
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
        "`/set max 5` — макс. кол-во позиций\n"
        "`/set gwei 3` — gas для покупки в gwei (0 = авто)",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_paused
    if is_paused:
        await update.message.reply_text("⏸ Бот уже на паузе. Используй /resume для возобновления.")
        return
    is_paused = True
    _save_settings()
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
    _save_settings()
    await update.message.reply_text(
        "▶️ *Снайпинг возобновлён*\n\n"
        "Слежу за новыми парами на PancakeSwap V2.",
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
        _save_settings()
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
        _save_settings()
        await update.message.reply_text(
            "🔵 *Авто-режим выключен*\n\n"
            "Бот снова будет присылать уведомления с кнопками для ручного подтверждения.",
            parse_mode=ParseMode.MARKDOWN,
        )


def _build_stats_report(trades: list[dict], bnb_price: float, title: str = "Статистика сделок") -> str:
    # Normalise: ensure pnl_pct and pnl_bnb are numeric (guard against corrupt entries)
    trades = [t for t in trades if isinstance(t.get("pnl_pct"), (int, float))]
    total  = len(trades)
    if total == 0:
        return f"📊 *{title}*\n\nНедостаточно данных."

    wins   = [t for t in trades if t["pnl_pct"] > 0]
    losses = [t for t in trades if t["pnl_pct"] <= 0]

    total_pnl_bnb = sum(t.get("pnl_bnb", 0) for t in trades)
    avg_pnl_pct   = sum(t["pnl_pct"] for t in trades) / total
    win_rate      = len(wins) / total * 100

    best  = max(trades, key=lambda t: t["pnl_pct"])
    worst = min(trades, key=lambda t: t["pnl_pct"])

    # Exit reason breakdown
    reasons: dict[str, list] = {}
    for t in trades:
        r = t.get("reason", "?")
        reasons.setdefault(r, []).append(t["pnl_pct"])
    reason_lines = []
    for r, pcts in sorted(reasons.items()):
        wr = len([p for p in pcts if p > 0]) / len(pcts) * 100
        avg = sum(pcts) / len(pcts)
        reason_lines.append(f"  {r}: {len(pcts)} сд. | WR {wr:.0f}% | avg {avg:+.1f}%")

    # Hold time (winners vs losers)
    win_holds  = [t.get("hold_sec", 0) for t in wins]
    loss_holds = [t.get("hold_sec", 0) for t in losses]
    avg_win_hold  = int(sum(win_holds)  / len(win_holds))  if win_holds  else 0
    avg_loss_hold = int(sum(loss_holds) / len(loss_holds)) if loss_holds else 0

    def fmt_sec(s: int) -> str:
        return f"{s // 60}м {s % 60}с" if s < 3600 else f"{s // 3600}ч {(s % 3600) // 60}м"

    # Liquidity breakdown (if data available)
    liq_trades = [t for t in trades if t.get("liquidity_usd", 0) > 0]
    liq_lines = []
    if liq_trades:
        for lo, hi, label in [(0, 25000, "<$25k"), (25000, 100000, "$25k-100k"), (100000, 1e9, ">$100k")]:
            bucket = [t for t in liq_trades if lo <= t["liquidity_usd"] < hi]
            if bucket:
                bwr = len([t for t in bucket if t["pnl_pct"] > 0]) / len(bucket) * 100
                bavg = sum(t["pnl_pct"] for t in bucket) / len(bucket)
                liq_lines.append(f"  {label}: {len(bucket)} сд. | WR {bwr:.0f}% | avg {bavg:+.1f}%")

    lines = [
        f"📊 *{title}*\n",
        f"Всего: *{total}* | Прибыльных: *{len(wins)}* | Убыточных: *{len(losses)}*",
        f"Win rate: *{win_rate:.1f}%*",
        f"Общий P&L: *{total_pnl_bnb:+.4f} BNB* (~${total_pnl_bnb * bnb_price:+.0f})",
        f"Средний P&L: *{avg_pnl_pct:+.1f}%* за сделку\n",
        f"Лучшая: *{best['symbol']}* {best['pnl_pct']:+.1f}%",
        f"Худшая: *{worst['symbol']}* {worst['pnl_pct']:+.1f}%\n",
        f"*Причины закрытия:*",
    ] + reason_lines + [
        f"\n*Среднее время удержания:*",
        f"  Победители: {fmt_sec(avg_win_hold)}",
        f"  Неудачники:  {fmt_sec(avg_loss_hold)}",
    ]
    if liq_lines:
        lines += [f"\n*По ликвидности:*"] + liq_lines

    return "\n".join(lines)


@owner_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Stats only count closed trades
    closed = [t for t in trade_history if t.get("status") == "closed" or "pnl_pct" in t]
    open_  = [t for t in trade_history if t.get("status") == "open"]

    if not closed:
        open_count = len(open_)
        msg = "📊 Нет закрытых сделок."
        if open_count:
            msg += f"\n\nОткрытых позиций в истории: *{open_count}* (ещё не закрыты)"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    bnb_price = await get_bnb_price(w3)
    args = context.args
    subcmd = args[0].lower() if args else ""

    if subcmd == "today":
        today  = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
        trades = [t for t in closed if t.get("closed_at", "").startswith(today)]
        if not trades:
            await update.message.reply_text("📊 Сегодня закрытых сделок нет.")
            return
        report = _build_stats_report(trades, bnb_price, "Статистика за сегодня")
    elif subcmd == "week":
        from datetime import timedelta
        week_ago = (datetime.now(MOSCOW_TZ) - timedelta(days=7)).strftime("%Y-%m-%d")
        trades   = [t for t in closed if t.get("closed_at", "") >= week_ago]
        if not trades:
            await update.message.reply_text("📊 За последнюю неделю нет сделок.")
            return
        report = _build_stats_report(trades, bnb_price, "Статистика за неделю")
    else:
        report = _build_stats_report(closed, bnb_price)

    if open_:
        report += f"\n\n📈 Открытых позиций: *{len(open_)}* (не учтены в статистике)"
    await update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)


@owner_only
async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not trade_history:
        await update.message.reply_text("📜 История пустая — покупок ещё не было.")
        return

    last15 = trade_history[-15:][::-1]
    lines  = ["📜 *Последние транзакции*\n"]
    for t in last15:
        liq = f" | 💧${t['liquidity_usd']:,.0f}" if t.get("liquidity_usd") else ""
        if t.get("status") == "open":
            # Position still open — show as "holding"
            lines.append(
                f"📈 *{t['symbol']}* — удерживается\n"
                f"    Куплено: {t['buy_bnb']} BNB{liq} | `{t.get('opened_at', '?')}`"
            )
        else:
            pnl_pct = t.get("pnl_pct", 0)
            emoji   = "✅" if pnl_pct > 0 else "🔴"
            hold    = t.get("hold_str", "?")
            lines.append(
                f"{emoji} *{t['symbol']}* {pnl_pct:+.1f}% "
                f"({t.get('pnl_bnb', 0):+.4f} BNB) — {t.get('reason', '?')}\n"
                f"    ⏱ {hold}{liq} | `{t.get('closed_at', '?')}`"
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
        "gwei":   ("GAS_BUY_GWEI",       0.0,  100.0, "Фикс. gas для покупки (0 = авто)"),
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

    _save_settings()

    await update.message.reply_text(
        f"✅ *{label}* обновлён\n"
        f"{old_value} → *{value}*\n"
        f"💾 Сохранено — переживёт перезапуск",
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
        if pos.stuck:
            phase = f"🚫 STUCK — продажа невозможна (honeypot?), попыток: {pos.sell_failures}"
        elif pos.tp1_done:
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
        writeoff_btn = InlineKeyboardButton(
            "🗑 Убрать в историю",
            callback_data=f"writeoff_{pos.token_address}",
        )
        sell_btn = InlineKeyboardButton(
            f"🔴 Продать {pos.symbol}",
            callback_data=f"sell_{pos.token_address}",
        )
        # For stuck (honeypot) positions: write-off is primary, sell is secondary
        if pos.stuck:
            keyboard = InlineKeyboardMarkup([[writeoff_btn], [sell_btn]])
        else:
            keyboard = InlineKeyboardMarkup([[sell_btn, writeoff_btn]])
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

    rpc_label = "NodeReal" if config.BSC_NODEREAL_KEY else "Public"
    gas_gwei  = w3.eth.gas_price / 1e9 if connected else 0
    gas_mode  = f"{config.GAS_BUY_GWEI} gwei (фикс)" if config.GAS_BUY_GWEI > 0 else f"{gas_gwei:.1f} x{config.GAS_MULTIPLIER}"
    ws_count  = len(config.BSC_WS_RPCS)

    await update.message.reply_text(
        f"*Статус Sniper Bot* — {status_icon}\n\n"
        f"RPC: {'✅' if connected else '❌'} {rpc_label} | WS endpoints: {ws_count}\n"
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
        f"Gas buy: {gas_mode}\n"
        f"Slip buy/sell: {config.SLIPPAGE_BUY}%/{config.SLIPPAGE_SELL}%\n"
        f"Deadline: {config.TX_DEADLINE_SEC}s\n"
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


# ── Background: daily report at 23:00 MSK ────────────────────────────────────

async def _daily_report():
    """Send trading summary every day at 23:00 Moscow time."""
    while True:
        now_msk   = datetime.now(MOSCOW_TZ)
        target    = now_msk.replace(hour=23, minute=0, second=0, microsecond=0)
        if now_msk >= target:
            target = target.replace(day=target.day + 1)
        wait_sec  = (target - now_msk).total_seconds()
        log.info(f"Daily report scheduled in {wait_sec/3600:.1f}h")
        await asyncio.sleep(wait_sec)

        today  = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
        trades = [t for t in trade_history if t.get("closed_at", "").startswith(today)]

        try:
            bnb_price = await get_bnb_price(w3)
            balance   = w3.eth.get_balance(trader.wallet) / 1e18

            if trades:
                report = _build_stats_report(trades, bnb_price,
                                             f"Итоги дня {today}")
                pnl_bnb = sum(t["pnl_bnb"] for t in trades)
                await tg_send(
                    f"{report}\n\n"
                    f"💼 Баланс кошелька: *{balance:.4f} BNB* (~${balance * bnb_price:.0f})\n"
                    f"Открытых позиций: *{len(pos_manager.positions)}*"
                )
            else:
                await tg_send(
                    f"📊 *Итоги дня {today}*\n\n"
                    f"Сегодня сделок не было.\n"
                    f"💼 Баланс: *{balance:.4f} BNB* (~${balance * bnb_price:.0f})\n"
                    f"Открытых позиций: *{len(pos_manager.positions)}*"
                )
        except Exception as e:
            log.error(f"Daily report error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

# Distributed lock on shared /data volume — prevents two Railway containers
# from running simultaneously during blue-green deploys.
# Old instance refreshes the lock every LOCK_REFRESH_SEC seconds.
# New instance waits up to LOCK_WAIT_SEC for the old one to stop, then takes over.
DIST_LOCK_FILE     = os.path.join(DATA_DIR, "tysmith.lock")
LOCK_EXPIRY_SEC    = 25   # lock is stale if not refreshed for this long
LOCK_REFRESH_SEC   = 10   # how often the running instance refreshes its lock
LOCK_WAIT_SEC      = 40   # how long new instance waits for old one to release


def _read_lock() -> dict:
    try:
        with open(DIST_LOCK_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _write_lock():
    try:
        with open(DIST_LOCK_FILE, "w") as f:
            json.dump({
                "ts":   time.time(),
                "pid":  os.getpid(),
                "host": socket.gethostname(),
            }, f)
    except Exception as e:
        log.warning(f"Lock write failed: {e}")


def _acquire_distributed_lock():
    """
    Block until we own the distributed lock or give up after LOCK_WAIT_SEC.
    Railway starts a new container before stopping the old one, so we poll
    until the old instance's lock expires (old instance stopped refreshing it).
    """
    deadline = time.time() + LOCK_WAIT_SEC
    while time.time() < deadline:
        data = _read_lock()
        age  = time.time() - data.get("ts", 0)
        host = data.get("host", "")
        if age >= LOCK_EXPIRY_SEC or host == socket.gethostname():
            # Lock is stale or belongs to us already → take it
            _write_lock()
            log.info(f"Distributed lock acquired (previous age={age:.0f}s, host={host})")
            return
        log.info(f"Waiting for previous instance to stop (lock age={age:.0f}s, host={host})…")
        time.sleep(3)
    # Timeout — take over anyway (better than not starting at all)
    log.warning("Lock wait timed out — taking over")
    _write_lock()


def _release_distributed_lock():
    try:
        os.remove(DIST_LOCK_FILE)
    except FileNotFoundError:
        pass


async def _lock_refresher():
    """Keep the distributed lock alive while the bot is running."""
    while True:
        await asyncio.sleep(LOCK_REFRESH_SEC)
        _write_lock()


async def main():
    _load_history()
    _load_settings()
    log.info("Sniper Bot starting...")

    app = Application.builder().token(config.BOT_TOKEN).build()

    # Drop any webhook that might be set — prevents conflict with polling.
    await app.bot.delete_webhook(drop_pending_updates=True)

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
    asyncio.create_task(_daily_report())
    asyncio.create_task(_lock_refresher())

    # Restore open positions from disk (survive restarts/redeploys)
    restored = pos_manager.load()

    # Clean up broken positions on startup:
    #   • zombie: buy_price_bnb == 0 or tokens_amount == 0 — unmonitorable
    #   • stuck:  sell was impossible (honeypot) — already gave up, free the slot
    zombie_names = []
    stuck_names  = []
    for addr in list(pos_manager.positions):
        pos = pos_manager.positions[addr]
        if pos.stuck:
            log.warning(f"Startup: removing stuck position {pos.symbol}")
            _record_trade(pos, pnl_pct=-100.0, reason="Honeypot", sell_price=0.0)
            pos_manager.remove(addr)
            stuck_names.append(pos.symbol)
        elif pos.buy_price_bnb <= 0 or pos.tokens_amount <= 0:
            log.warning(
                f"Startup: removing zombie position {pos.symbol} "
                f"(price={pos.buy_price_bnb}, amount={pos.tokens_amount})"
            )
            _record_trade(pos, pnl_pct=0.0, reason="Invalid (нет данных)", sell_price=0.0)
            pos_manager.remove(addr)
            zombie_names.append(pos.symbol)

    log.info(f"Ready. Wallet: {trader.wallet}")
    startup_msg = (
        "🚀 *Sniper Bot запущен*\n"
        "Слежу за новыми парами на PancakeSwap V2 (BSC)...\n\n"
        "/help — все команды"
    )
    if restored:
        startup_msg += f"\n\n♻️ Восстановлено позиций: *{restored}* — мониторинг возобновлён"
    if stuck_names:
        startup_msg += (
            f"\n\n🚫 Удалены honeypot-позиции: *{', '.join(stuck_names)}*\n"
            f"Записаны в историю как убыток"
        )
    if zombie_names:
        startup_msg += (
            f"\n\n🗑 Удалены зомби-позиции (нет цены входа): *{', '.join(zombie_names)}*\n"
            f"Записаны в историю как `Invalid`"
        )
    await tg_send(startup_msg)

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        _release_distributed_lock()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    _acquire_distributed_lock()
    asyncio.run(main())
