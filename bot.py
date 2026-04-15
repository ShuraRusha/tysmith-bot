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
    MessageHandler,
    filters,
)
from web3 import Web3
from web3.middleware import geth_poa_middleware

import blacklist
import config
from analyzer import analyze_token, check_token, get_bnb_price
from position import Position, PositionManager
from trader import Trader
from watcher import watch_pairs, watch_pending_pairs

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

# Tokens currently being bought (prevents concurrent buy of the same token)
_buying_tokens: set[str] = {}

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


def _calc_moon_bag(tokens_received: int, buy_bnb: float, bnb_price: float) -> int:
    """Return the number of raw tokens to reserve as a moon bag (not auto-sold).
    Activated only when trade size >= MOON_BAG_MIN_USD. Returns 0 if disabled."""
    if buy_bnb * bnb_price < config.MOON_BAG_MIN_USD:
        return 0
    return int(tokens_received * config.MOON_BAG_PCT / 100)


# ── Bot state ─────────────────────────────────────────────────────────────────

is_paused: bool = False
is_auto:   bool = config.AUTO_BUY
trade_history: list[dict] = []


# ── Settings persistence ──────────────────────────────────────────────────────

# Increment when adding new persistent params or changing hardcoded defaults.
# Used to migrate old settings files that pre-date a change.
SETTINGS_VERSION = 8

_PERSISTENT_SETTINGS = [
    "BUY_PCT_OF_BALANCE", "BUY_MIN_BNB", "BUY_MAX_BNB",
    "STOP_LOSS", "TAKE_PROFIT_1", "TRAILING_STOP_PCT",
    "MIN_LIQUIDITY_USD", "MAX_BUY_TAX", "MAX_SELL_TAX",
    "MAX_POSITIONS", "GAS_BUY_GWEI",
    # Added in v2 (new buying filters):
    "MIN_MARKET_CAP_USD", "MIN_FDV_USD", "MAX_FDV_USD",
    "MIN_VOLUME_5M_USD", "MAX_TOKEN_AGE_DAYS", "MAX_TOP10_HOLDER_PCT",
    "MOON_BAG_MIN_USD", "MOON_BAG_PCT",
    # Added in v5:
    "LP_HOLDER_MAX_PCT", "MIN_HOLDER_COUNT",
    # Added in v7:
    "MAX_DEPLOYER_TOKENS_30D",
]


def _save_settings():
    try:
        data = {k: getattr(config, k) for k in _PERSISTENT_SETTINGS}
        data["__is_auto"]   = is_auto
        data["__is_paused"] = is_paused
        data["__version"]   = SETTINGS_VERSION
        tmp_path = SETTINGS_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, SETTINGS_FILE)
    except Exception as e:
        log.error(f"Failed to save settings: {e}")


def _load_settings():
    global is_auto, is_paused
    try:
        if not os.path.exists(SETTINGS_FILE):
            return
        with open(SETTINGS_FILE) as f:
            data = json.load(f)

        saved_version = int(data.get("__version", 1))

        for key, val in data.items():
            if key.startswith("__"):
                continue
            if key in _PERSISTENT_SETTINGS and hasattr(config, key):
                setattr(config, key, val)

        # ── Migrations ────────────────────────────────────────────────────────
        # v1→v2: MIN_LIQUIDITY_USD default raised 10k→50k.
        # If the file still has the old default (≤10k), apply the new one.
        # User can always lower it back via /set liq <value>.
        if saved_version < 2 and config.MIN_LIQUIDITY_USD <= 10_000:
            config.MIN_LIQUIDITY_USD = 50_000.0
            log.info("Settings migration v1→v2: MIN_LIQUIDITY_USD 10000 → 50000")

        # Migration v2 → v3: optimized defaults (user-approved 2026-04-14)
        #   GAS_BUY_GWEI       3 → 5   sniper speed
        #   TAKE_PROFIT_1     30 → 50  ride momentum longer before partial exit
        #   TRAILING_STOP_PCT 20 → 15  lock gains faster after TP1
        #   MIN_MARKET_CAP_USD 30k → 50k
        #   MIN_FDV_USD       200k → 300k
        #   MIN_VOLUME_5M_USD   1k → 3k  filter illiquid tokens
        #   MAX_TOKEN_AGE_DAYS  30 → 7   sniper = fresh pairs only
        if saved_version < 3:
            config.GAS_BUY_GWEI       = 5.0
            config.TAKE_PROFIT_1      = 50.0
            config.TRAILING_STOP_PCT  = 15.0
            config.MIN_MARKET_CAP_USD = 50_000.0
            config.MIN_FDV_USD        = 300_000.0
            config.MIN_VOLUME_5M_USD  = 3_000.0
            config.MAX_TOKEN_AGE_DAYS = 7
            log.info("Settings migration v2→v3: applied optimized sniper defaults")

        # Migration v3 → v4: MIN_LIQUIDITY_USD 50k → 30k (more signals, same safety)
        if saved_version < 4:
            config.MIN_LIQUIDITY_USD = 30_000.0
            log.info("Settings migration v3→v4: MIN_LIQUIDITY_USD 50000 → 30000")

        # Migration v4 → v5: LP_HOLDER_MAX_PCT 50 → 30, MIN_HOLDER_COUNT = 50
        if saved_version < 5:
            config.LP_HOLDER_MAX_PCT = 30.0
            config.MIN_HOLDER_COUNT  = 50
            log.info("Settings migration v4→v5: LP_HOLDER_MAX_PCT=30, MIN_HOLDER_COUNT=50")

        # Migration v5 → v6: MIN_HOLDER_COUNT 50 → 25
        # New tokens take 15-30 min to reach 50 holders; 25 is realistic threshold
        if saved_version < 6:
            config.MIN_HOLDER_COUNT = 25
            log.info("Settings migration v5→v6: MIN_HOLDER_COUNT 50 → 25")

        # Migration v6 → v7: MAX_DEPLOYER_TOKENS_30D added (deployer history check)
        if saved_version < 7:
            config.MAX_DEPLOYER_TOKENS_30D = 3
            log.info("Settings migration v6→v7: MAX_DEPLOYER_TOKENS_30D = 3")

        # Migration v7 → v8: loosen entry filters for more trade volume
        #   MIN_LIQUIDITY_USD   30k → 20k  (safe for small trades, +50% more pairs)
        #   MIN_FDV_USD         300k → 150k (catch earlier-stage tokens)
        #   MIN_MARKET_CAP_USD  50k → 20k   (more opportunities)
        #   MIN_VOLUME_5M_USD   3k → 1k     (new tokens have low initial volume)
        #   MIN_HOLDER_COUNT    25 → 10      (new tokens accumulate holders slowly)
        if saved_version < 8:
            config.MIN_LIQUIDITY_USD = 20_000.0
            config.MIN_FDV_USD       = 150_000.0
            config.MIN_MARKET_CAP_USD = 20_000.0
            config.MIN_VOLUME_5M_USD = 1_000.0
            config.MIN_HOLDER_COUNT  = 10
            log.info(
                "Settings migration v7→v8: loosened entry filters "
                "(liq=20k, fdv=150k, mcap=20k, vol5m=1k, holders=10)"
            )

        # Restore bot mode
        if "__is_auto" in data:
            is_auto = bool(data["__is_auto"])
        if "__is_paused" in data:
            is_paused = bool(data["__is_paused"])

        log.info(
            f"Loaded persisted settings (file v{saved_version}, current v{SETTINGS_VERSION}) | "
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
        # Atomic write: write to temp file first, then rename.
        # Prevents corrupted reads if the process is killed mid-write (Railway deploy).
        tmp_path = TRADE_LOG_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(trade_history[-500:], f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, TRADE_LOG_FILE)
    except Exception as e:
        log.error(f"Failed to save trade history: {e}")


def _ensure_history_loaded():
    """Reload trade history from disk if in-memory list is empty.
    Handles the case where a Railway redeploy starts a new instance before
    the old one's in-memory data was available."""
    global trade_history
    if trade_history:
        return
    try:
        if os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE) as f:
                data = json.load(f)
            if data:
                trade_history = data
                log.info(f"History hot-reloaded from disk: {len(data)} entries")
    except Exception as e:
        log.warning(f"History hot-reload failed: {e}")


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
    # Auto-blacklist deployer when a token is confirmed stuck (honeypot)
    if reason == "Honeypot" and pos.deployer_address:
        was_new = blacklist.add(
            pos.deployer_address,
            reason=f"Honeypot: {pos.symbol} ({pos.token_address[:10]}…)",
        )
        if was_new:
            log.warning(f"Deployer {pos.deployer_address[:10]}… auto-blacklisted after {pos.symbol} honeypot")

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


# ── Mempool pre-analysis cache ────────────────────────────────────────────────
# Stores results from mempool-detected pairs so on_pair_found can skip analysis.
# Key: token_address (lowercase) → {"result": check_token result, "ts": time.time()}
_mempool_cache: dict[str, dict] = {}
_MEMPOOL_CACHE_TTL = 120  # seconds — discard stale pre-analysis results


async def on_pending_pair_found(token_address: str, base_token: str, pair_address: str):
    """
    Called from mempool watcher when a pending createPair tx is detected.
    Runs check_token immediately and caches the result.
    When PairCreated event fires, on_pair_found picks up the cached result.
    """
    key = token_address.lower()
    if key in _mempool_cache:
        return  # already analyzing or analyzed

    _mempool_cache[key] = {"result": None, "ts": time.time()}  # placeholder = "in progress"

    try:
        result = await check_token(
            token_address, pair_address, base_token, w3,
            config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
            wallet_address=trader.wallet,
            min_market_cap_usd=config.MIN_MARKET_CAP_USD,
            min_fdv_usd=config.MIN_FDV_USD,
            max_fdv_usd=config.MAX_FDV_USD,
            max_top10_holder_pct=config.MAX_TOP10_HOLDER_PCT,
            min_volume_5m_usd=config.MIN_VOLUME_5M_USD,
            max_token_age_days=config.MAX_TOKEN_AGE_DAYS,
            lp_holder_max_pct=config.LP_HOLDER_MAX_PCT,
            min_holder_count=config.MIN_HOLDER_COUNT,
            bscscan_api_key=config.BSCSCAN_API_KEY,
            max_deployer_tokens_30d=config.MAX_DEPLOYER_TOKENS_30D,
        )
        _mempool_cache[key] = {"result": result, "ts": time.time()}
        status = "OK" if result["ok"] else f"rejected: {result['reason']}"
        log.info(f"Mempool pre-analysis done for {token_address[:10]}…: {status}")
    except Exception as e:
        log.warning(f"Mempool pre-analysis error for {token_address[:10]}…: {e}")
        _mempool_cache.pop(key, None)


# ── New pair handler ──────────────────────────────────────────────────────────

async def on_pair_found(token_address: str, base_token: str, pair_address: str):
    if is_paused:
        log.info(f"Bot paused — skipping {token_address}")
        return

    # Check mempool pre-analysis cache first
    key = token_address.lower()
    cached = _mempool_cache.pop(key, None)
    if cached and cached.get("result") and (time.time() - cached["ts"]) < _MEMPOOL_CACHE_TTL:
        result = cached["result"]
        log.info(f"Using mempool pre-analysis cache for {token_address[:10]}…")
    else:
        log.info(f"Analyzing: {token_address}")
        result = await check_token(
            token_address, pair_address, base_token, w3,
            config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
            wallet_address=trader.wallet,
            min_market_cap_usd=config.MIN_MARKET_CAP_USD,
            min_fdv_usd=config.MIN_FDV_USD,
            max_fdv_usd=config.MAX_FDV_USD,
            max_top10_holder_pct=config.MAX_TOP10_HOLDER_PCT,
            min_volume_5m_usd=config.MIN_VOLUME_5M_USD,
            max_token_age_days=config.MAX_TOKEN_AGE_DAYS,
            lp_holder_max_pct=config.LP_HOLDER_MAX_PCT,
            min_holder_count=config.MIN_HOLDER_COUNT,
            bscscan_api_key=config.BSCSCAN_API_KEY,
            max_deployer_tokens_30d=config.MAX_DEPLOYER_TOKENS_30D,
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

    fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
    text = (
        f"🎯 *Новый токен прошёл все проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💧 Ликвидность: *${info['liquidity_usd']:,.0f}*{fdv_str}\n"
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

        # Prevent concurrent buy of the same token (race between await points)
        if token_address in _buying_tokens:
            log.info(f"Auto: {info['symbol']} already being bought, skipping")
            return
        _buying_tokens.add(token_address)

        try:
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
                moon_bag = _calc_moon_bag(result["tokens_received"], buy_amount, bnb_price)
                tradeable = result["tokens_received"] - moon_bag
                pos = Position(
                    token_address     = token_address,
                    symbol            = info["symbol"],
                    name              = info["name"],
                    pair_address      = pair_address,
                    buy_price_bnb     = entry_price,
                    tokens_amount     = tradeable,
                    decimals          = result["decimals"],
                    buy_bnb           = buy_amount,
                    take_profit_1     = config.TAKE_PROFIT_1,
                    take_profit_1_pct = config.TAKE_PROFIT_1_PCT,
                    trailing_stop_pct = config.TRAILING_STOP_PCT,
                    stop_loss         = config.STOP_LOSS,
                    liquidity_usd     = info["liquidity_usd"],
                    buy_tax           = info["buy_tax"],
                    sell_tax          = info["sell_tax"],
                    moon_bag_tokens   = moon_bag,
                    deployer_address  = info.get("deployer") or "",
                )
                pos_manager.add(pos)
                _record_buy(pos, result["tx_hash"])
                amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
                fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
                moon_str = (
                    f"\n🌙 Moon bag: *{moon_bag / 10**result['decimals']:.2f} {info['symbol']}* "
                    f"({config.MOON_BAG_PCT:.0f}%) — не продаётся авто"
                ) if moon_bag > 0 else ""
                await tg_send(
                    f"✅ *Куплено авто* — {info['symbol']}\n\n"
                    f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                    f"Цена входа: {entry_price:.8f} BNB\n"
                    f"Tx: `{result['tx_hash']}`\n\n"
                    f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%  "
                    f"| Trailing: -{config.TRAILING_STOP_PCT}%  "
                    f"| SL: -{config.STOP_LOSS}%{fdv_str}"
                    f"{moon_str}\n"
                    f"Позиций открыто: {len(pos_manager.positions)}/{max_pos}"
                )
            else:
                await tg_send(f"❌ Авто: ошибка покупки *{info['symbol']}*: {result['reason']}")
        finally:
            _buying_tokens.discard(token_address)
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
            info_bnb_price = token_info["info"].get("bnb_price", 0) or 0
            moon_bag = _calc_moon_bag(result["tokens_received"], buy_amount, info_bnb_price)
            tradeable = result["tokens_received"] - moon_bag
            pos = Position(
                token_address      = token_address,
                symbol             = sym,
                name               = token_info["info"]["name"],
                pair_address       = token_info["pair_address"],
                buy_price_bnb      = entry_price,
                tokens_amount      = tradeable,
                decimals           = result["decimals"],
                buy_bnb            = buy_amount,
                take_profit_1      = config.TAKE_PROFIT_1,
                take_profit_1_pct  = config.TAKE_PROFIT_1_PCT,
                trailing_stop_pct  = config.TRAILING_STOP_PCT,
                stop_loss          = config.STOP_LOSS,
                liquidity_usd      = token_info["info"]["liquidity_usd"],
                buy_tax            = token_info["info"]["buy_tax"],
                sell_tax           = token_info["info"]["sell_tax"],
                moon_bag_tokens    = moon_bag,
                deployer_address   = token_info["info"].get("deployer") or "",
            )
            pos_manager.add(pos)
            _record_buy(pos, result["tx_hash"])

            amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
            moon_str = (
                f"\n🌙 Moon bag: *{moon_bag / 10**result['decimals']:.2f} {sym}* "
                f"({config.MOON_BAG_PCT:.0f}%) — не продаётся авто"
            ) if moon_bag > 0 else ""
            await query.edit_message_text(
                f"✅ *Куплено!* — {sym}\n\n"
                f"Получено: {amount_fmt:.4f} {sym}\n"
                f"Цена входа: {entry_price:.8f} BNB\n"
                f"Tx: `{result['tx_hash']}`\n\n"
                f"TP1: +{config.TAKE_PROFIT_1}% → продать {config.TAKE_PROFIT_1_PCT:.0f}%\n"
                f"Trailing stop: -{config.TRAILING_STOP_PCT}% от пика  |  SL: -{config.STOP_LOSS}%"
                f"{moon_str}",
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
            trader.sell_escalating, token_address, pos.tokens_amount
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
        "*/history* — последние 10 сделок\n\n"
        "💡 Пришли адрес контракта (0x...) — получи полный анализ токена",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Команды бота*\n\n"
        "*Мониторинг*\n"
        "/status — баланс кошелька и текущие настройки\n"
        "/positions — открытые позиции с P&L\n\n"
        "*Анализ токенов*\n"
        "/analyze 0x... — полный анализ токена по адресу\n"
        "_(или просто пришли адрес контракта — бот проанализирует автоматически)_\n\n"
        "*Чёрный список деплоеров*\n"
        "/blacklist — список заблокированных деплоеров\n"
        "/blacklist add 0x... — добавить деплоера вручную\n"
        "/blacklist remove 0x... — удалить из списка\n\n"
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
        "`/set liq 50000` — мин. ликвидность USD\n"
        "`/set mcap 30000` — мин. market cap USD\n"
        "`/set fdvmin 200000` — мин. FDV USD\n"
        "`/set fdvmax 10000000` — макс. FDV USD\n"
        "`/set vol5m 1000` — мин. объём за 5 мин USD\n"
        "`/set age 30` — макс. возраст токена (дней)\n"
        "`/set top10 30` — макс. топ-10 холдеры % (excl. DEX)\n"
        "`/set lp 30` — макс. % LP в одном незаблокированном кошельке\n"
        "`/set holders 50` — мин. кол-во холдеров токена\n"
        "`/set tax 5` — макс. налог buy+sell в %\n"
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
    _ensure_history_loaded()
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
    _ensure_history_loaded()
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
        # Trade sizing
        "pct":    ("BUY_PCT_OF_BALANCE",  0.0,   20.0,  "% баланса на сделку (0 = авто-тир)"),
        "minbuy": ("BUY_MIN_BNB",         0.01,  1.0,   "Мин. сумма сделки BNB"),
        "maxbuy": ("BUY_MAX_BNB",         0.05,  10.0,  "Макс. сумма сделки BNB"),
        # Exit strategy
        "sl":     ("STOP_LOSS",           1.0,   90.0,  "Стоп-лосс %"),
        "tp1":    ("TAKE_PROFIT_1",       5.0,   500.0, "TP1 %"),
        "trail":  ("TRAILING_STOP_PCT",   1.0,   90.0,  "Trailing stop %"),
        # Token quality filters
        "liq":    ("MIN_LIQUIDITY_USD",   500.0, 1e7,   "Мин. ликвидность USD"),
        "mcap":   ("MIN_MARKET_CAP_USD",  1000.0,1e7,   "Мин. market cap USD"),
        "fdvmin": ("MIN_FDV_USD",         1000.0,1e7,   "Мин. FDV USD"),
        "fdvmax": ("MAX_FDV_USD",         1000.0,1e9,   "Макс. FDV USD"),
        "vol5m":  ("MIN_VOLUME_5M_USD",   0.0,   1e6,   "Мин. объём за 5 мин USD"),
        "age":    ("MAX_TOKEN_AGE_DAYS",  1,     365,   "Макс. возраст токена (дней)"),
        "top10":  ("MAX_TOP10_HOLDER_PCT",1.0,   99.0,  "Макс. топ-10 холдеры % (excl. DEX)"),
        "lp":     ("LP_HOLDER_MAX_PCT",  1.0,   99.0,  "Макс. % LP в одном незаблокированном кошельке"),
        "holders":("MIN_HOLDER_COUNT",   1,     10000, "Мин. кол-во холдеров токена"),
        # Taxes and limits
        "tax":    ("MAX_BUY_TAX",         1.0,   50.0,  "Макс. налог %"),
        "max":    ("MAX_POSITIONS",       1,     20,    "Макс. позиций"),
        "gwei":   ("GAS_BUY_GWEI",        0.0,   100.0, "Фикс. gas для покупки (0 = авто)"),
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
    # Integer params
    if attr in ("MAX_POSITIONS", "MAX_TOKEN_AGE_DAYS", "MIN_HOLDER_COUNT"):
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

    moon_str = (
        f"Moon bag: {config.MOON_BAG_PCT:.0f}% при сделке ≥${config.MOON_BAG_MIN_USD:.0f}"
    )

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
        f"*Выход:*\n"
        f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции\n"
        f"Trailing stop: -{config.TRAILING_STOP_PCT}% от пика  |  SL: -{config.STOP_LOSS}%\n"
        f"{moon_str}\n\n"
        f"*Фильтры покупки:*\n"
        f"Ликвидность: ${config.MIN_LIQUIDITY_USD:,.0f} мин\n"
        f"Market cap: ${config.MIN_MARKET_CAP_USD:,.0f} мин\n"
        f"FDV: ${config.MIN_FDV_USD/1000:.0f}k – ${config.MAX_FDV_USD/1000000:.0f}М\n"
        f"Объём 5м: ${config.MIN_VOLUME_5M_USD:,.0f} мин\n"
        f"Возраст: {config.MAX_TOKEN_AGE_DAYS} дней макс\n"
        f"Холдеры: {config.MIN_HOLDER_COUNT} мин\n"
        f"Топ-10 холдеры: {config.MAX_TOP10_HOLDER_PCT:.0f}% макс\n"
        f"LP незаблокирован: {config.LP_HOLDER_MAX_PCT:.0f}% макс на кошелёк\n"
        f"Max tax: {config.MAX_BUY_TAX}% buy / {config.MAX_SELL_TAX}% sell\n"
        f"Деплоер: макс {config.MAX_DEPLOYER_TOKENS_30D} контракт(ов) за 30 дн. "
        f"{'✅ BSCScan OK' if config.BSCSCAN_API_KEY else '⚠️ BSCScan API ключ не задан'}\n\n"
        f"*Исполнение:*\n"
        f"Gas buy: {gas_mode}\n"
        f"Slip buy/sell: {config.SLIPPAGE_BUY}%/{config.SLIPPAGE_SELL}%\n"
        f"Deadline: {config.TX_DEADLINE_SEC}s",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Analyze a token by address. Usage: /analyze 0x... or just paste the address."""
    args = context.args
    if not args:
        await update.message.reply_text(
            "❌ Укажи адрес токена:\n`/analyze 0x...`\n\n"
            "Или просто пришли адрес контракта (42 символа, начинается с 0x).",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await _do_analyze(update, args[0].strip())


@owner_only
async def handle_address_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle plain token address messages (0x... 42 chars) as analyze requests."""
    text = (update.message.text or "").strip()
    if Web3.is_address(text):
        await _do_analyze(update, text)


async def _do_analyze(update, raw_address: str):
    """Core analyze logic shared between command and message handler."""
    if not Web3.is_address(raw_address):
        await update.message.reply_text("❌ Некорректный адрес токена.")
        return

    token_address = Web3.to_checksum_address(raw_address)
    api_sources = "GoPlus + Honeypot.is + DexScreener + on-chain"
    if config.BSCSCAN_API_KEY:
        api_sources += " + BSCScan деплоер"
    wait_msg = await update.message.reply_text(
        f"🔍 *Анализирую токен...*\n_({api_sources})_",
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        data = await analyze_token(token_address, w3, trader.wallet, bscscan_api_key=config.BSCSCAN_API_KEY)
    except Exception as e:
        log.error(f"analyze_token error: {e}")
        await wait_msg.edit_text(f"❌ Ошибка анализа: {e}")
        return

    if not data["found"]:
        await wait_msg.edit_text(f"❌ {data['reason']}")
        return

    # ── Format report ──────────────────────────────────────────────────────────
    sym  = data["symbol"]
    name = data["name"]

    # Liquidity line
    liq_str = f"${data['liquidity_usd']:,.0f}" if data["liquidity_usd"] else "неизвестно"
    liq_ok  = data["liquidity_usd"] >= config.MIN_LIQUIDITY_USD if data["liquidity_usd"] else None
    liq_icon = ("✅" if liq_ok else "❌") if liq_ok is not None else "❓"

    # FDV line
    fdv_str  = f"${data['fdv_usd']:,.0f}" if data["fdv_usd"] else "неизвестно"
    fdv_ok   = config.MIN_FDV_USD <= data["fdv_usd"] <= config.MAX_FDV_USD if data["fdv_usd"] else None
    fdv_icon = ("✅" if fdv_ok else "❌") if fdv_ok is not None else "❓"

    # Age line
    if data["age_days"] is not None:
        age_str  = f"{data['age_days']:.0f} дней" if data["age_days"] >= 1 else f"{data['age_days'] * 24:.0f} часов"
        age_ok   = data["age_days"] <= config.MAX_TOKEN_AGE_DAYS
        age_icon = "✅" if age_ok else "❌"
    else:
        age_str  = "не проиндексирован"
        age_icon = "❓"

    # Volume line
    if data["vol_5m"] is not None:
        vol_str  = f"${data['vol_5m']:,.0f}"
        vol_ok   = data["vol_5m"] >= config.MIN_VOLUME_5M_USD
        vol_icon = "✅" if vol_ok else "❌"
    else:
        vol_str  = "нет данных"
        vol_icon = "❓"

    # Tax lines — prefer GoPlus data; fall back to honeypot.is simulation
    gp_buy_tax   = data["buy_tax"]
    gp_sell_tax  = data["sell_tax"]
    hp_buy_tax   = data.get("hp_buy_tax")
    hp_sell_tax  = data.get("hp_sell_tax")

    # Use GoPlus if indexed, else honeypot.is simulation (works at listing time)
    eff_buy_tax  = gp_buy_tax  if data["goplus_ok"] and gp_buy_tax  > 0 else (hp_buy_tax  or 0.0)
    eff_sell_tax = gp_sell_tax if data["goplus_ok"] and gp_sell_tax > 0 else (hp_sell_tax or 0.0)
    tax_source   = "(GoPlus)" if data["goplus_ok"] else ("(honeypot.is симуляция)" if hp_buy_tax is not None else "(нет данных)")

    tax_buy_ok  = eff_buy_tax  <= config.MAX_BUY_TAX
    tax_sell_ok = eff_sell_tax <= config.MAX_SELL_TAX
    tax_buy_icon  = "✅" if tax_buy_ok  else "❌"
    tax_sell_icon = "✅" if tax_sell_ok else "❌"

    # Top-10 holders
    top10 = data["top10_pct"]
    top10_ok   = top10 <= config.MAX_TOP10_HOLDER_PCT
    top10_icon = "✅" if top10_ok else "❌"

    # LP lock
    lp_locked = data["lp_locked"]
    if lp_locked is True:
        lp_str = "✅ Заблокирован"
    elif lp_locked is False:
        lp_str = "❌ Не заблокирован (rug risk!)"
    else:
        lp_str = "❓ Данных нет"

    # Simulation checks
    buy_sim_icon  = "✅" if data["sim_buy_ok"]  else "❌"
    sell_sim_icon = "✅" if data["sim_sell_ok"] else "❌"
    hp_icon       = "✅" if data["hp_is_ok"]    else "❌"
    gp_icon       = "✅" if data["goplus_ok"]   else "⚠️"

    # Critical flags block
    if data["critical_flags"]:
        flags_block = "\n".join(f"🚨 {v}" for v in data["critical_flags"].values())
    else:
        flags_block = "✅ Критических флагов нет"

    # Warnings block
    if data["warnings"]:
        warn_block = "\n".join(f"⚠️ {w}" for w in data["warnings"])
    else:
        warn_block = "✅ Предупреждений нет"

    # ── Recommendation ─────────────────────────────────────────────────────────
    red_flags = []
    if data["critical_flags"]:
        red_flags.append(f"критические GoPlus-флаги: {', '.join(data['critical_flags'].values())}")
    if not data["sim_buy_ok"]:
        red_flags.append(f"покупка отклонена симуляцией: {data['sim_buy_reason']}")
    if not data["sim_sell_ok"]:
        red_flags.append(f"продажа отклонена симуляцией: {data['sim_sell_reason']}")
    if not data["hp_is_ok"]:
        red_flags.append(f"honeypot.is: {data['hp_is_reason']}")
    if liq_ok is False:
        red_flags.append(f"ликвидность ${data['liquidity_usd']:,.0f} < минимума")
    if fdv_ok is False:
        red_flags.append(f"FDV вне диапазона")
    if age_icon == "❌":
        red_flags.append(f"токен слишком старый ({age_str})")
    if vol_icon == "❌":
        red_flags.append(f"объём за 5м слишком низкий ({vol_str})")
    if not tax_buy_ok:
        red_flags.append(f"buy tax {eff_buy_tax:.1f}% > {config.MAX_BUY_TAX}% {tax_source}")
    if not tax_sell_ok:
        red_flags.append(f"sell tax {eff_sell_tax:.1f}% > {config.MAX_SELL_TAX}% {tax_source}")
    if not top10_ok:
        red_flags.append(f"топ-10 холдеры {top10:.1f}% > {config.MAX_TOP10_HOLDER_PCT}%")
    if lp_locked is False:
        red_flags.append("ликвидность не заблокирована")
    if not data.get("deployer_ok", True):
        red_flags.append(data.get("deployer_reason", "серийный деплоер"))

    caution_flags = list(data["warnings"])
    if lp_locked is None:
        caution_flags.append("LP-холдеры не проиндексированы")
    if not data["goplus_ok"]:
        caution_flags.append("GoPlus недоступен")

    if red_flags:
        rec_icon = "🔴"
        rec_text = "НЕ ПОКУПАТЬ"
        rec_detail = "Причины: " + "; ".join(red_flags[:3])
    elif caution_flags:
        rec_icon = "🟡"
        rec_text = "ОСТОРОЖНО"
        rec_detail = "Риски: " + "; ".join(caution_flags[:3])
    else:
        rec_icon = "🟢"
        rec_text = "ПРОШЁЛ ВСЕ ФИЛЬТРЫ"
        rec_detail = "Токен соответствует всем условиям покупки"

    base_sym = "BNB" if data["base_token"].lower() == config.WBNB.lower() else (
        "BUSD" if data["base_token"].lower() == config.BUSD.lower() else "USDT"
    )

    # Deployer block
    deployer_addr = data.get("deployer")
    deploy_count  = data.get("deploy_count_30d")
    if deployer_addr:
        deployer_short = deployer_addr[:6] + "…" + deployer_addr[-4:]
        if deploy_count is not None:
            deploy_icon = "❌" if not data.get("deployer_ok", True) else ("⚠️" if deploy_count >= 2 else "✅")
            deployer_line = (
                f"{deploy_icon} Деплоер: `{deployer_short}` "
                f"| {deploy_count} контракт(ов) за 30 дн."
            )
        else:
            deployer_line = f"✅ Деплоер: `{deployer_short}`"
    elif config.BSCSCAN_API_KEY:
        deployer_line = "❓ Деплоер: не найден в BSCScan"
    else:
        deployer_line = "➖ Деплоер: BSCScan API ключ не задан"

    text = (
        f"🔍 *Анализ токена*\n\n"
        f"🪙 *{name}* (`{sym}`)\n"
        f"📄 `{token_address}`\n"
        f"🔗 Пара: {base_sym}/PancakeSwap V2\n\n"
        f"*📊 Метрики:*\n"
        f"{liq_icon} Ликвидность: *{liq_str}* (мин ${config.MIN_LIQUIDITY_USD:,.0f})\n"
        f"{fdv_icon} FDV: *{fdv_str}* (${config.MIN_FDV_USD/1000:.0f}k–${config.MAX_FDV_USD/1000000:.0f}М)\n"
        f"{age_icon} Возраст пары: *{age_str}* (макс {config.MAX_TOKEN_AGE_DAYS} дн.)\n"
        f"{vol_icon} Объём 5м: *{vol_str}* (мин ${config.MIN_VOLUME_5M_USD:,.0f})\n"
        f"👥 Холдеры: *{data['holder_count']}*\n\n"
        f"*💸 Налоги* {tax_source}:\n"
        f"{tax_buy_icon} Buy tax: *{eff_buy_tax:.1f}%* (макс {config.MAX_BUY_TAX}%)\n"
        f"{tax_sell_icon} Sell tax: *{eff_sell_tax:.1f}%* (макс {config.MAX_SELL_TAX}%)\n\n"
        f"*🧑‍🤝‍🧑 Концентрация:*\n"
        f"{top10_icon} Топ-10 холдеры: *{top10:.1f}%* (макс {config.MAX_TOP10_HOLDER_PCT}%)\n"
        f"🔒 LP: {lp_str}\n\n"
        f"*🛡 Безопасность:*\n"
        f"{buy_sim_icon} Buy-симуляция: {'OK' if data['sim_buy_ok'] else data['sim_buy_reason']}\n"
        f"{sell_sim_icon} Sell-симуляция: {'OK' if data['sim_sell_ok'] else data['sim_sell_reason']}\n"
        f"{hp_icon} Honeypot.is: {'OK' if data['hp_is_ok'] else data['hp_is_reason']}\n"
        f"{gp_icon} GoPlus: {'доступен' if data['goplus_ok'] else 'недоступен'}\n"
        f"{deployer_line}\n\n"
        f"*🚩 GoPlus флаги:*\n{flags_block}\n\n"
        f"*⚠️ Предупреждения:*\n{warn_block}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"{rec_icon} *Рекомендация: {rec_text}*\n"
        f"_{rec_detail}_"
    )

    await wait_msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)


# ── Deployer blacklist commands ──────────────────────────────────────────────

@owner_only
async def cmd_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manage the deployer blacklist.
      /blacklist          — show list
      /blacklist add 0x…  — add deployer
      /blacklist remove 0x… — remove deployer
    """
    args = context.args or []

    # /blacklist (no args) — show current list
    if not args:
        bl = blacklist.get_all()
        if not bl:
            await update.message.reply_text("📋 Чёрный список деплоеров пуст.")
            return
        lines = []
        for addr, info in bl.items():
            short = addr[:6] + "…" + addr[-4:]
            reason = info.get("reason") or "—"
            hits = info.get("hits", 1)
            lines.append(f"`{short}` | {hits}x | {reason}")
        text = f"🚫 *Чёрный список деплоеров* ({len(bl)}):\n\n" + "\n".join(lines)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        return

    action = args[0].lower()

    # /blacklist add 0x...
    if action == "add" and len(args) >= 2:
        addr = args[1].strip()
        if not Web3.is_address(addr):
            await update.message.reply_text("❌ Некорректный адрес.")
            return
        reason = " ".join(args[2:]) if len(args) > 2 else "добавлен вручную"
        was_new = blacklist.add(addr, reason=reason)
        status = "✅ Добавлен" if was_new else "⚠️ Уже в списке (счётчик обновлён)"
        await update.message.reply_text(
            f"{status}: `{addr[:10]}…`\nПричина: {reason}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # /blacklist remove 0x...
    if action in ("remove", "del", "rm") and len(args) >= 2:
        addr = args[1].strip()
        removed = blacklist.remove(addr)
        if removed:
            await update.message.reply_text(f"✅ Удалён из чёрного списка: `{addr[:10]}…`", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Адрес не найден в чёрном списке.")
        return

    await update.message.reply_text(
        "❓ Использование:\n"
        "`/blacklist` — показать список\n"
        "`/blacklist add 0x…` — добавить\n"
        "`/blacklist remove 0x…` — удалить",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Background: remove expired pending alerts ────────────────────────────────

async def _cleanup_pending():
    """Remove expired pending alerts and stale mempool cache entries every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        now = time.time()

        # Clean up expired pending token alerts (manual mode buttons)
        expired = [k for k, v in pending.items() if now - v["ts"] > config.PENDING_TTL]
        for k in expired:
            pending.pop(k, None)
        if expired:
            log.info(f"Cleaned up {len(expired)} expired pending token(s)")

        # Clean up stale mempool pre-analysis cache entries
        stale = [k for k, v in _mempool_cache.items()
                 if now - v.get("ts", 0) > _MEMPOOL_CACHE_TTL]
        for k in stale:
            _mempool_cache.pop(k, None)
        if stale:
            log.info(f"Cleaned up {len(stale)} stale mempool cache entries")


# ── Background: daily report at 23:00 MSK ────────────────────────────────────

DAILY_REPORT_SENTINEL = os.path.join(DATA_DIR, "tysmith_last_report_date.txt")


def _daily_report_already_sent(today: str) -> bool:
    """Check sentinel file to prevent duplicate reports during Railway redeploys."""
    try:
        if os.path.exists(DAILY_REPORT_SENTINEL):
            with open(DAILY_REPORT_SENTINEL) as f:
                return f.read().strip() == today
    except Exception:
        pass
    return False


def _mark_daily_report_sent(today: str):
    try:
        with open(DAILY_REPORT_SENTINEL, "w") as f:
            f.write(today)
    except Exception as e:
        log.warning(f"Failed to write daily report sentinel: {e}")


async def _daily_report():
    """Send trading summary every day at 23:00 Moscow time."""
    while True:
        now_msk   = datetime.now(MOSCOW_TZ)
        target    = now_msk.replace(hour=23, minute=0, second=0, microsecond=0)
        if now_msk >= target:
            from datetime import timedelta
            target = target + timedelta(days=1)
        wait_sec  = (target - now_msk).total_seconds()
        log.info(f"Daily report scheduled in {wait_sec/3600:.1f}h")
        await asyncio.sleep(wait_sec)

        today  = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")

        # Prevent duplicate reports when two instances overlap during deploy
        if _daily_report_already_sent(today):
            log.info(f"Daily report for {today} already sent — skipping")
            continue

        _ensure_history_loaded()
        trades = [t for t in trade_history if t.get("closed_at", "").startswith(today)]

        try:
            bnb_price = await get_bnb_price(w3)
            balance   = w3.eth.get_balance(trader.wallet) / 1e18

            if trades:
                report = _build_stats_report(trades, bnb_price,
                                             f"Итоги дня {today}")
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

            _mark_daily_report_sent(today)
        except Exception as e:
            log.error(f"Daily report error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

# Distributed lock on shared /data volume — prevents two Railway containers
# from running simultaneously during blue-green deploys.
#
# Protocol:
#   1. New instance waits until the lock is stale (old instance stopped refreshing).
#   2. New instance writes its own hostname into the lock file and starts.
#   3. Old instance's _lock_refresher detects a foreign hostname in the lock →
#      gracefully shuts itself down (Telegram polling stop + sys.exit).
#
# This guarantees only ONE instance processes Telegram updates at any time.
DIST_LOCK_FILE   = os.path.join(DATA_DIR, "tysmith.lock")
LOCK_EXPIRY_SEC  = 20   # lock considered stale if not refreshed within this time
LOCK_REFRESH_SEC = 7    # how often the running instance refreshes its lock
LOCK_WAIT_SEC    = 45   # how long new instance waits before force-taking the lock


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


async def _lock_refresher(app):
    """
    Keep the distributed lock alive.

    On every refresh cycle, read the lock file first.
    If another instance (different hostname) has written a fresh lock,
    that means a new deployment took over → gracefully shut this instance down.
    This prevents the double-bot problem during Railway blue-green deploys.
    """
    my_host = socket.gethostname()
    while True:
        await asyncio.sleep(LOCK_REFRESH_SEC)

        data      = _read_lock()
        lock_host = data.get("host", "")
        lock_age  = time.time() - data.get("ts", 0)

        if lock_host and lock_host != my_host and lock_age < LOCK_EXPIRY_SEC:
            # A newer instance has taken the lock — we are the old one, must exit.
            log.warning(
                f"Lock taken by new instance ({lock_host}) — "
                f"this instance ({my_host}) is shutting down to avoid duplication"
            )
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception as e:
                log.error(f"Shutdown error: {e}")
            finally:
                sys.exit(0)

        _write_lock()


async def main():
    _load_history()
    _load_settings()
    blacklist.load()
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
    app.add_handler(CommandHandler("analyze",   cmd_analyze))
    app.add_handler(CommandHandler("blacklist", cmd_blacklist))
    app.add_handler(CallbackQueryHandler(handle_callback))
    # Handle plain token addresses sent as messages (0x... 42 chars)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r"^0x[0-9a-fA-F]{40}$"),
        handle_address_message,
    ))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    asyncio.create_task(pos_manager.monitor())
    asyncio.create_task(watch_pairs(config.BSC_WS_RPC, on_pair_found))
    asyncio.create_task(_cleanup_pending())
    asyncio.create_task(_daily_report())

    if config.MEMPOOL_ENABLED:
        asyncio.create_task(watch_pending_pairs(config.BSC_WS_RPC, on_pending_pair_found))
        log.info("Mempool monitoring enabled — pre-analyzing pending createPair txs")
    asyncio.create_task(_lock_refresher(app))

    # Restore open positions from disk (survive restarts/redeploys)
    restored = pos_manager.load()

    # Clean up only true "zombie" positions on startup:
    # buy_price_bnb == 0 or tokens_amount == 0 → unmonitorable, can never trigger SL/TP.
    # Stuck (honeypot) positions are kept — they show in /positions with the write-off
    # button so the user can see what happened and act manually.
    zombie_names = []
    for addr in list(pos_manager.positions):
        pos = pos_manager.positions[addr]
        if pos.buy_price_bnb <= 0 or pos.tokens_amount <= 0:
            log.warning(
                f"Startup: removing zombie position {pos.symbol} "
                f"(price={pos.buy_price_bnb}, amount={pos.tokens_amount})"
            )
            _record_trade(pos, pnl_pct=0.0, reason="Invalid (нет данных)", sell_price=0.0)
            pos_manager.remove(addr)
            zombie_names.append(pos.symbol)

    # Use post-cleanup count so the message matches what /positions actually shows
    active_count = len(pos_manager.positions)
    stuck_count  = sum(1 for p in pos_manager.get_all() if p.stuck)

    log.info(f"Ready. Wallet: {trader.wallet}")
    bl_count    = blacklist.count()
    bl_note     = f" | 🚫 Blacklist: {bl_count}" if bl_count else ""
    startup_msg = (
        "🚀 *Sniper Bot запущен*\n"
        f"Слежу за новыми парами на PancakeSwap V2 (BSC)...{bl_note}\n\n"
        "/help — все команды"
    )
    if active_count:
        startup_msg += f"\n\n♻️ Восстановлено позиций: *{active_count}* — мониторинг возобновлён"
        if stuck_count:
            startup_msg += (
                f"\n⚠️ Из них заблокированы (honeypot): *{stuck_count}* — "
                f"используй /positions → 🗑 Убрать в историю"
            )
    if zombie_names:
        startup_msg += (
            f"\n\n🗑 Удалены невалидные позиции (нет цены входа): *{', '.join(zombie_names)}*"
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
