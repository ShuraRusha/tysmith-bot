import asyncio
import collections
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
from analyzer import analyze_token, check_token, check_token_fast, fetch_security_partial, get_bnb_price, _get_liquidity_usd_sync as _liq_sync, _get_bnb_price_sync as _bnb_price_sync, _simulate_buy_sync
from position import (
    Position, PositionManager,
    POSITIONS_FILE_BASE, POSITIONS_FILE_BISWAP, POSITIONS_FILE_BASESWAP,
)
from trader import Trader
from watcher import watch_pairs, watch_pending_pairs, PAIR_CREATED_TOPIC, MemPoolNotSupportedError, _seen_pairs as _ws_seen_pairs, _seen_lock as _ws_seen_lock, _ws_endpoint_status

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

# ── Poll w3 instances — one per HTTP RPC, used by _poll_new_pairs fallback ────
# Pre-created at startup so polling can rotate without repeated object creation.
_poll_w3s: list = [_make_w3(url) for url in config.BSC_HTTP_RPCS]
log.info(f"Poll w3 pool: {len(_poll_w3s)} HTTP RPC endpoints")

# ── Base chain setup (optional) ───────────────────────────────────────────────
w3_base     = None
trader_base = None
_base_chain_rpc_failed = False  # True = configured but all RPCs failed
if config.BASE_CHAIN_ENABLED:
    for rpc_url in config.BASE_HTTP_RPCS:
        try:
            _wb = _make_w3(rpc_url)
            # Try up to 2 times per endpoint (cold-start latency on Railway)
            _ok = _wb.is_connected()
            if not _ok:
                import time as _t; _t.sleep(2)
                _ok = _wb.is_connected()
            if _ok:
                w3_base = _wb
                log.info(f"Base chain connected: {rpc_url[:50]}...")
                break
            log.warning(f"Base RPC unavailable: {rpc_url[:50]}...")
        except Exception as _e:
            log.warning(f"Base RPC error {rpc_url[:50]}: {_e}")
    if w3_base is None:
        log.error(
            "All Base RPCs failed — Base chain DISABLED. "
            "Set BASE_HTTP_RPC to a reliable endpoint (e.g. Alchemy free tier) "
            "and redeploy."
        )
        _base_chain_rpc_failed = True
        config.BASE_CHAIN_ENABLED = False
    else:
        trader_base = Trader(
            w3_base,
            config.PRIVATE_KEY,
            config.GAS_MULTIPLIER,
            chain_id=config.BASE_CHAIN_ID,
            router_address=config.UNISWAP_V2_ROUTER_BASE,
            native_token=config.WETH_BASE,
        )
        log.info(f"Base chain trader ready. Wallet: {trader_base.wallet}")

# ── BiSwap V2 (BSC) setup (optional) ─────────────────────────────────────────
trader_biswap   = None
if config.BISWAP_ENABLED:
    trader_biswap = Trader(
        w3, config.PRIVATE_KEY, config.GAS_MULTIPLIER,
        router_address=config.BISWAP_ROUTER,
        native_token=config.WBNB,
    )
    log.info(f"BiSwap trader ready. Router: {config.BISWAP_ROUTER[:10]}…")

# ── BaseSwap V2 (Base) setup (optional) ───────────────────────────────────────
trader_baseswap = None
if config.BASESWAP_ENABLED and config.BASE_CHAIN_ENABLED and w3_base:
    trader_baseswap = Trader(
        w3_base, config.PRIVATE_KEY, config.GAS_MULTIPLIER,
        chain_id=config.BASE_CHAIN_ID,
        router_address=config.BASESWAP_ROUTER_BASE,
        native_token=config.WETH_BASE,
    )
    log.info(f"BaseSwap trader ready. Router: {config.BASESWAP_ROUTER_BASE[:10]}…")

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


# Throttle low-balance alerts to at most one per 5 minutes
_low_balance_alert_ts: float = 0.0
_LOW_BALANCE_ALERT_INTERVAL = 300.0


async def _maybe_alert_low_balance(balance: float, chain: str = "BSC"):
    global _low_balance_alert_ts
    # Never alert if balance is zero — user just hasn't funded this chain wallet
    if balance == 0.0:
        return
    now = time.time()
    if now - _low_balance_alert_ts < _LOW_BALANCE_ALERT_INTERVAL:
        return
    _low_balance_alert_ts = now
    native = "ETH" if chain != "BSC" else "BNB"
    min_needed = (config.BUY_MIN_BNB / (config.BUY_PCT_OF_BALANCE / 100.0)
                  if config.BUY_PCT_OF_BALANCE > 0
                  else config.BUY_MIN_BNB / 0.05) + config.GAS_RESERVE_BNB
    await tg_send(
        f"⚠️ [{chain}] Баланс слишком мал для торговли!\n"
        f"Текущий баланс: {balance:.4f} {native}\n"
        f"Минимально нужно: ~{min_needed:.3f} {native}\n"
        f"Пополните кошелёк или снизьте BUY_MIN_BNB в настройках."
    )


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

# Activity tracking — visibility into what the bot is seeing
_stats_seen    = 0          # total pairs detected since start
_stats_rejected = 0         # rejected by any filter
_last_seen_ts:  float = 0.0 # timestamp of last detected pair
_last_reject:   str   = ""  # reason of last rejection
_last_pair_token: str = ""  # token address of last detected pair (for /debug)
_reject_log: list[dict] = []  # last 20 rejections with token + reason
_MAX_REJECT_LOG = 20

# Rolling window of pair event timestamps — used to compute pairs/hour in /debug
_pair_event_times: collections.deque = collections.deque(maxlen=500)

# DEBUG_ALERTS — send Telegram notification for every rejected token (verbose)
DEBUG_ALERTS = os.getenv("DEBUG_ALERTS", "false").lower() == "true"

# Speed/competition tracking — how many blocks after listing the bot buys
_stats_delta_blocks: list[int] = []  # rolling window of last 20 buys

# Circuit breaker — auto-pause after N consecutive losing trades
CIRCUIT_BREAKER_LOSSES = int(os.getenv("CIRCUIT_BREAKER_LOSSES", "10"))
_consecutive_losses: int = 0  # reset to 0 on any profitable close

# ── Persistent analytics (survives restarts) ──────────────────────────────────
# All-time rejection counts by category, stored to disk on each update.
ANALYTICS_FILE = "/data/tysmith_analytics.json"
_analytics: dict = {
    "total_seen":     0,   # all-time pairs detected
    "total_rejected": 0,   # all-time pairs rejected
    "rejection_counts": {}, # category → count (all-time)
}

_ANALYTICS_SAVE_EVERY = 10  # write to disk every N rejections (avoid I/O flood)
_analytics_unsaved = 0       # counter since last save


def _categorize_rejection(reason: str) -> str:
    """Map a raw rejection reason string to a short category label."""
    r = reason.lower()
    if "ликвидность" in r:                         return "Ликвидность"
    if "market cap" in r:                           return "Market cap"
    if "fdv" in r:                                  return "FDV"
    if "блокирует продажу" in r or "sell simulation" in r: return "Блокировка продажи"
    if "honeypot" in r:                             return "Honeypot"
    if "sell tax" in r:                             return "Sell tax"
    if "buy tax" in r:                              return "Buy tax"
    if "rug риск" in r or "lp" in r:               return "LP не заблокирован"
    if "топ-10 холдеров" in r:                      return "Концентрация холдеров"
    if "деплоер в чёрном" in r:                    return "Деплоер (блеклист)"
    if "серийный" in r or "контракт" in r:         return "Серийный деплоер"
    if "слишком старый" in r or "возраст" in r:    return "Возраст токена"
    if "объём" in r or "volume" in r:              return "Объём 5м"
    if "холдеров слишком мало" in r:               return "Мало холдеров"
    if "симуляция" in r:                           return "Симуляция (ошибка)"
    if "selfdestruct" in r:                        return "Selfdestruct"
    if "blacklist" in r or "заморозить" in r:      return "Blacklist функция"
    if "ownership" in r:                           return "Ownership риск"
    if "нет ликвидности" in r:                    return "Нет ликвидности"
    if "цена нативного" in r:                     return "Ошибка RPC"
    return "Другое"


def _track_rejection(reason: str) -> None:
    """Increment persistent rejection counter for this reason category."""
    global _analytics_unsaved
    cat = _categorize_rejection(reason)
    _analytics["total_rejected"] = _analytics.get("total_rejected", 0) + 1
    counts = _analytics.setdefault("rejection_counts", {})
    counts[cat] = counts.get(cat, 0) + 1
    _analytics_unsaved += 1
    if _analytics_unsaved >= _ANALYTICS_SAVE_EVERY:
        _save_analytics()
        _analytics_unsaved = 0


def _save_analytics() -> None:
    try:
        with open(ANALYTICS_FILE, "w") as f:
            json.dump(_analytics, f)
    except Exception as e:
        log.warning(f"Could not save analytics: {e}")


def _load_analytics() -> None:
    global _analytics
    try:
        with open(ANALYTICS_FILE, "r") as f:
            data = json.load(f)
        _analytics.update(data)
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"Could not load analytics: {e}")

# ── Cross-DEX token deduplication ─────────────────────────────────────────────
# Prevents the same token from being processed by multiple DEX watchers
# simultaneously (e.g. PancakeSwap + BiSwap both list the same token).
# Uses token address as key, not pair address (pairs differ per DEX).
_seen_tokens_ts: dict[str, float] = {}
_CROSS_DEX_TOKEN_TTL = 120  # seconds — window to consider a token "already handled"


def _is_token_duplicate(token_address: str, dex_label: str = "") -> bool:
    """
    Returns True if this token was already seen by another DEX watcher recently.
    Marks it as seen and returns False if this is the first time.
    Thread-safe for asyncio (single-threaded event loop, no await inside).
    """
    key = token_address.lower()
    now = time.time()
    # One-time bypass for wait-for-liquidity retries.
    # Avoids popping from _seen_tokens_ts (which created re-detection windows).
    if key in _liq_retry_bypass:
        _liq_retry_bypass.discard(key)
        _seen_tokens_ts[key] = now  # refresh dedup TTL after retry
        return False
    last_seen = _seen_tokens_ts.get(key, 0)
    if now - last_seen < _CROSS_DEX_TOKEN_TTL:
        log.info(f"{dex_label} skipping {key[:10]}… — already handled by another DEX watcher")
        return True
    _seen_tokens_ts[key] = now
    # Prune old entries to avoid unbounded growth
    if len(_seen_tokens_ts) > 2000:
        cutoff = now - _CROSS_DEX_TOKEN_TTL
        stale = [k for k, v in _seen_tokens_ts.items() if v < cutoff]
        for k in stale:
            del _seen_tokens_ts[k]
    return False


# ── Settings persistence ──────────────────────────────────────────────────────

# Increment when adding new persistent params or changing hardcoded defaults.
# Used to migrate old settings files that pre-date a change.
SETTINGS_VERSION = 23

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

        # Migration v8 → v9: fix filters that block ALL fresh listings
        #   MIN_HOLDER_COUNT  10 → 0  (disabled: sniper enters first, 1-3 holders is normal)
        #   MIN_FDV_USD      150k → 50k  (tokens at $63-70k FDV were blocked; real safety = sim)
        #   MIN_MARKET_CAP_USD 20k → 10k (same reasoning)
        #   MIN_LIQUIDITY_USD  20k → 15k ($18.5k pair was blocked by $1.4k)
        #   MIN_VOLUME_5M_USD  1k → 500  (fresh tokens haven't had 5 min to accumulate volume)
        if saved_version < 9:
            config.MIN_HOLDER_COUNT   = 0
            config.MIN_FDV_USD        = 50_000.0
            config.MIN_MARKET_CAP_USD = 10_000.0
            config.MIN_LIQUIDITY_USD  = 15_000.0
            config.MIN_VOLUME_5M_USD  = 500.0
            log.info(
                "Settings migration v8→v9: tuned for fresh listing sniper "
                "(holders=0, fdv=50k, mcap=10k, liq=15k, vol5m=500)"
            )

        # Migration v9 → v10: loosen tax filters + force auto mode on
        #   MAX_BUY_TAX   5% → 10%  (too strict — normal tokens blocked, honeypot.is catches real ones)
        #   MAX_SELL_TAX  5% → 10%  (same reasoning)
        #   MIN_LIQUIDITY_USD 15k → 10k (more pairs qualify)
        #   is_auto = True  (default was off — always start in auto mode now)
        if saved_version < 10:
            config.MAX_BUY_TAX        = 10.0
            config.MAX_SELL_TAX       = 10.0
            config.MIN_LIQUIDITY_USD  = 5_000.0   # was 10k in original migration — fixed
            is_auto = True   # force auto mode on — that's the whole point of a sniper bot
            log.info(
                "Settings migration v9→v10: tax 5%→10%, liq→5k, auto mode ON"
            )

        # Migration v10 → v11: ensure MIN_LIQUIDITY_USD = 5k regardless of what was saved
        # (v9→v10 originally had 10k by mistake, this corrects any file that has that value)
        if saved_version < 12:
            config.MIN_LIQUIDITY_USD = 5_000.0

        # Migration v12 → v13: loosen filters that were blocking most tokens
        #   MAX_DEPLOYER_TOKENS_30D  3 → 8   (активный разработчик ≠ скамер)
        #   MIN_FDV_USD          50k → 10k   (новые токены стартуют с малого FDV)
        #   LP_HOLDER_MAX_PCT      30 → 50   (деплоер держит LP до блокировки)
        #   is_anti_whale/trading_cooldown больше не блокируют — только предупреждение
        if saved_version < 13:
            config.MAX_DEPLOYER_TOKENS_30D = 8
            config.MIN_FDV_USD             = 10_000.0
            config.LP_HOLDER_MAX_PCT       = 50.0
            log.info(
                "Settings migration v12→v13: deployer_30d=8, fdv=10k, lp_holder=50%, "
                "anti_whale/cooldown → предупреждение"
            )

        # Migration v13 → v14: агрессивная настройка для максимального количества сделок
        #   can_take_back_ownership / transfer_pausable → предупреждение (были блоком)
        #   MIN_LIQUIDITY_USD     5k → 3k   (ранний вход в малоликвидные пары)
        #   MIN_FDV_USD          10k → 5k   (ещё раньше при листинге)
        #   MAX_TOP10_HOLDER_PCT  30 → 60   (новые токены = концентрированное распределение)
        #   TOP_HOLDER_MAX_PCT    30 → 50   (один кошелёк может держать больше у старта)
        #   LP_HOLDER_MAX_PCT     50 → 70   (деплоер не всегда успел залочить LP)
        #   MIN_VOLUME_5M_USD    500 → 0    (снайпер входит в первые секунды — объёма нет)
        #   MAX_DEPLOYER_TOKENS_30D 8 → 15  (активные разработчики запускают часто)
        #   BUY_MIN_BNB         0.02 → 0.01 (меньший минимум — работает при малом балансе)
        if saved_version < 14:
            config.MIN_LIQUIDITY_USD       = 3_000.0
            config.MIN_FDV_USD             = 5_000.0
            config.MAX_TOP10_HOLDER_PCT    = 60.0
            config.TOP_HOLDER_MAX_PCT      = 50.0
            config.LP_HOLDER_MAX_PCT       = 70.0
            config.MIN_VOLUME_5M_USD       = 0.0
            config.MAX_DEPLOYER_TOKENS_30D = 15
            config.BUY_MIN_BNB             = 0.01
            log.info(
                "Settings migration v13→v14: liq=3k, fdv=5k, top10=60%, lp=70%, "
                "vol5m=0 (off), deployer=15, buy_min=0.01 BNB; "
                "can_take_back_ownership + transfer_pausable → предупреждение"
            )

        # Migration v14 → v15: увеличен газ для более быстрого попадания в блок
        #   GAS_BUY_GWEI  5 → 10  (конкурируем с другими ботами за место в блоке)
        if saved_version < 15:
            config.GAS_BUY_GWEI = 10.0
            log.info("Settings migration v14→v15: GAS_BUY_GWEI 5 → 10")

        # Migration v15 → v16: MIN_FDV_USD отключён (0)
        #   Симуляция buy/sell + LP check = достаточная защита
        #   FDV фильтр режет легитимные токены на старте листинга
        if saved_version < 16:
            config.MIN_FDV_USD = 0.0
            log.info("Settings migration v15→v16: MIN_FDV_USD → 0 (отключён)")

        # Migration v16 → v17: снайпер-настройки для T+0 листинга
        #   MIN_LIQUIDITY_USD      3000 → 2000  (нижний порог — больше пар проходит)
        #   MIN_MARKET_CAP_USD    10000 → 1000  (FDV на T+0 всегда мал — не показатель качества)
        #   LP_HOLDER_MAX_PCT        70 → 100   (100 = выкл.: деплоер держит 100% LP при листинге)
        #   MAX_DEPLOYER_TOKENS_30D  15 → 20    (немного мягче для активных разработчиков)
        if saved_version < 17:
            config.MIN_LIQUIDITY_USD       = 2_000.0
            config.MIN_MARKET_CAP_USD      = 1_000.0
            config.LP_HOLDER_MAX_PCT       = 100.0
            config.MAX_DEPLOYER_TOKENS_30D = 20
            log.info(
                "Settings migration v16→v17: liq=2k, mcap=1k, lp_holder=100 (выкл.), deployer=20"
            )

        # Migration v17 → v18: снизить порог ликвидности
        if saved_version < 18:
            config.MIN_LIQUIDITY_USD = 500.0
            log.info("Settings migration v17→v18: MIN_LIQUIDITY_USD 2000 → 500")

        # Migration v18 → v19: отключить фильтры бесполезные на T+0.
        # Market cap и FDV на старте ВСЕГДА малы — они не отличают хороший токен от скама.
        # Единственный реальный фильтр — симуляция buy+sell.
        if saved_version < 19:
            config.MIN_LIQUIDITY_USD  = 100.0    # минимум чтобы пул не был совсем пустой
            config.MIN_MARKET_CAP_USD = 0.0      # 0 = выкл. — бесполезен на T+0
            config.MIN_FDV_USD        = 0.0      # 0 = выкл. — бесполезен на T+0
            config.MIN_VOLUME_5M_USD  = 0.0      # 0 = выкл. — снайпер входит первым
            config.MIN_HOLDER_COUNT   = 0        # 0 = выкл. — 1-3 холдера норма при листинге
            log.info(
                "Settings migration v18→v19: отключены T+0-бесполезные фильтры "
                "(mcap=0, fdv=0, liq=100, vol5m=0, holders=0)"
            )

        # Migration v19 → v20: поднять trailing stop 15% → 30% для волатильных мемкоинов
        if saved_version < 20:
            config.TRAILING_STOP_PCT = 30.0
            log.info("Settings migration v19→v20: TRAILING_STOP_PCT 15 → 30")

        # Migration v20 → v21: снизить gas 10→5 gwei (BSC 1-3 gwei норма, 10 переплата)
        # SLIPPAGE_BUY/SELL не в persistent settings — обновляются через config.py напрямую
        if saved_version < 21:
            config.GAS_BUY_GWEI = 5.0
            log.info("Settings migration v20→v21: GAS_BUY_GWEI 10 → 5")

        # Migration v21 → v22: расширить фильтры для большего охвата
        #   MAX_FDV_USD       10M → 100M  (некоторые листинги приходят с FDV >10M)
        #   TOP_HOLDER_MAX_PCT  50 → 90   (деплоер держит 80-90% токенов на T+0 — это норма)
        if saved_version < 22:
            config.MAX_FDV_USD          = 100_000_000.0
            config.TOP_HOLDER_MAX_PCT   = 90.0
            log.info("Settings migration v21→v22: MAX_FDV_USD 10M → 100M, TOP_HOLDER_MAX_PCT 50 → 90")

        # Migration v22 → v23: налоги + TP1 — математика работает при 20%+20% налогах
        #   MAX_BUY_TAX   10% → 20%   (~40% BSC токенов имеют launch tax 10-20%)
        #   MAX_SELL_TAX  10% → 20%   (при 20% налогах: break-even = +56%, TP1=75% = +5% net)
        #   TAKE_PROFIT_1  50% → 75%  (при 20% налогах: TP1=50% < break-even 56%, убыток!)
        if saved_version < 23:
            config.MAX_BUY_TAX   = 20.0
            config.MAX_SELL_TAX  = 20.0
            config.TAKE_PROFIT_1 = 75.0
            log.info("Settings migration v22→v23: MAX_BUY_TAX/SELL_TAX 10→20%, TAKE_PROFIT_1 50→75%")

        # Restore bot mode (after migrations so v10 override above takes effect)
        if "__is_auto" in data and saved_version >= 10:
            is_auto = bool(data["__is_auto"])
        if "__is_paused" in data:
            is_paused = bool(data["__is_paused"])

        # ENV var AUTO_BUY=false explicitly disables auto mode (override everything)
        # By default AUTO_BUY=true so this only fires when user explicitly sets =false
        if not config.AUTO_BUY:
            is_auto = False
            log.info("AUTO_BUY=false env var → auto mode disabled")

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


def _record_buy(pos: Position, tx_hash: str, delta_blocks: int = None, buyers_before: int = -1):
    """Record a buy event immediately when position opens."""
    entry: dict = {
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
    }
    if delta_blocks is not None:
        entry["delta_blocks"]  = delta_blocks
        entry["buyers_before"] = buyers_before
    trade_history.append(entry)
    _save_history()


def _record_trade(pos: Position, pnl_pct: float, reason: str, sell_price: float = 0.0):
    """Close a trade record. Updates the matching open entry if it exists."""
    global is_paused, _consecutive_losses

    # Circuit breaker: track consecutive losses, auto-pause on threshold
    if pnl_pct < 0:
        _consecutive_losses += 1
        if _consecutive_losses >= CIRCUIT_BREAKER_LOSSES and not is_paused:
            is_paused = True
            _save_settings()
            log.warning(
                f"Circuit breaker triggered: {_consecutive_losses} consecutive losing trades — bot paused"
            )
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(tg_send(
                    f"🛑 *Автопауза* — сработал защитный механизм\n\n"
                    f"*{_consecutive_losses} убыточных сделок подряд* — бот остановлен чтобы не допустить критических потерь.\n\n"
                    f"Последняя: *{pos.symbol}* {pnl_pct:+.1f}%\n\n"
                    f"Проверь настройки фильтров (/status) и возобнови торговлю вручную: /resume"
                ))
            except RuntimeError:
                pass  # no running event loop (shouldn't happen in normal operation)
    else:
        _consecutive_losses = 0  # profitable trade resets the streak

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
    for attempt in range(3):
        try:
            bot = Bot(token=config.BOT_TOKEN)
            await bot.send_message(
                chat_id=config.CHAT_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return
        except Exception as e:
            err = str(e)
            # Bad Markdown → retry once without parse_mode (plain text)
            if "can't parse" in err.lower() or "bad request" in err.lower():
                try:
                    bot2 = Bot(token=config.BOT_TOKEN)
                    await bot2.send_message(
                        chat_id=config.CHAT_ID,
                        text=text,
                        disable_web_page_preview=True,
                    )
                    return
                except Exception as e2:
                    log.error(f"tg_send fallback failed: {e2}")
                    return
            # Flood / rate limit → wait and retry
            if "flood" in err.lower() or "retry" in err.lower():
                await asyncio.sleep(5 * (attempt + 1))
                continue
            log.error(f"tg_send error (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2)
    log.error("tg_send: all 3 attempts failed — message dropped")

pos_manager = PositionManager(trader, tg_send)
pos_manager.on_close = _record_trade

pos_manager_base = None
if config.BASE_CHAIN_ENABLED and trader_base:
    pos_manager_base = PositionManager(
        trader_base, tg_send,
        positions_file=POSITIONS_FILE_BASE,
    )
    pos_manager_base.on_close = _record_trade

pos_manager_biswap = None
if config.BISWAP_ENABLED and trader_biswap:
    pos_manager_biswap = PositionManager(
        trader_biswap, tg_send,
        positions_file=POSITIONS_FILE_BISWAP,
    )
    pos_manager_biswap.on_close = _record_trade

pos_manager_baseswap = None
if config.BASESWAP_ENABLED and trader_baseswap:
    pos_manager_baseswap = PositionManager(
        trader_baseswap, tg_send,
        positions_file=POSITIONS_FILE_BASESWAP,
    )
    pos_manager_baseswap.on_close = _record_trade


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

# Mempool stats — how many tokens pre-analyzed, how many cache hits saved time
_mp_stats_pending: int   = 0   # tokens pre-analyzed from mempool
_mp_stats_hits:    int   = 0   # times cache was used in on_pair_found
_mp_stats_saved_s: float = 0.0 # total seconds saved by cache hits
_mp_enabled:       bool  = False  # set True when mempool task actually starts

# ── Wait-for-liquidity queue ──────────────────────────────────────────────────
# Tracks tokens where PairCreated fired before addLiquidity, so we don't
# reject them permanently — instead we poll until reserves appear.
_liquidity_waiting: set[str] = set()   # lowercase token addresses
_LIQ_WAIT_FAST_INTERVAL = 2.0          # first 15 attempts every 2s
_LIQ_WAIT_FAST_COUNT    = 15
_LIQ_WAIT_SLOW_INTERVAL = 3.0          # then every BSC block (~3s)
_LIQ_WAIT_TTL           = 900          # give up after 15 minutes (was 5)

# Minimum liquidity to TRIGGER the retry check (lower than MIN_LIQUIDITY_USD).
# Allows simulation to run when $25+ is present; buy decision still uses MIN_LIQUIDITY_USD.
_LIQ_WAIT_TRIGGER_USD   = 25.0

# One-time bypass set — allows _wait_for_liquidity retry to pass _is_token_duplicate
# WITHOUT popping from _seen_tokens_ts (which was causing re-detection bugs)
_liq_retry_bypass: set[str] = set()

# Dedup for near-miss threshold alerts — prevents the same token firing twice
# when both WS and HTTP poll detect it, or when a retry re-triggers the alert.
_near_miss_sent: dict[str, float] = {}   # token_address.lower() → timestamp
_NEAR_MISS_TTL = 120   # seconds — suppress duplicate near-miss for same token

# Security results from the original check_token() call, keyed by token_address.lower().
# When a token fails only due to liquidity, the 5-second security gather is already done
# and cached here so the retry only needs ~300ms (buy sim + liquidity + FDV).
_liq_security_cache: dict[str, dict] = {}
_LIQ_SECURITY_CACHE_TTL = 600   # discard after 10 minutes

# Rejection reasons that mean "no liquidity yet" — NOT a honeypot/scam reject
_LIQUIDITY_REASONS = (
    "нет ликвидности",
    "нулевой выход из пула",
    "ликвидность:",    # "Ликвидность: $30 < $100" — low but non-zero liquidity
)

# Reasons that mean "contract not open yet" — trading disabled, not a honeypot.
# Retry for up to _TRADING_WAIT_MAX_SEC before giving up.
_TRADING_NOT_READY_REASONS = (
    "симуляция отклонена контрактом",   # enableTrading() not called yet
)
_TRADING_WAIT_MAX_SEC = 300   # 5 minutes max wait for trading to open


def _simulate_buy_sync_for_wait(w3_instance, token_address: str) -> bool:
    """Quick buy-sim check: returns True if trading is open, False if still blocked."""
    result = _simulate_buy_sync(w3_instance, token_address,
                                "0x0000000000000000000000000000000000000001")
    return result["ok"]


async def _wait_for_liquidity_and_retry(
    token_address: str,
    base_token: str,
    pair_address: str,
    creation_block: int,
    w3_instance=None,
    callback=None,
    label: str = "",
):
    """
    Called when on_pair_found gets a liquidity-only rejection.
    Polls pair reserves until MIN_LIQUIDITY_USD is met, then re-runs the
    appropriate on_pair_found callback so all existing filters apply.
    """
    key = token_address.lower()
    if key in _liquidity_waiting:
        return   # already polling for this token
    _liquidity_waiting.add(key)

    sym = token_address[:10] + "…"
    tag = f"[{label}] " if label else ""
    log.info(f"{tag}[WaitLiq] {sym} — polling for liquidity (TTL={_LIQ_WAIT_TTL}s)")

    deadline   = time.time() + _LIQ_WAIT_TTL
    start_time = time.time()
    attempt    = 0
    _w3        = w3_instance or w3
    _callback  = callback or on_pair_found
    bnb_price  = await asyncio.to_thread(_bnb_price_sync, _w3)

    try:
        while time.time() < deadline:
            liq = await asyncio.to_thread(
                _liq_sync, _w3, pair_address, base_token, bnb_price
            )
            if liq >= _LIQ_WAIT_TRIGGER_USD:
                log.info(f"{tag}[WaitLiq] {sym} — liquidity ${liq:,.0f} (attempt {attempt+1})")
                _liq_retry_bypass.add(key)

                # Fast-path: cached security → only buy sim + liquidity + FDV (~300ms)
                cached_sec = _liq_security_cache.get(key)
                if (_callback is on_pair_found
                        and cached_sec
                        and (time.time() - cached_sec["ts"]) < _LIQ_SECURITY_CACHE_TTL
                        and cached_sec.get("security")):
                    fast_result = await check_token_fast(
                        token_address, pair_address, base_token, _w3,
                        min_liquidity_usd=config.MIN_LIQUIDITY_USD,
                        min_market_cap_usd=config.MIN_MARKET_CAP_USD,
                        min_fdv_usd=config.MIN_FDV_USD,
                        max_fdv_usd=config.MAX_FDV_USD,
                        security=cached_sec["security"],
                    )
                    if fast_result["ok"]:
                        elapsed = int(time.time() - start_time)
                        log.info(f"{tag}[WaitLiq] {sym} — fast-path PASSED (+{elapsed}s), triggering buy")
                        await tg_send(
                            f"⏳ Ликвидность появилась для `{token_address}`\n"
                            f"Прошло с листинга: {elapsed}s — все проверки пройдены, покупаю…"
                        )
                        _liq_security_cache.pop(key, None)
                        await _callback(
                            token_address, base_token, pair_address, creation_block,
                            _precheck_result=fast_result,
                        )
                        return

                    reason = fast_result["reason"]
                    reason_lc = reason.lower()
                    log.info(f"{tag}[WaitLiq] {sym} — fast-path: {reason}")

                    # Liquidity still building — keep polling, don't exit
                    if any(r in reason_lc for r in _LIQUIDITY_REASONS):
                        pass  # continue loop

                    # Contract not open yet (enableTrading not called) — retry up to 5 min
                    elif (any(r in reason_lc for r in _TRADING_NOT_READY_REASONS)
                          and time.time() - start_time < _TRADING_WAIT_MAX_SEC):
                        log.info(f"{tag}[WaitLiq] {sym} — trading not enabled yet, retrying...")

                    else:
                        # Permanent rejection (honeypot, tax, deployer, etc.)
                        log.info(f"{tag}[WaitLiq] {sym} — permanent reject after liquidity: {reason}")
                        _liq_security_cache.pop(key, None)
                        await tg_send(
                            f"⚠️ {sym} — ликвидность ${liq:,.0f}, но отклонён\n"
                            f"`{token_address}`\n"
                            f"Причина: {reason}"
                        )
                        return
                else:
                    # No security cache (e.g. initial "sim rejected" failure before APIs ran).
                    # Check if trading is now open by running buy sim only (~200ms).
                    # If open → run full callback (will re-run APIs).
                    # If still not open → keep polling this same loop (avoid cascading tasks).
                    _sim_check = await asyncio.to_thread(
                        _simulate_buy_sync_for_wait, _w3, token_address,
                    )
                    if _sim_check:
                        # Trading is open — fire full callback to run all security checks
                        elapsed = int(time.time() - start_time)
                        log.info(f"{tag}[WaitLiq] {sym} — trading now open (+{elapsed}s), running full check")
                        await tg_send(
                            f"⏳ Торговля открылась для `{token_address}`\n"
                            f"Прошло с листинга: {elapsed}s — запускаю полную проверку…"
                        )
                        _liq_security_cache.pop(key, None)
                        await _callback(token_address, base_token, pair_address, creation_block)
                        return
                    else:
                        # Trading still not open — keep polling same loop
                        log.info(f"{tag}[WaitLiq] {sym} — trading not enabled yet, retrying...")

            interval = _LIQ_WAIT_FAST_INTERVAL if attempt < _LIQ_WAIT_FAST_COUNT else _LIQ_WAIT_SLOW_INTERVAL
            attempt += 1
            await asyncio.sleep(interval)

        log.info(f"{tag}[WaitLiq] {sym} — TTL expired after {attempt} polls (max liq seen: tracked in logs)")
    finally:
        _liquidity_waiting.discard(key)
        _liq_security_cache.pop(key, None)   # clean up on timeout


async def on_pending_pair_found(token_address: str, base_token: str, pair_address: str):
    """
    Called from mempool watcher when a pending createPair tx is detected.

    Runs ONLY external API checks (GoPlus, honeypot.is, BSCScan, DexScreener) — no
    on-chain buy simulation, because the pair doesn't exist yet while the tx is pending.

    When PairCreated fires, on_pair_found finds the cached security and runs only the
    fast on-chain Stage 1 (~300ms) instead of the full 5-10s check_token().
    """
    global _mp_stats_pending
    key = token_address.lower()
    if key in _mempool_cache:
        return  # already analyzing

    t0 = time.time()
    _mempool_cache[key] = {"security": None, "pre_reject": None, "ts": t0}
    _mp_stats_pending += 1

    try:
        mp = await fetch_security_partial(
            token_address, pair_address,
            bscscan_api_key=config.BSCSCAN_API_KEY,
            max_buy_tax=config.MAX_BUY_TAX,
            max_sell_tax=config.MAX_SELL_TAX,
            wallet_address=trader.wallet,
            max_deployer_tokens_30d=config.MAX_DEPLOYER_TOKENS_30D,
            min_holder_count=config.MIN_HOLDER_COUNT,
            max_top10_holder_pct=config.MAX_TOP10_HOLDER_PCT,
            max_token_age_days=config.MAX_TOKEN_AGE_DAYS,
            min_volume_5m_usd=config.MIN_VOLUME_5M_USD,
        )
        elapsed = time.time() - t0
        _mempool_cache[key] = {
            "security":   mp["security"],
            "pre_reject": mp["pre_reject"],
            "ts":         time.time(),
            "elapsed":    elapsed,
        }
        status = f"❌ {mp['pre_reject']}" if mp["pre_reject"] else "✅ APIs OK"
        log.info(f"Mempool pre-analysis {token_address[:10]}…: {status} [{elapsed:.1f}s]")
    except Exception as e:
        log.warning(f"Mempool pre-analysis error for {token_address[:10]}…: {e}")
        _mempool_cache.pop(key, None)


# ── Speed/competition helpers ────────────────────────────────────────────────

# Swap(address,uint256,uint256,uint256,uint256,address) — same on all Uniswap V2 forks
_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


async def _count_prior_buyers(pair_address: str, from_block: int, to_block: int) -> int:
    """Return number of Swap events on the pair between creation and our buy block."""
    if not from_block or not to_block or to_block < from_block:
        return -1
    try:
        idx = _poll_ok_rpc_idx
        pw3 = _poll_w3s[idx] if _poll_w3s else w3
        logs = await asyncio.to_thread(
            pw3.eth.get_logs,
            {
                "address":   pair_address,
                "topics":    [_SWAP_TOPIC],
                "fromBlock": from_block,
                "toBlock":   to_block,
            },
        )
        # Each log = one swap; subtract 1 for our own buy (last one in range)
        return max(0, len(logs) - 1)
    except Exception:
        return -1


# ── New pair handler ──────────────────────────────────────────────────────────

async def on_pair_found(
    token_address: str,
    base_token: str,
    pair_address: str,
    creation_block: int = 0,
    _precheck_result: dict = None,   # pass pre-validated result to skip check_token()
):
    try:
        await _on_pair_found_inner(
            token_address, base_token, pair_address, creation_block, _precheck_result
        )
    except Exception as e:
        log.exception(f"[on_pair_found] unhandled exception for {token_address[:10]}: {e}")
        await tg_send(f"🚨 Ошибка в обработчике токена {token_address[:10]}…\n`{e}`")


async def _on_pair_found_inner(
    token_address: str,
    base_token: str,
    pair_address: str,
    creation_block: int = 0,
    _precheck_result: dict = None,
):
    global _stats_seen, _stats_rejected, _last_seen_ts, _last_reject, _last_pair_token

    if is_paused:
        log.info(f"Bot paused — skipping {token_address}")
        return

    if _is_token_duplicate(token_address, "[PancakeSwap]"):
        return

    _stats_seen  += 1
    _analytics["total_seen"] = _analytics.get("total_seen", 0) + 1
    _last_seen_ts = time.time()
    _last_pair_token = token_address
    _pair_event_times.append(_last_seen_ts)

    # Fast-path: security was pre-validated (e.g. from wait-for-liquidity cache)
    if _precheck_result is not None:
        result = _precheck_result
        log.info(f"⚡ Fast-path for {token_address[:10]}… — skipped full security gather")
    # Mempool pre-analysis cache: APIs already ran while createPair was pending
    else:
        key = token_address.lower()
        cached = _mempool_cache.pop(key, None)
        if cached and cached.get("security") and (time.time() - cached["ts"]) < _MEMPOOL_CACHE_TTL:
            global _mp_stats_hits, _mp_stats_saved_s
            saved_sec = cached.get("elapsed", 0.0)
            _mp_stats_hits   += 1
            _mp_stats_saved_s += saved_sec

            if cached["pre_reject"]:
                # APIs already found honeypot / bad deployer — instant reject
                log.info(f"⚡ Mempool pre-reject {token_address[:10]}…: {cached['pre_reject']}")
                result = {"ok": False, "reason": cached["pre_reject"]}
            else:
                # APIs passed — run ONLY on-chain Stage 1 (~300ms) with cached security
                log.info(
                    f"⚡ Mempool HIT {token_address[:10]}… (saved ~{saved_sec:.1f}s) "
                    f"— running Stage 1 on-chain only"
                )
                result = await check_token_fast(
                    token_address, pair_address, base_token, w3,
                    min_liquidity_usd=config.MIN_LIQUIDITY_USD,
                    min_market_cap_usd=config.MIN_MARKET_CAP_USD,
                    min_fdv_usd=config.MIN_FDV_USD,
                    max_fdv_usd=config.MAX_FDV_USD,
                    security=cached["security"],
                )
                if result["ok"]:
                    # Store security for any subsequent wait-for-liquidity retries
                    _liq_security_cache[key] = {
                        "security": cached["security"],
                        "ts": time.time(),
                        "base_token": base_token,
                        "pair_address": pair_address,
                    }
        else:
            log.info(f"Analyzing: {token_address}")
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
            except Exception as e:
                log.error(f"[PancakeSwap] check_token error for {token_address[:10]}…: {e}")
                return

    if not result["ok"]:
        _stats_rejected += 1
        _last_reject     = result["reason"]
        _track_rejection(result["reason"])
        # Log rejection with symbol if available
        sym = (result.get("info") or {}).get("symbol") or token_address[:10]
        entry = {
            "ts":      datetime.now(MOSCOW_TZ).strftime("%H:%M:%S"),
            "token":   token_address,
            "symbol":  sym,
            "reason":  result["reason"],
        }
        _reject_log.append(entry)
        if len(_reject_log) > _MAX_REJECT_LOG:
            _reject_log.pop(0)
        log.info(f"Rejected {sym} ({token_address[:10]}…): {result['reason']}")
        if DEBUG_ALERTS:
            await tg_send(
                f"🔍 *{sym}* — отклонён\n"
                f"`{token_address}`\n"
                f"Причина: {result['reason']}"
            )

        # If rejection is purely liquidity (reserves not yet added after PairCreated),
        # OR trading is not yet enabled (liquidity present but enableTrading() not called),
        # schedule a polling task instead of discarding the token permanently.
        reason_lc_outer = result["reason"].lower()
        _should_retry = (
            any(r in reason_lc_outer for r in _LIQUIDITY_REASONS)
            or any(r in reason_lc_outer for r in _TRADING_NOT_READY_REASONS)
        )
        if _should_retry:
            # Cache the already-computed security data for the fast retry path.
            # check_token() includes "_security" in the rejection dict when all
            # security APIs passed and only the on-chain liquidity check failed.
            security_data = result.get("_security")
            if security_data:
                _liq_security_cache[token_address.lower()] = {
                    "security": security_data,
                    "ts": time.time(),
                    "base_token": base_token,
                    "pair_address": pair_address,
                }
            asyncio.create_task(
                _wait_for_liquidity_and_retry(token_address, base_token, pair_address, creation_block)
            )
            return

        # Near-miss alert: passed all safety checks but blocked by a threshold filter
        _THRESHOLD_REASONS = (
            "Ликвидность:", "FDV:", "Market cap:", "Объём за 5 мин:",
            "Токен слишком старый", "Холдеров слишком мало",
        )
        if any(result["reason"].startswith(r) for r in _THRESHOLD_REASONS):
            _nm_key = token_address.lower()
            _nm_now = time.time()
            if _nm_now - _near_miss_sent.get(_nm_key, 0) > _NEAR_MISS_TTL:
                _near_miss_sent[_nm_key] = _nm_now
                await tg_send(
                    f"🔍 *Близко, но не прошёл фильтр*\n"
                    f"`{token_address}`\n"
                    f"❌ {result['reason']}"
                )
        return

    info      = result["info"]
    bnb_price = info["bnb_price"]

    balance    = await asyncio.to_thread(lambda: w3.eth.get_balance(trader.wallet) / 1e18)
    buy_amount = calculate_buy_amount(balance)
    if buy_amount == 0.0:
        log.info(f"Skipping {token_address}: balance too low for min trade size")
        await _maybe_alert_low_balance(balance, "BSC")
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
            await tg_send(
                f"📌 *{info['symbol']}* прошёл все фильтры, но слот занят\n"
                f"Открыто позиций: *{len(pos_manager.positions)}/{max_pos}*\n"
                f"💧 Ликвидность: ${info['liquidity_usd']:,.0f}{fdv_str}\n"
                f"`{token_address}`\n"
                f"_Закрой позицию чтобы освободить слот, или увеличь /set MAX\\_AUTO\\_POSITIONS_"
            )
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
                _qty        = result["tokens_received"] / 10 ** result["decimals"]
                entry_price = price_before if price_before > 0 else (
                    buy_amount / _qty if _qty > 0 else buy_amount
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
                    buy_gas_bnb       = result.get("gas_bnb", 0.0),
                )
                pos_manager.add(pos)

                # Speed metrics — how late vs pair creation
                buy_block = result.get("block_number", 0)
                delta_blocks  = (buy_block - creation_block) if creation_block and buy_block else None
                buyers_before = await _count_prior_buyers(pair_address, creation_block, buy_block) if creation_block and buy_block else -1
                if delta_blocks is not None:
                    _stats_delta_blocks.append(delta_blocks)
                    if len(_stats_delta_blocks) > 20:
                        _stats_delta_blocks.pop(0)
                    log.info(f"Speed: +{delta_blocks} blocks from listing | {buyers_before} buyers before us")

                _record_buy(pos, result["tx_hash"], delta_blocks=delta_blocks, buyers_before=buyers_before)
                amount_fmt = result["tokens_received"] / 10 ** result["decimals"]
                fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
                moon_str = (
                    f"\n🌙 Moon bag: *{moon_bag / 10**result['decimals']:.2f} {info['symbol']}* "
                    f"({config.MOON_BAG_PCT:.0f}%) — не продаётся авто"
                ) if moon_bag > 0 else ""
                speed_str = ""
                if delta_blocks is not None:
                    buyers_txt = f" | Покупок до нас: {buyers_before}" if buyers_before >= 0 else ""
                    speed_str = f"\n📊 Блоков от листинга: +{delta_blocks}{buyers_txt}"
                await tg_send(
                    f"✅ *Куплено авто* — {info['symbol']}\n\n"
                    f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                    f"Цена входа: {entry_price:.8f} BNB\n"
                    f"Tx: `{result['tx_hash']}`"
                    f"{speed_str}\n\n"
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
    log.warning(
        f"MANUAL mode for {info['symbol']} — is_auto={is_auto}. "
        f"To enable auto: /auto on  OR set AUTO_BUY=true in Railway env vars"
    )
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


# ── Base chain pair handler ───────────────────────────────────────────────────

async def on_base_pair_found(token_address: str, base_token: str, pair_address: str, creation_block: int = 0):
    """Mirror of on_pair_found but for Base chain (Uniswap V2)."""
    global _stats_seen, _stats_rejected, _last_seen_ts, _last_reject, _last_pair_token

    if is_paused or not trader_base or not pos_manager_base:
        return

    if _is_token_duplicate(token_address, "[Base]"):
        return

    _stats_seen  += 1
    _analytics["total_seen"] = _analytics.get("total_seen", 0) + 1
    _last_seen_ts = time.time()
    _last_pair_token = token_address
    _pair_event_times.append(_last_seen_ts)

    log.info(f"[Base] Analyzing: {token_address}")
    try:
        result = await check_token(
            token_address, pair_address, base_token, w3_base,
            config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
            wallet_address=trader_base.wallet,
            min_market_cap_usd=config.MIN_MARKET_CAP_USD,
            min_fdv_usd=config.MIN_FDV_USD,
            max_fdv_usd=config.MAX_FDV_USD,
            max_top10_holder_pct=config.MAX_TOP10_HOLDER_PCT,
            min_volume_5m_usd=config.MIN_VOLUME_5M_USD,
            max_token_age_days=config.MAX_TOKEN_AGE_DAYS,
            lp_holder_max_pct=config.LP_HOLDER_MAX_PCT,
            min_holder_count=config.MIN_HOLDER_COUNT,
            bscscan_api_key=config.BASESCAN_API_KEY,
            max_deployer_tokens_30d=config.MAX_DEPLOYER_TOKENS_30D,
            chain_id=config.BASE_CHAIN_ID,
            router_address=config.UNISWAP_V2_ROUTER_BASE,
            native_token=config.WETH_BASE,
            stable_token=config.USDC_BASE,
            dex_chain="base",
            explorer_url="https://api.basescan.org/api",
        )
    except Exception as e:
        log.error(f"[Base] check_token error for {token_address[:10]}…: {e}")
        return

    if not result["ok"]:
        _stats_rejected += 1
        _last_reject     = result["reason"]
        _track_rejection(result["reason"])
        sym = (result.get("info") or {}).get("symbol") or token_address[:10]
        entry = {
            "ts":     datetime.now(MOSCOW_TZ).strftime("%H:%M:%S"),
            "token":  token_address,
            "symbol": f"[Base] {sym}",
            "reason": result["reason"],
        }
        _reject_log.append(entry)
        if len(_reject_log) > _MAX_REJECT_LOG:
            _reject_log.pop(0)
        log.info(f"[Base] Rejected {sym}: {result['reason']}")

        _rlc_base = result["reason"].lower()
        if any(r in _rlc_base for r in _LIQUIDITY_REASONS) or any(r in _rlc_base for r in _TRADING_NOT_READY_REASONS):
            asyncio.create_task(
                _wait_for_liquidity_and_retry(
                    token_address, base_token, pair_address, creation_block,
                    w3_instance=w3_base, callback=on_base_pair_found, label="Base",
                )
            )
            return

        _THRESHOLD_REASONS = (
            "Ликвидность:", "FDV:", "Market cap:", "Объём за 5 мин:",
            "Токен слишком старый", "Холдеров слишком мало",
        )
        if any(result["reason"].startswith(r) for r in _THRESHOLD_REASONS):
            _nm_key = token_address.lower()
            _nm_now = time.time()
            if _nm_now - _near_miss_sent.get(_nm_key, 0) > _NEAR_MISS_TTL:
                _near_miss_sent[_nm_key] = _nm_now
                await tg_send(
                    f"🔵 *[Base] Близко, не прошёл фильтр*\n"
                    f"`{token_address}`\n"
                    f"❌ {result['reason']}"
                )
        return

    info       = result["info"]
    eth_price  = info["bnb_price"]
    balance    = await asyncio.to_thread(lambda: w3_base.eth.get_balance(trader_base.wallet) / 1e18)
    buy_amount = calculate_buy_amount(balance)

    if buy_amount == 0.0:
        log.info(f"[Base] Skipping {token_address}: balance too low")
        await _maybe_alert_low_balance(balance, "Base")
        return

    warnings = info.get("extra_warnings", [])
    if info["is_mintable"]:   warnings.append("⚠️ Mintable")
    if info["hidden_owner"]:  warnings.append("⚠️ Hidden owner")
    warn_block = "\n".join(warnings) if warnings else "✅ Дополнительных угроз нет"

    fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
    text = (
        f"🔵 *[Base] Новый токен прошёл все проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💧 Ликвидность: *${info['liquidity_usd']:,.0f}*{fdv_str}\n"
        f"💸 Buy tax: *{info['buy_tax']:.1f}%*  |  Sell tax: *{info['sell_tax']:.1f}%*\n"
        f"👥 Холдеры: {info['holder_count']}\n\n"
        f"{warn_block}\n\n"
        f"📊 TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции  "
        f"| Trailing: -{config.TRAILING_STOP_PCT}%  | SL: -{config.STOP_LOSS}%\n"
        f"💰 Покупка: *{buy_amount} ETH* (~${buy_amount * eth_price:.0f}) "
        f"| Баланс: {balance:.3f} ETH"
    )

    if is_auto:
        max_pos = calculate_max_positions(balance)
        if len(pos_manager_base.positions) >= max_pos:
            log.info(f"[Base] Auto: max positions ({max_pos}) reached, skipping {info['symbol']}")
            await tg_send(
                f"🔵📌 *[Base] {info['symbol']}* прошёл все фильтры, но слот занят\n"
                f"Открыто позиций: *{len(pos_manager_base.positions)}/{max_pos}*\n"
                f"💧 Ликвидность: ${info['liquidity_usd']:,.0f}{fdv_str}\n"
                f"`{token_address}`"
            )
            return
        if token_address in pos_manager_base.positions:
            return
        if token_address in _buying_tokens:
            return
        _buying_tokens.add(token_address)

        try:
            await tg_send(
                f"🔵⚡ *[Base] Авто-покупка* — {info['name']} (`{info['symbol']}`)\n"
                f"💰 {buy_amount} ETH (~${buy_amount * eth_price:.0f}) | "
                f"Ликвидность: ${info['liquidity_usd']:,.0f}\n"
                f"{warn_block}"
            )

            approve_task    = asyncio.to_thread(trader_base.approve_token, token_address)
            price_task      = asyncio.to_thread(trader_base.get_price, token_address, base_token)
            approve_result, price_before = await asyncio.gather(approve_task, price_task)

            if not approve_result["ok"]:
                await tg_send(
                    f"❌ [Base] Авто: не удалось одобрить *{info['symbol']}*\n"
                    f"`{approve_result['reason']}`"
                )
                return

            buy_result = await asyncio.to_thread(trader_base.buy, token_address, buy_amount)

            if buy_result["ok"]:
                _qty        = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                entry_price = price_before if price_before > 0 else (
                    buy_amount / _qty if _qty > 0 else buy_amount
                )
                moon_bag = _calc_moon_bag(buy_result["tokens_received"], buy_amount, eth_price)
                tradeable = buy_result["tokens_received"] - moon_bag
                pos = Position(
                    token_address     = token_address,
                    symbol            = info["symbol"],
                    name              = info["name"],
                    pair_address      = pair_address,
                    buy_price_bnb     = entry_price,
                    tokens_amount     = tradeable,
                    decimals          = buy_result["decimals"],
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
                    chain             = "base",
                )
                pos_manager_base.add(pos)
                _record_buy(pos, buy_result["tx_hash"])
                amount_fmt = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                await tg_send(
                    f"🔵✅ *[Base] Куплено авто* — {info['symbol']}\n\n"
                    f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                    f"Цена входа: {entry_price:.8f} ETH\n"
                    f"Tx: `{buy_result['tx_hash']}`\n\n"
                    f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%  "
                    f"| SL: -{config.STOP_LOSS}%"
                )
            else:
                await tg_send(
                    f"❌ [Base] Авто: ошибка покупки *{info['symbol']}*: {buy_result['reason']}"
                )
        finally:
            _buying_tokens.discard(token_address)
        return

    # Manual mode: send notification (no inline buy button for Base in this version)
    await tg_send(text)


# ── BiSwap pair handler ───────────────────────────────────────────────────────

async def on_biswap_pair_found(token_address: str, base_token: str, pair_address: str, creation_block: int = 0):
    """Mirror of on_pair_found for BiSwap V2 (BSC)."""
    global _stats_seen, _stats_rejected, _last_seen_ts, _last_reject, _last_pair_token

    if is_paused or not trader_biswap or not pos_manager_biswap:
        return

    if _is_token_duplicate(token_address, "[BiSwap]"):
        return

    _stats_seen  += 1
    _analytics["total_seen"] = _analytics.get("total_seen", 0) + 1
    _last_seen_ts = time.time()
    _last_pair_token = token_address
    _pair_event_times.append(_last_seen_ts)

    log.info(f"[BiSwap] Analyzing: {token_address}")
    try:
        result = await check_token(
            token_address, pair_address, base_token, w3,
            config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
            wallet_address=trader_biswap.wallet,
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
            router_address=config.BISWAP_ROUTER,
            native_token=config.WBNB,
        )
    except Exception as e:
        log.error(f"[BiSwap] check_token error for {token_address[:10]}…: {e}")
        return

    if not result["ok"]:
        _stats_rejected += 1
        _last_reject     = result["reason"]
        _track_rejection(result["reason"])
        sym = (result.get("info") or {}).get("symbol") or token_address[:10]
        entry = {
            "ts":     datetime.now(MOSCOW_TZ).strftime("%H:%M:%S"),
            "token":  token_address,
            "symbol": f"[BiSwap] {sym}",
            "reason": result["reason"],
        }
        _reject_log.append(entry)
        if len(_reject_log) > _MAX_REJECT_LOG:
            _reject_log.pop(0)
        log.info(f"[BiSwap] Rejected {sym}: {result['reason']}")

        _rlc_bi = result["reason"].lower()
        if any(r in _rlc_bi for r in _LIQUIDITY_REASONS) or any(r in _rlc_bi for r in _TRADING_NOT_READY_REASONS):
            asyncio.create_task(
                _wait_for_liquidity_and_retry(
                    token_address, base_token, pair_address, creation_block,
                    w3_instance=w3, callback=on_biswap_pair_found, label="BiSwap",
                )
            )
        return

    info      = result["info"]
    bnb_price = info["bnb_price"]
    balance    = await asyncio.to_thread(lambda: w3.eth.get_balance(trader_biswap.wallet) / 1e18)
    buy_amount = calculate_buy_amount(balance)
    if buy_amount == 0.0:
        log.info(f"[BiSwap] Skipping {token_address}: balance too low")
        await _maybe_alert_low_balance(balance, "BSC/BiSwap")
        return

    warnings = info.get("extra_warnings", [])
    if info["is_mintable"]:  warnings.append("⚠️ Mintable")
    if info["hidden_owner"]: warnings.append("⚠️ Hidden owner")
    warn_block = "\n".join(warnings) if warnings else "✅ Дополнительных угроз нет"

    fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
    text = (
        f"🟠 *[BiSwap] Новый токен прошёл все проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💧 Ликвидность: *${info['liquidity_usd']:,.0f}*{fdv_str}\n"
        f"💸 Buy tax: *{info['buy_tax']:.1f}%*  |  Sell tax: *{info['sell_tax']:.1f}%*\n"
        f"👥 Холдеры: {info['holder_count']}\n\n"
        f"{warn_block}\n\n"
        f"📊 TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции  "
        f"| Trailing: -{config.TRAILING_STOP_PCT}%  | SL: -{config.STOP_LOSS}%\n"
        f"💰 Покупка: *{buy_amount} BNB* (~${buy_amount * bnb_price:.0f}) "
        f"| Баланс: {balance:.3f} BNB"
    )

    if is_auto:
        max_pos = calculate_max_positions(balance)
        if len(pos_manager_biswap.positions) >= max_pos:
            log.info(f"[BiSwap] Auto: max positions ({max_pos}) reached, skipping {info['symbol']}")
            await tg_send(
                f"🟠📌 *[BiSwap] {info['symbol']}* прошёл все фильтры, но слот занят\n"
                f"Открыто позиций: *{len(pos_manager_biswap.positions)}/{max_pos}*\n"
                f"💧 Ликвидность: ${info['liquidity_usd']:,.0f}{fdv_str}\n"
                f"`{token_address}`"
            )
            return
        if token_address in pos_manager_biswap.positions:
            return
        if token_address in _buying_tokens:
            return
        _buying_tokens.add(token_address)

        try:
            await tg_send(
                f"🟠⚡ *[BiSwap] Авто-покупка* — {info['name']} (`{info['symbol']}`)\n"
                f"💰 {buy_amount} BNB (~${buy_amount * bnb_price:.0f}) | "
                f"Ликвидность: ${info['liquidity_usd']:,.0f}\n"
                f"{warn_block}"
            )

            approve_task    = asyncio.to_thread(trader_biswap.approve_token, token_address)
            price_task      = asyncio.to_thread(trader_biswap.get_price, token_address, base_token)
            approve_result, price_before = await asyncio.gather(approve_task, price_task)

            if not approve_result["ok"]:
                await tg_send(
                    f"❌ [BiSwap] Авто: не удалось одобрить *{info['symbol']}*\n"
                    f"`{approve_result['reason']}`"
                )
                return

            buy_result = await asyncio.to_thread(trader_biswap.buy, token_address, buy_amount)

            if buy_result["ok"]:
                _qty        = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                entry_price = price_before if price_before > 0 else (
                    buy_amount / _qty if _qty > 0 else buy_amount
                )
                moon_bag  = _calc_moon_bag(buy_result["tokens_received"], buy_amount, bnb_price)
                tradeable = buy_result["tokens_received"] - moon_bag
                pos = Position(
                    token_address     = token_address,
                    symbol            = info["symbol"],
                    name              = info["name"],
                    pair_address      = pair_address,
                    buy_price_bnb     = entry_price,
                    tokens_amount     = tradeable,
                    decimals          = buy_result["decimals"],
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
                    chain             = "bsc",
                )
                pos_manager_biswap.add(pos)
                _record_buy(pos, buy_result["tx_hash"])
                amount_fmt = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                await tg_send(
                    f"🟠✅ *[BiSwap] Куплено авто* — {info['symbol']}\n\n"
                    f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                    f"Цена входа: {entry_price:.8f} BNB\n"
                    f"Tx: `{buy_result['tx_hash']}`\n\n"
                    f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%  "
                    f"| SL: -{config.STOP_LOSS}%"
                )
            else:
                await tg_send(
                    f"❌ [BiSwap] Авто: ошибка покупки *{info['symbol']}*: {buy_result['reason']}"
                )
        finally:
            _buying_tokens.discard(token_address)
        return

    await tg_send(text)


# ── BaseSwap pair handler ─────────────────────────────────────────────────────

async def on_baseswap_pair_found(token_address: str, base_token: str, pair_address: str, creation_block: int = 0):
    """Mirror of on_base_pair_found for BaseSwap V2 (Base)."""
    global _stats_seen, _stats_rejected, _last_seen_ts, _last_reject, _last_pair_token

    if is_paused or not trader_baseswap or not pos_manager_baseswap:
        return

    if _is_token_duplicate(token_address, "[BaseSwap]"):
        return

    _stats_seen  += 1
    _analytics["total_seen"] = _analytics.get("total_seen", 0) + 1
    _last_seen_ts = time.time()
    _last_pair_token = token_address
    _pair_event_times.append(_last_seen_ts)

    log.info(f"[BaseSwap] Analyzing: {token_address}")
    try:
        result = await check_token(
            token_address, pair_address, base_token, w3_base,
            config.MIN_LIQUIDITY_USD, config.MAX_BUY_TAX, config.MAX_SELL_TAX,
            wallet_address=trader_baseswap.wallet,
            min_market_cap_usd=config.MIN_MARKET_CAP_USD,
            min_fdv_usd=config.MIN_FDV_USD,
            max_fdv_usd=config.MAX_FDV_USD,
            max_top10_holder_pct=config.MAX_TOP10_HOLDER_PCT,
            min_volume_5m_usd=config.MIN_VOLUME_5M_USD,
            max_token_age_days=config.MAX_TOKEN_AGE_DAYS,
            lp_holder_max_pct=config.LP_HOLDER_MAX_PCT,
            min_holder_count=config.MIN_HOLDER_COUNT,
            bscscan_api_key=config.BASESCAN_API_KEY,
            max_deployer_tokens_30d=config.MAX_DEPLOYER_TOKENS_30D,
            chain_id=config.BASE_CHAIN_ID,
            router_address=config.BASESWAP_ROUTER_BASE,
            native_token=config.WETH_BASE,
            stable_token=config.USDC_BASE,
            dex_chain="base",
            explorer_url="https://api.basescan.org/api",
        )
    except Exception as e:
        log.error(f"[BaseSwap] check_token error for {token_address[:10]}…: {e}")
        return

    if not result["ok"]:
        _stats_rejected += 1
        _last_reject     = result["reason"]
        _track_rejection(result["reason"])
        sym = (result.get("info") or {}).get("symbol") or token_address[:10]
        entry = {
            "ts":     datetime.now(MOSCOW_TZ).strftime("%H:%M:%S"),
            "token":  token_address,
            "symbol": f"[BaseSwap] {sym}",
            "reason": result["reason"],
        }
        _reject_log.append(entry)
        if len(_reject_log) > _MAX_REJECT_LOG:
            _reject_log.pop(0)
        log.info(f"[BaseSwap] Rejected {sym}: {result['reason']}")

        _rlc_bs = result["reason"].lower()
        if any(r in _rlc_bs for r in _LIQUIDITY_REASONS) or any(r in _rlc_bs for r in _TRADING_NOT_READY_REASONS):
            asyncio.create_task(
                _wait_for_liquidity_and_retry(
                    token_address, base_token, pair_address, creation_block,
                    w3_instance=w3_base, callback=on_baseswap_pair_found, label="BaseSwap",
                )
            )
        return

    info       = result["info"]
    eth_price  = info["bnb_price"]
    balance    = await asyncio.to_thread(lambda: w3_base.eth.get_balance(trader_baseswap.wallet) / 1e18)
    buy_amount = calculate_buy_amount(balance)
    if buy_amount == 0.0:
        log.info(f"[BaseSwap] Skipping {token_address}: balance too low")
        await _maybe_alert_low_balance(balance, "Base/BaseSwap")
        return

    warnings = info.get("extra_warnings", [])
    if info["is_mintable"]:  warnings.append("⚠️ Mintable")
    if info["hidden_owner"]: warnings.append("⚠️ Hidden owner")
    warn_block = "\n".join(warnings) if warnings else "✅ Дополнительных угроз нет"

    fdv_str = f" | FDV: ${info['fdv_usd']:,.0f}" if info.get("fdv_usd") else ""
    text = (
        f"🔷 *[BaseSwap] Новый токен прошёл все проверки*\n\n"
        f"🪙 *{info['name']}* (`{info['symbol']}`)\n"
        f"📄 `{token_address}`\n\n"
        f"💧 Ликвидность: *${info['liquidity_usd']:,.0f}*{fdv_str}\n"
        f"💸 Buy tax: *{info['buy_tax']:.1f}%*  |  Sell tax: *{info['sell_tax']:.1f}%*\n"
        f"👥 Холдеры: {info['holder_count']}\n\n"
        f"{warn_block}\n\n"
        f"📊 TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции  "
        f"| Trailing: -{config.TRAILING_STOP_PCT}%  | SL: -{config.STOP_LOSS}%\n"
        f"💰 Покупка: *{buy_amount} ETH* (~${buy_amount * eth_price:.0f}) "
        f"| Баланс: {balance:.3f} ETH"
    )

    if is_auto:
        max_pos = calculate_max_positions(balance)
        if len(pos_manager_baseswap.positions) >= max_pos:
            log.info(f"[BaseSwap] Auto: max positions ({max_pos}) reached, skipping {info['symbol']}")
            await tg_send(
                f"🔷📌 *[BaseSwap] {info['symbol']}* прошёл все фильтры, но слот занят\n"
                f"Открыто позиций: *{len(pos_manager_baseswap.positions)}/{max_pos}*\n"
                f"💧 Ликвидность: ${info['liquidity_usd']:,.0f}{fdv_str}\n"
                f"`{token_address}`"
            )
            return
        if token_address in pos_manager_baseswap.positions:
            return
        if token_address in _buying_tokens:
            return
        _buying_tokens.add(token_address)

        try:
            await tg_send(
                f"🔷⚡ *[BaseSwap] Авто-покупка* — {info['name']} (`{info['symbol']}`)\n"
                f"💰 {buy_amount} ETH (~${buy_amount * eth_price:.0f}) | "
                f"Ликвидность: ${info['liquidity_usd']:,.0f}\n"
                f"{warn_block}"
            )

            approve_task    = asyncio.to_thread(trader_baseswap.approve_token, token_address)
            price_task      = asyncio.to_thread(trader_baseswap.get_price, token_address, base_token)
            approve_result, price_before = await asyncio.gather(approve_task, price_task)

            if not approve_result["ok"]:
                await tg_send(
                    f"❌ [BaseSwap] Авто: не удалось одобрить *{info['symbol']}*\n"
                    f"`{approve_result['reason']}`"
                )
                return

            buy_result = await asyncio.to_thread(trader_baseswap.buy, token_address, buy_amount)

            if buy_result["ok"]:
                _qty        = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                entry_price = price_before if price_before > 0 else (
                    buy_amount / _qty if _qty > 0 else buy_amount
                )
                moon_bag  = _calc_moon_bag(buy_result["tokens_received"], buy_amount, eth_price)
                tradeable = buy_result["tokens_received"] - moon_bag
                pos = Position(
                    token_address     = token_address,
                    symbol            = info["symbol"],
                    name              = info["name"],
                    pair_address      = pair_address,
                    buy_price_bnb     = entry_price,
                    tokens_amount     = tradeable,
                    decimals          = buy_result["decimals"],
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
                    chain             = "base",
                )
                pos_manager_baseswap.add(pos)
                _record_buy(pos, buy_result["tx_hash"])
                amount_fmt = buy_result["tokens_received"] / 10 ** buy_result["decimals"]
                await tg_send(
                    f"🔷✅ *[BaseSwap] Куплено авто* — {info['symbol']}\n\n"
                    f"Получено: {amount_fmt:.4f} {info['symbol']}\n"
                    f"Цена входа: {entry_price:.8f} ETH\n"
                    f"Tx: `{buy_result['tx_hash']}`\n\n"
                    f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}%  "
                    f"| SL: -{config.STOP_LOSS}%"
                )
            else:
                await tg_send(
                    f"❌ [BaseSwap] Авто: ошибка покупки *{info['symbol']}*: {buy_result['reason']}"
                )
        finally:
            _buying_tokens.discard(token_address)
        return

    await tg_send(text)


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
        current_balance = await asyncio.to_thread(lambda: w3.eth.get_balance(trader.wallet) / 1e18)
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
            _qty        = result["tokens_received"] / 10 ** result["decimals"]
            entry_price = price_before if price_before > 0 else (
                buy_amount / _qty if _qty > 0 else buy_amount
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
        "*Диагностика*\n"
        "/rejects — последние 10 отклонённых токенов с причиной\n"
        "/debug — диагностика: статус WS, количество пар/час, heartbeat\n\n"
        "*Статистика*\n"
        "/analytics — полная аналитика: воронка, топ причин отклонения, honeypot, P&L\n"
        "/stats — статистика сделок (win rate, PnL, breakdown)\n"
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
            "Параметры: `buy`, `sl`, `tp1`, `trail`, `liq`, `mcap`, `lp`, `deployer`, `tax`, `max`, `gwei`",
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
        "liq":    ("MIN_LIQUIDITY_USD",   0.0,   1e7,   "Мин. ликвидность USD (0 = выкл.)"),
        "mcap":   ("MIN_MARKET_CAP_USD",  0.0,   1e7,   "Мин. market cap USD (0 = выкл.)"),
        "fdvmin": ("MIN_FDV_USD",         0.0,   1e7,   "Мин. FDV USD (0 = выкл.)"),
        "fdvmax": ("MAX_FDV_USD",         1000.0,1e9,   "Макс. FDV USD"),
        "vol5m":  ("MIN_VOLUME_5M_USD",   0.0,   1e6,   "Мин. объём за 5 мин USD"),
        "age":    ("MAX_TOKEN_AGE_DAYS",  1,     365,   "Макс. возраст токена (дней)"),
        "top10":  ("MAX_TOP10_HOLDER_PCT",1.0,   99.0,  "Макс. топ-10 холдеры % (excl. DEX)"),
        "lp":     ("LP_HOLDER_MAX_PCT",  1.0,   100.0, "Макс. % LP в одном незаблокированном кошельке (100 = выкл.)"),
        "holders":("MIN_HOLDER_COUNT",   1,     10000, "Мин. кол-во холдеров токена"),
        "deployer":("MAX_DEPLOYER_TOKENS_30D", 1, 200, "Макс. контрактов деплоера за 30 дней"),
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
    connected  = await asyncio.to_thread(w3.is_connected)
    balance    = await asyncio.to_thread(lambda: w3.eth.get_balance(trader.wallet) / 1e18)
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
    gas_gwei  = (await asyncio.to_thread(lambda: w3.eth.gas_price) / 1e9) if connected else 0
    gas_mode  = f"{config.GAS_BUY_GWEI} gwei (фикс)" if config.GAS_BUY_GWEI > 0 else f"{gas_gwei:.1f} x{config.GAS_MULTIPLIER}"
    ws_count  = len(config.BSC_WS_RPCS)

    moon_str = (
        f"Moon bag: {config.MOON_BAG_PCT:.0f}% при сделке ≥${config.MOON_BAG_MIN_USD:.0f}"
    )

    # ── Activity summary ──────────────────────────────────────────────────────
    if _last_seen_ts:
        last_seen_ago = int(time.time() - _last_seen_ts)
        if last_seen_ago < 60:
            last_seen_str = f"{last_seen_ago}с назад"
        elif last_seen_ago < 3600:
            last_seen_str = f"{last_seen_ago // 60}м назад"
        else:
            last_seen_str = f"{last_seen_ago // 3600}ч назад"
        activity_str = (
            f"*Активность (с запуска):*\n"
            f"Пар замечено: {_stats_seen} | Отклонено: {_stats_rejected}\n"
            f"Последняя пара: {last_seen_str}\n"
        )
        if _last_reject:
            activity_str += f"Последний отказ: {_last_reject}\n"
        if _consecutive_losses > 0:
            activity_str += (
                f"⚠️ Убыточных сделок подряд: *{_consecutive_losses}/{CIRCUIT_BREAKER_LOSSES}* "
                f"(автопауза при достижении)\n"
            )
    else:
        activity_str = "*Активность:* пар ещё не замечено\n"

    # ── Poll diagnostics ──────────────────────────────────────────────────────
    if _poll_last_ok_ts:
        poll_ago = int(time.time() - _poll_last_ok_ts)
        poll_str = f"✅ последний успешный поллинг: {poll_ago}с назад (RPC#{_poll_ok_rpc_idx})"
    else:
        poll_str = "⚠️ поллинг ещё не завершил ни одного успешного запроса"
    if _poll_consecutive_errors:
        poll_str += f" | ошибок подряд: {_poll_consecutive_errors}"

    if _stats_delta_blocks:
        avg_delta = sum(_stats_delta_blocks) / len(_stats_delta_blocks)
        speed_status = f"⚡ Скорость покупки: avg +{avg_delta:.1f} блоков от листинга (последние {len(_stats_delta_blocks)} покупок)\n"
    else:
        speed_status = ""

    msg = (
        f"Статус Sniper Bot — {status_icon}\n\n"
        f"RPC: {'✅' if connected else '❌'} {rpc_label} | WS: {ws_count} | Poll RPCs: {len(_poll_w3s)}\n"
        f"Кошелёк: {trader.wallet}\n"
        f"Баланс: {balance:.4f} BNB (~${balance * bnb_price:.0f})\n"
        f"Позиций: {len(pos_manager.positions)}/{calculate_max_positions(balance) if is_auto else config.MAX_POSITIONS}\n"
        f"Сделок в истории: {len(trade_history)}\n\n"
        f"{activity_str}"
        f"{speed_status}"
        f"HTTP Poll: {poll_str}\n"
        f"Mempool: {'активен' if _mp_enabled else ('выкл' if not config.MEMPOOL_ENABLED else 'стартует')}"
        f" | pre-analyzed: {_mp_stats_pending} | cache hits: {_mp_stats_hits}"
        f"{f' | saved: {_mp_stats_saved_s:.0f}s' if _mp_stats_hits else ''}\n\n"
        f"Размер позиции:\n"
        f"Режим: {size_mode}\n"
        f"Следующая сделка: {buy_amount} BNB (~${buy_amount * bnb_price:.0f})\n"
        f"Мин: {config.BUY_MIN_BNB} BNB | Макс: {config.BUY_MAX_BNB} BNB | Газ-резерв: {config.GAS_RESERVE_BNB} BNB\n\n"
        f"Выход:\n"
        f"TP1: +{config.TAKE_PROFIT_1}% → {config.TAKE_PROFIT_1_PCT:.0f}% позиции\n"
        f"Trailing stop: -{config.TRAILING_STOP_PCT}% от пика | SL: -{config.STOP_LOSS}%\n"
        f"{moon_str}\n\n"
        f"Фильтры покупки:\n"
        f"Ликвидность: ${config.MIN_LIQUIDITY_USD:,.0f} мин\n"
        f"Market cap: ${config.MIN_MARKET_CAP_USD:,.0f} мин\n"
        f"FDV: ${config.MIN_FDV_USD/1000:.0f}k – ${config.MAX_FDV_USD/1000000:.0f}M\n"
        f"Объём 5м: ${config.MIN_VOLUME_5M_USD:,.0f} мин\n"
        f"Возраст: {config.MAX_TOKEN_AGE_DAYS} дней макс\n"
        f"Холдеры: {config.MIN_HOLDER_COUNT} мин\n"
        f"Топ-10 холдеры: {config.MAX_TOP10_HOLDER_PCT:.0f}% макс\n"
        f"LP незаблокирован: {config.LP_HOLDER_MAX_PCT:.0f}% макс на кошелёк\n"
        f"Max tax: {config.MAX_BUY_TAX}% buy / {config.MAX_SELL_TAX}% sell\n"
        f"Деплоер: макс {config.MAX_DEPLOYER_TOKENS_30D} контракт(ов) за 30 дн. "
        f"{'BSCScan OK' if config.BSCSCAN_API_KEY else 'BSCScan нет ключа'}"
        f"{' | Basescan OK' if config.BASESCAN_API_KEY else ' | Basescan нет ключа'}\n\n"
        f"Исполнение:\n"
        f"Gas buy: {gas_mode}\n"
        f"Slip buy/sell: {config.SLIPPAGE_BUY}%/{config.SLIPPAGE_SELL}%\n"
        f"Deadline: {config.TX_DEADLINE_SEC}s"
    )
    await update.message.reply_text(msg)


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
async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Diagnostic info: WS endpoint health, pair rate, RPC status."""
    now = time.time()

    # ── WebSocket endpoints ───────────────────────────────────────────────────
    ws_lines = []
    for ep_key, st in _ws_endpoint_status.items():
        icon = "🟢" if st["connected"] else "🔴"
        if st["last_event_ts"]:
            ago = int(now - st["last_event_ts"])
            ago_str = f"{ago}s ago" if ago < 120 else f"{ago // 60}m ago"
            ev_str = f"last event {ago_str}, total={st['events_total']}"
        else:
            ev_str = "no events yet"
        err_str = f" | err: {st['last_error'][:50]}" if st["last_error"] else ""
        recon = f" | reconnects={st['reconnects']}" if st["reconnects"] else ""
        url_short = st.get("url", ep_key)[:55]
        ws_lines.append(f"{icon} `{url_short}`\n    {ev_str}{recon}{err_str}")

    if not ws_lines:
        ws_lines = ["⚠️ Нет данных — WS ещё не запустился или нет соединений"]

    # ── Pairs per hour (rolling window) ──────────────────────────────────────
    cutoff_1h = now - 3600
    pairs_1h  = sum(1 for t in _pair_event_times if t > cutoff_1h)
    cutoff_10m = now - 600
    pairs_10m  = sum(1 for t in _pair_event_times if t > cutoff_10m)

    if _last_seen_ts:
        last_ago = int(now - _last_seen_ts)
        last_str = f"{last_ago}s назад" if last_ago < 120 else f"{last_ago // 60}м назад"
        last_token = f"`{_last_pair_token[:20]}…`" if _last_pair_token else "—"
        last_info = f"Последняя пара: {last_token} ({last_str})"
    else:
        last_info = "Пар ещё не получено"

    # ── HTTP poll ─────────────────────────────────────────────────────────────
    if _poll_last_ok_ts:
        poll_ago = int(now - _poll_last_ok_ts)
        poll_str = f"✅ последний ОК: {poll_ago}s назад (RPC#{_poll_ok_rpc_idx})"
    else:
        poll_str = "⚠️ ещё не завершил успешный опрос"
    if _poll_consecutive_errors:
        poll_str += f" | ошибок: {_poll_consecutive_errors}"

    # ── Watchdog status ───────────────────────────────────────────────────────
    uptime_min = int((now - _BOT_START_TIME) / 60)
    since_last = int(now - _last_seen_ts) if _last_seen_ts else uptime_min * 60
    watchdog_ok = since_last < 600
    wd_icon = "🟢" if watchdog_ok else "🚨"

    # ── Chain status ──────────────────────────────────────────────────────────
    bsc_icon  = "🟢" if w3 and w3.is_connected() else "🔴"
    base_icon = "🟢" if (config.BASE_CHAIN_ENABLED and w3_base) else "🔴"
    base_label = (
        "🟢 активен" if (config.BASE_CHAIN_ENABLED and w3_base)
        else ("🔴 BASE_CHAIN_ENABLED=true, но все RPC недоступны при старте — добавь надёжный BASE_HTTP_RPC"
              if _base_chain_rpc_failed
              else "⚫ выкл (BASE_CHAIN_ENABLED=false)")
    )

    # ── Mempool status ────────────────────────────────────────────────────────
    if not config.MEMPOOL_ENABLED:
        mp_str = "⚫ выкл (MEMPOOL\\_ENABLED=false)"
    elif _mp_enabled:
        mp_str = (
            f"🧠 активен | pre-analyzed: {_mp_stats_pending}"
            f" | cache hits: {_mp_stats_hits}"
            + (f" | saved: {_mp_stats_saved_s:.0f}s" if _mp_stats_hits else "")
        )
    else:
        mp_str = "⏳ стартует..."

    ws_block = "\n".join(ws_lines)
    await update.message.reply_text(
        f"*🔍 Диагностика бота*\n\n"
        f"⏱ Uptime: {uptime_min}м\n\n"
        f"*Сети:*\n"
        f"{bsc_icon} BSC (PancakeSwap V2)\n"
        f"{base_icon} Base — {base_label}\n\n"
        f"*WebSocket endpoints ({len(_ws_endpoint_status)}):*\n"
        f"{ws_block}\n\n"
        f"*Pair rate:*\n"
        f"За последние 10 мин: *{pairs_10m}* пар\n"
        f"За последний час: *{pairs_1h}* пар\n"
        f"{last_info}\n\n"
        f"*HTTP Poll:* {poll_str}\n\n"
        f"*Mempool:* {mp_str}\n\n"
        f"*Heartbeat:* {wd_icon} "
        f"{'OK' if watchdog_ok else f'нет пар {since_last//60}м — проверь WS'}\n\n"
        f"DEBUG\\_ALERTS: {'🔔 вкл' if DEBUG_ALERTS else '🔕 выкл'}\n"
        f"Отклонено всего: {_stats_rejected} | Замечено: {_stats_seen}",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def cmd_rejects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show last rejected tokens with reason."""
    if not _reject_log:
        await update.message.reply_text(
            "✅ Отклонённых токенов нет — или бот только запустился.\n"
            f"Пар замечено с запуска: {_stats_seen}"
        )
        return

    lines = [f"🚫 Последние отклонения (всего: {_stats_rejected})\n"]
    for e in _reject_log[-10:][::-1]:
        lines.append(f"{e['ts']}  {e['symbol']} — {e['reason']}")

    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Full analytics dashboard: funnel, rejection breakdown, trading performance."""
    # ── Funnel ────────────────────────────────────────────────────────────────
    total_seen_ever = _analytics.get("total_seen", 0)
    total_rej_ever  = _analytics.get("total_rejected", 0)
    total_seen_now  = _stats_seen
    total_rej_now   = _stats_rejected

    closed_trades = [t for t in trade_history if t.get("status") == "closed" or "pnl_pct" in t]
    open_trades   = [t for t in trade_history if t.get("status") == "open"]
    total_bought  = len(closed_trades) + len(open_trades)

    passed_filters = total_bought  # every bought token passed filters
    pass_rate = passed_filters / total_seen_ever * 100 if total_seen_ever else 0
    buy_rate  = total_bought  / total_seen_ever * 100 if total_seen_ever else 0

    funnel = (
        f"*🔍 Воронка (за всё время):*\n"
        f"Обнаружено пар: *{total_seen_ever:,}*\n"
        f"Отклонено: *{total_rej_ever:,}* ({100 - pass_rate:.1f}%)\n"
        f"Прошло фильтры: *{passed_filters}* ({pass_rate:.2f}%)\n"
        f"Куплено: *{total_bought}* ({buy_rate:.2f}%)\n"
    )

    # ── Rejection breakdown ────────────────────────────────────────────────────
    counts = _analytics.get("rejection_counts", {})
    if counts:
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        top = sorted_counts[:10]
        rej_lines = [f"\n*❌ Топ причин отклонения (за всё время):*"]
        for i, (cat, cnt) in enumerate(top, 1):
            pct = cnt / total_rej_ever * 100 if total_rej_ever else 0
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            rej_lines.append(f"{i}\\. *{cat}* — {pct:.1f}% ({cnt:,} шт.)")
        rejection_section = "\n".join(rej_lines)
    else:
        rejection_section = "\n*❌ Статистика отклонений:* пока нет данных"

    # ── Safety catches ─────────────────────────────────────────────────────────
    honeypot_caught    = counts.get("Honeypot", 0) + counts.get("Блокировка продажи", 0)
    honeypot_bought    = sum(1 for t in closed_trades if t.get("reason") == "Honeypot")
    rug_stuck          = sum(1 for t in closed_trades if t.get("reason") == "Honeypot")
    blacklisted_caught = counts.get("Деплоер (блеклист)", 0)
    serial_caught      = counts.get("Серийный деплоер", 0)

    safety = (
        f"\n*🛡️ Безопасность:*\n"
        f"Honeypot поймано фильтром: *{honeypot_caught}*\n"
        f"Куплено honeypot (прорвалось): *{honeypot_bought}*\n"
        f"Деплоер в блеклисте: *{blacklisted_caught}* заблок.\n"
        f"Серийный скамер: *{serial_caught}* заблок.\n"
    )

    # ── Trading performance ────────────────────────────────────────────────────
    if closed_trades:
        valid = [t for t in closed_trades if isinstance(t.get("pnl_pct"), (int, float))]
        wins  = [t for t in valid if t["pnl_pct"] > 0]
        losses= [t for t in valid if t["pnl_pct"] <= 0]
        wr    = len(wins) / len(valid) * 100 if valid else 0
        avg_pnl = sum(t["pnl_pct"] for t in valid) / len(valid) if valid else 0
        total_pnl_bnb = sum(t.get("pnl_bnb", 0) for t in valid)
        best  = max(valid, key=lambda t: t["pnl_pct"], default=None)
        worst = min(valid, key=lambda t: t["pnl_pct"], default=None)

        avg_hold_win  = (sum(t.get("hold_sec", 0) for t in wins)   / len(wins)   if wins   else 0)
        avg_hold_loss = (sum(t.get("hold_sec", 0) for t in losses) / len(losses) if losses else 0)
        fmt_time = lambda s: f"{int(s)//3600}ч {(int(s)%3600)//60}м" if s >= 3600 else f"{int(s)//60}м {int(s)%60}с"

        best_str  = f"+{best['pnl_pct']:.1f}% ({best.get('symbol','?')})"  if best  else "—"
        worst_str = f"{worst['pnl_pct']:.1f}% ({worst.get('symbol','?')})" if worst else "—"

        # Exit reason breakdown
        reasons: dict[str, list] = {}
        for t in valid:
            reasons.setdefault(t.get("reason", "?"), []).append(t["pnl_pct"])
        reason_lines = []
        for r, pnls in sorted(reasons.items(), key=lambda x: -len(x[1])):
            rwr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
            reason_lines.append(f"  {r}: {len(pnls)} сд., WR {rwr:.0f}%, avg {sum(pnls)/len(pnls):+.1f}%")

        speed_str = ""
        if _stats_delta_blocks:
            avg_d = sum(_stats_delta_blocks) / len(_stats_delta_blocks)
            speed_str = f"Avg блоков от листинга: *{avg_d:.1f}* (последние {len(_stats_delta_blocks)})\n"

        perf = (
            f"\n*💰 Торговля ({len(valid)} закрытых):*\n"
            f"Прибыльных: *{len(wins)}* | Убыточных: *{len(losses)}* | WR: *{wr:.0f}%*\n"
            f"Avg P&L: *{avg_pnl:+.1f}%* | Итого: *{total_pnl_bnb:+.4f} BNB*\n"
            f"Лучшая: {best_str}\n"
            f"Худшая: {worst_str}\n"
            f"Avg hold (win): {fmt_time(avg_hold_win)} | (loss): {fmt_time(avg_hold_loss)}\n"
            + speed_str +
            f"Открытых позиций: *{len(open_trades)}*\n"
            f"\n*Причины выхода:*\n" + "\n".join(reason_lines)
        )
    else:
        perf = "\n*💰 Торговля:* сделок пока нет"

    # ── Session stats ──────────────────────────────────────────────────────────
    session = (
        f"\n\n*📡 Сессия (с запуска):*\n"
        f"Обнаружено: {total_seen_now} | Отклонено: {total_rej_now}\n"
        f"Последовательных потерь: {_consecutive_losses}/{CIRCUIT_BREAKER_LOSSES}"
    )

    full = funnel + rejection_section + safety + perf + session
    # Telegram limit 4096 chars — split if needed
    for i in range(0, len(full), 4000):
        await update.message.reply_text(
            full[i:i+4000],
            parse_mode=ParseMode.MARKDOWN,
        )


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


# ── WebSocket watchdog ────────────────────────────────────────────────────────

_BOT_START_TIME = time.time()
_WS_ALERT_INTERVAL = 30 * 60   # repeat alert every 30 min while broken


async def _ws_watchdog():
    """
    Alert if no PairCreated events received for 15 minutes after startup.
    BSC normally produces dozens of new pairs per hour — silence = broken WS.
    """
    global _last_seen_ts
    await asyncio.sleep(10 * 60)   # give WS 10 min to connect and deliver first event
    while True:
        uptime = time.time() - _BOT_START_TIME
        since_last = time.time() - _last_seen_ts if _last_seen_ts else uptime

        if since_last > 10 * 60 and not is_paused:
            log.error(f"WS watchdog: no pairs seen for {since_last/60:.0f} min — alerting")
            try:
                await tg_send(
                    f"⚠️ *WebSocket не получает события!*\n\n"
                    f"Пар замечено за последние *{since_last/60:.0f} мин*: *0*\n"
                    f"На BSC обычно >10 пар/час — скорее всего WS-соединение оборвалось.\n\n"
                    f"Бот пытается переподключиться автоматически.\n"
                    f"Если проблема не исчезнет через 5 мин — перезапусти сервис на Railway."
                )
            except Exception as e:
                log.error(f"WS watchdog alert failed: {e}")
            await asyncio.sleep(_WS_ALERT_INTERVAL)
        else:
            await asyncio.sleep(5 * 60)   # check every 5 min when healthy


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
# After acquiring the lock, new instance waits this long before starting Telegram
# polling to let the old instance (which may still be polling) fully stop.
# Must be > LOCK_REFRESH_SEC so old instance has time to detect the lock change.
LOCK_POLLING_GRACE_SEC = LOCK_REFRESH_SEC + 8  # 15 s


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


def _acquire_distributed_lock() -> bool:
    """
    Block until we own the distributed lock or give up after LOCK_WAIT_SEC.
    Railway starts a new container before stopping the old one, so we poll
    until the old instance's lock expires (old instance stopped refreshing it).

    Returns True if we took the lock from ANOTHER host (need polling grace period).
    Returns False if we already owned the lock (restart, no grace period needed).
    """
    my_host  = socket.gethostname()
    deadline = time.time() + LOCK_WAIT_SEC
    while time.time() < deadline:
        data = _read_lock()
        age  = time.time() - data.get("ts", 0)
        host = data.get("host", "")
        if age >= LOCK_EXPIRY_SEC or host == my_host:
            # Lock is stale or belongs to us already → take it
            took_from_other = bool(host and host != my_host and age < LOCK_EXPIRY_SEC)
            _write_lock()
            log.info(f"Distributed lock acquired (previous age={age:.0f}s, host={host})")
            return took_from_other
        log.info(f"Waiting for previous instance to stop (lock age={age:.0f}s, host={host})…")
        time.sleep(3)
    # Timeout — take over anyway (better than not starting at all)
    log.warning("Lock wait timed out — taking over")
    _write_lock()
    return True  # assume there's another instance that may still be polling


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
        try:
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
        except (asyncio.CancelledError, SystemExit):
            raise
        except Exception as e:
            log.error(f"Lock refresher error (non-fatal): {e}")


async def _start_mempool_watcher():
    """
    Start the mempool watcher using NodeReal WS (the only free node that supports
    newPendingTransactions on BSC). Sends a Telegram alert if the node rejects
    the subscription (wrong tier) and stops retrying to avoid log spam.
    """
    global _mp_enabled
    # Prefer the NodeReal WS; fall back to the configured BSC WS
    mempool_ws = config._NR_WS or config.BSC_WS_RPC
    if not config.BSC_NODEREAL_KEY:
        await tg_send(
            "⚠️ *Mempool включён, но BSC\\_NODEREAL\\_KEY не задан*\n\n"
            "Публичные ноды не поддерживают `newPendingTransactions`.\n"
            "Добавь ключ NodeReal в Railway → Variables → BSC\\_NODEREAL\\_KEY\n"
            "_(Mempool бесполезен без NodeReal — отключён автоматически)_"
        )
        log.warning("MEMPOOL_ENABLED=true but BSC_NODEREAL_KEY not set — mempool disabled")
        return

    _mp_enabled = True
    await tg_send(
        f"🧠 *Mempool мониторинг запущен*\n"
        f"Нода: `{mempool_ws[:50]}…`\n"
        f"Токены будут проанализированы до создания пары — мгновенная покупка при листинге."
    )
    log.info(f"Mempool watcher starting on {mempool_ws[:60]}…")

    backoff = 5
    while True:
        try:
            await watch_pending_pairs(mempool_ws, on_pending_pair_found)
        except MemPoolNotSupportedError as e:
            _mp_enabled = False
            await tg_send(
                f"⚠️ *Мемпул не поддерживается нодой NodeReal*\n\n"
                f"`{e}`\n\n"
                f"Скорее всего тариф NodeReal не включает `newPendingTransactions`.\n"
                f"Нужен тариф *Growth* или выше (nodereal.io/pricing).\n"
                f"_Бот продолжает работу без мемпула._"
            )
            log.error(f"Mempool not supported: {e} — disabling permanently")
            return  # don't restart
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"Mempool watcher crashed: {e} — restarting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        else:
            backoff = 5  # reset on clean exit


async def _resilient_task(coro_factory, name: str, restart_delay: float = 5.0):
    """Run a coroutine factory in a loop, logging and restarting on any exception."""
    while True:
        try:
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"Task '{name}' crashed: {e}", exc_info=True)
            await asyncio.sleep(restart_delay)


# ── Backup polling: catch any pairs WebSocket might have missed ───────────────

_last_polled_block: int  = 0
_poll_consecutive_errors: int = 0   # how many polls in a row failed all RPCs
_poll_ok_rpc_idx:     int = 0       # index of last working RPC in _poll_w3s
_poll_last_ok_ts:     float = 0.0   # timestamp of last successful poll


async def _poll_new_pairs():
    """
    Polls eth_getLogs every 15 seconds across all BSC_HTTP_RPCS in round-robin.
    Deduplication via _is_token_duplicate prevents reprocessing WS-seen tokens.
    Acts as primary detection when WebSocket nodes are stale.
    """
    global _last_polled_block, _poll_consecutive_errors, _poll_ok_rpc_idx, _poll_last_ok_ts
    POLL_INTERVAL    = 5    # seconds between polls (was 15 — too slow for sniper)
    LOOK_BACK_BLOCKS = 200  # ~10 min catchup on restart (was 8 = 24s, too short)
    ALERT_AFTER      = 20   # alert to Telegram after this many consecutive all-RPC failures

    log.info(f"Poll task started — {len(_poll_w3s)} RPC(s), interval {POLL_INTERVAL}s")

    while True:
        await asyncio.sleep(POLL_INTERVAL)
        if is_paused:
            continue

        # Determine block range
        latest    = None
        logs      = None
        last_err  = ""

        # Try each RPC starting from last known-good one
        n = len(_poll_w3s)
        for i in range(n):
            idx   = (_poll_ok_rpc_idx + i) % n
            pw3   = _poll_w3s[idx]
            try:
                latest = await asyncio.to_thread(lambda _w=pw3: _w.eth.block_number)
                if _last_polled_block > 0:
                    from_block = _last_polled_block + 1
                else:
                    from_block = max(0, latest - LOOK_BACK_BLOCKS)
                if from_block > latest:
                    _last_polled_block = latest
                    logs = []
                    _poll_ok_rpc_idx = idx
                    break

                logs = await asyncio.to_thread(
                    pw3.eth.get_logs,
                    {
                        "address":   Web3.to_checksum_address(config.PANCAKE_FACTORY_V2),
                        "fromBlock": from_block,
                        "toBlock":   latest,
                        "topics":    [PAIR_CREATED_TOPIC],
                    },
                )
                _last_polled_block = latest
                _poll_ok_rpc_idx   = idx
                _poll_consecutive_errors = 0
                _poll_last_ok_ts   = time.time()
                rpc_short = config.BSC_HTTP_RPCS[idx][:35] if idx < len(config.BSC_HTTP_RPCS) else "?"
                if logs:
                    log.info(f"Poll[{idx}] {rpc_short}…: {len(logs)} events (blocks {from_block}–{latest})")
                break
            except Exception as e:
                last_err = str(e)
                log.warning(f"Poll[{idx}] RPC failed: {e}")

        if logs is None:
            # All RPCs failed this round
            _poll_consecutive_errors += 1
            log.error(f"Poll: all {n} RPCs failed (streak={_poll_consecutive_errors}). Last: {last_err}")
            if _poll_consecutive_errors == ALERT_AFTER:
                await tg_send(
                    f"🔴 *HTTP поллинг не работает* — все {n} RPC упали {ALERT_AFTER} раз подряд\n"
                    f"Последняя ошибка: `{last_err[:200]}`\n\n"
                    f"Пары не обнаруживаются ни через WS, ни через поллинг.\n"
                    f"Проверь Railway логи и статус RPC-нод."
                )
            continue

        for log_entry in logs:
            try:
                topics = log_entry.get("topics", [])
                if len(topics) < 3:
                    continue
                token0 = Web3.to_checksum_address(log_entry["topics"][1][-20:])
                token1 = Web3.to_checksum_address(log_entry["topics"][2][-20:])
                pair   = Web3.to_checksum_address(log_entry["data"][12:32])
                creation_block = log_entry.get("blockNumber", 0)
                if isinstance(creation_block, str):
                    creation_block = int(creation_block, 16)

                t0, t1 = token0.lower(), token1.lower()
                if t0 in config.BASE_TOKENS and t1 not in config.BASE_TOKENS:
                    new_token, base_token = token1, token0
                elif t1 in config.BASE_TOKENS and t0 not in config.BASE_TOKENS:
                    new_token, base_token = token0, token1
                else:
                    continue

                # Deduplicate against WS-seen pairs so HTTP poll and WebSocket
                # don't both process the same PairCreated event.
                async with _ws_seen_lock:
                    if pair in _ws_seen_pairs:
                        continue
                    _ws_seen_pairs.add(pair)

                asyncio.create_task(on_pair_found(new_token, base_token, pair, creation_block))
            except Exception as e:
                log.warning(f"Poll: log parse error: {e}")


async def main(need_polling_grace: bool = False):
    _load_history()
    _load_settings()
    _load_analytics()
    blacklist.load()
    log.info("Sniper Bot starting...")

    app = Application.builder().token(config.BOT_TOKEN).build()

    # Drop any webhook that might be set — prevents conflict with polling.
    await app.bot.delete_webhook(drop_pending_updates=True)

    # If we took the lock from another host, that instance may still be polling
    # Telegram for up to LOCK_REFRESH_SEC seconds. Wait before we start polling
    # to prevent HTTP 409 Conflict and NodeReal WS HTTP 429.
    if need_polling_grace:
        log.info(
            f"Polling grace period: waiting {LOCK_POLLING_GRACE_SEC}s "
            "for previous instance to stop polling..."
        )
        await asyncio.sleep(LOCK_POLLING_GRACE_SEC)

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
    app.add_handler(CommandHandler("rejects",   cmd_rejects))
    app.add_handler(CommandHandler("debug",     cmd_debug))
    app.add_handler(CommandHandler("analytics", cmd_analytics))
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

    asyncio.create_task(_resilient_task(pos_manager.monitor, "pos_monitor"))
    asyncio.create_task(_resilient_task(
        lambda: watch_pairs(config.BSC_WS_RPC, on_pair_found, ws_rpcs=config.BSC_WS_RPCS),
        "watch_pairs_bsc",
    ))
    asyncio.create_task(_resilient_task(_cleanup_pending, "cleanup_pending"))
    asyncio.create_task(_resilient_task(_daily_report, "daily_report"))
    asyncio.create_task(_resilient_task(_ws_watchdog, "ws_watchdog"))
    asyncio.create_task(_resilient_task(_poll_new_pairs, "poll_new_pairs"))

    if config.MEMPOOL_ENABLED:
        asyncio.create_task(_start_mempool_watcher())

    if config.BASE_CHAIN_ENABLED and pos_manager_base:
        asyncio.create_task(_resilient_task(pos_manager_base.monitor, "pos_monitor_base"))
        asyncio.create_task(_resilient_task(
            lambda: watch_pairs(
                config.BASE_WS_RPC,
                on_base_pair_found,
                factory_address=config.UNISWAP_V2_FACTORY_BASE,
                base_tokens=config.BASE_TOKENS_BASE,
                ws_rpcs=config.BASE_WS_RPCS,
            ),
            "watch_pairs_base",
        ))
        log.info(f"Base chain watcher started (Uniswap V2, factory={config.UNISWAP_V2_FACTORY_BASE[:10]}…)")

    if config.BISWAP_ENABLED and pos_manager_biswap:
        asyncio.create_task(_resilient_task(pos_manager_biswap.monitor, "pos_monitor_biswap"))
        asyncio.create_task(_resilient_task(
            lambda: watch_pairs(
                config.BSC_WS_RPC,
                on_biswap_pair_found,
                factory_address=config.BISWAP_FACTORY,
                base_tokens=config.BASE_TOKENS,
                ws_rpcs=config.BSC_WS_RPCS,
            ),
            "watch_pairs_biswap",
        ))
        log.info(f"BiSwap watcher started (factory={config.BISWAP_FACTORY[:10]}…)")

    if config.BASESWAP_ENABLED and pos_manager_baseswap and w3_base:
        asyncio.create_task(_resilient_task(pos_manager_baseswap.monitor, "pos_monitor_baseswap"))
        asyncio.create_task(_resilient_task(
            lambda: watch_pairs(
                config.BASE_WS_RPC,
                on_baseswap_pair_found,
                factory_address=config.BASESWAP_FACTORY_BASE,
                base_tokens=config.BASE_TOKENS_BASE,
                ws_rpcs=config.BASE_WS_RPCS,
            ),
            "watch_pairs_baseswap",
        ))
        log.info(f"BaseSwap watcher started (factory={config.BASESWAP_FACTORY_BASE[:10]}…)")

    asyncio.create_task(_lock_refresher(app))

    # Restore open positions from disk (survive restarts/redeploys)
    restored = pos_manager.load()
    if pos_manager_base:
        pos_manager_base.load()
    if pos_manager_biswap:
        pos_manager_biswap.load()
    if pos_manager_baseswap:
        pos_manager_baseswap.load()

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
    chains_parts = ["PancakeSwap V2 (BSC)"]
    if config.BISWAP_ENABLED and pos_manager_biswap:
        chains_parts.append("BiSwap V2 (BSC)")
    if config.BASE_CHAIN_ENABLED and pos_manager_base:
        chains_parts.append("Uniswap V2 (Base)")
    if config.BASESWAP_ENABLED and pos_manager_baseswap:
        chains_parts.append("BaseSwap V2 (Base)")
    chains_str = " + ".join(chains_parts)
    startup_msg = (
        "🚀 *Sniper Bot запущен*\n"
        f"Слежу за новыми парами на {chains_str}...{bl_note}\n\n"
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
    _need_grace = _acquire_distributed_lock()
    asyncio.run(main(need_polling_grace=_need_grace))
