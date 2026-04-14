import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID", "")

# ── Wallet ────────────────────────────────────────────────────────────────────
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")

# ── BSC RPC ───────────────────────────────────────────────────────────────────
# Priority: NodeReal (premium, fast) → public Binance → public backup
# Set BSC_NODEREAL_KEY in env to enable premium RPC (free tier: nodereal.io)
BSC_NODEREAL_KEY = os.getenv("BSC_NODEREAL_KEY", "")

if BSC_NODEREAL_KEY:
    _NR_HTTP = f"https://bsc-mainnet.nodereal.io/v1/{BSC_NODEREAL_KEY}"
    _NR_WS   = f"wss://bsc-mainnet.nodereal.io/ws/v1/{BSC_NODEREAL_KEY}"
else:
    _NR_HTTP = ""
    _NR_WS   = ""

BSC_WS_RPC          = os.getenv("BSC_WS_RPC",          _NR_WS or "wss://bsc-ws-node.nariox.org")
BSC_HTTP_RPC        = os.getenv("BSC_HTTP_RPC",        _NR_HTTP or "https://bsc-dataseed.binance.org/")
BSC_HTTP_RPC_BACKUP = os.getenv("BSC_HTTP_RPC_BACKUP", "https://bsc-dataseed1.defibit.io/")
# All HTTP endpoints for round-robin fallback (filtered to non-empty)
BSC_HTTP_RPCS = [u for u in [BSC_HTTP_RPC, BSC_HTTP_RPC_BACKUP,
                              "https://bsc-dataseed2.binance.org/",
                              "https://bsc-dataseed3.binance.org/"] if u]
# All WS endpoints for failover
BSC_WS_RPCS = [u for u in [BSC_WS_RPC,
                             "wss://bsc-ws-node.nariox.org"] if u]
# Deduplicate while preserving order
BSC_HTTP_RPCS = list(dict.fromkeys(BSC_HTTP_RPCS))
BSC_WS_RPCS   = list(dict.fromkeys(BSC_WS_RPCS))

# ── Trading params ────────────────────────────────────────────────────────────
# Dynamic position sizing: buy BUY_PCT_OF_BALANCE % of wallet per trade
# Tiers (auto-applied):  balance ≤ 1 BNB → 5% | 1-5 BNB → 3% | >5 BNB → 2%
# Trade is skipped if calculated amount < BUY_MIN_BNB (gas would eat too much profit)
BUY_PCT_OF_BALANCE = float(os.getenv("BUY_PCT_OF_BALANCE", "0"))    # 0 = auto-tier
BUY_MIN_BNB        = float(os.getenv("BUY_MIN_BNB",        "0.02")) # skip trade if below (газ BSC ~0.001-0.002 BNB, 0.02 = 10x газ)
BUY_MAX_BNB        = float(os.getenv("BUY_MAX_BNB",        "0.5"))  # hard cap per trade
GAS_RESERVE_BNB    = float(os.getenv("GAS_RESERVE_BNB",    "0.015"))# always keep in wallet

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "30000"))  # raised: pool liquidity floor
MAX_BUY_TAX       = float(os.getenv("MAX_BUY_TAX",       "5"))
MAX_SELL_TAX      = float(os.getenv("MAX_SELL_TAX",      "5"))

# ── Entry/Exit strategy ───────────────────────────────────────────────────────
# Phase 1 — fixed TP: sell TAKE_PROFIT_1_PCT% at TAKE_PROFIT_1% gain
TAKE_PROFIT_1     = float(os.getenv("TAKE_PROFIT_1",     "50"))   # % gain → partial exit
TAKE_PROFIT_1_PCT = float(os.getenv("TAKE_PROFIT_1_PCT", "50"))   # % of tokens to sell at TP1

# Phase 2 — trailing stop on remaining position after TP1
# Sells all remaining tokens if price drops TRAILING_STOP_PCT% from peak
TRAILING_STOP_PCT = float(os.getenv("TRAILING_STOP_PCT", "15"))   # % drop from peak → full exit

# Fixed stop loss before TP1 is reached
STOP_LOSS         = float(os.getenv("STOP_LOSS",         "20"))

# ── Execution params ──────────────────────────────────────────────────────────
# Sniper-optimized: aggressive buy slippage (new tokens are volatile),
# tighter sell slippage (selling into established pool)
SLIPPAGE_BUY      = float(os.getenv("SLIPPAGE_BUY",     "12"))    # % — aggressive for new tokens
SLIPPAGE_SELL     = float(os.getenv("SLIPPAGE_SELL",     "15"))    # % — wider to survive hidden taxes
GAS_MULTIPLIER    = float(os.getenv("GAS_MULTIPLIER",    "1.5"))   # outbid other buyers on gas
GAS_BUY_GWEI      = float(os.getenv("GAS_BUY_GWEI",      "5"))    # fixed gwei for buys (5 = fast on BSC)
GAS_LIMIT_BUY      = int(os.getenv("GAS_LIMIT_BUY",   "500000"))  # gas limit for buy txs
GAS_LIMIT_SELL     = int(os.getenv("GAS_LIMIT_SELL",   "350000"))  # gas limit for sell txs
GAS_LIMIT_APPROVE  = int(os.getenv("GAS_LIMIT_APPROVE","80000"))   # gas limit for approve txs
TX_DEADLINE_SEC   = int(os.getenv("TX_DEADLINE_SEC",     "30"))    # short deadline — reject stale

# ── Bot behaviour ─────────────────────────────────────────────────────────────
MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS",     "3"))    # manual mode cap
PENDING_TTL       = int(os.getenv("PENDING_TTL",       "60"))   # seconds before alert expires

# Auto-buy mode: bot buys immediately without user confirmation
# MAX_AUTO_POSITIONS=0 → calculated automatically from balance tier (recommended)
AUTO_BUY           = os.getenv("AUTO_BUY",           "false").lower() == "true"
MAX_AUTO_POSITIONS = int(os.getenv("MAX_AUTO_POSITIONS", "0"))  # 0 = auto formula

# ── Safety filters ────────────────────────────────────────────────────────────
TOP_HOLDER_MAX_PCT   = float(os.getenv("TOP_HOLDER_MAX_PCT",   "30"))  # reject if single wallet > X%
MAX_TOP10_HOLDER_PCT = float(os.getenv("MAX_TOP10_HOLDER_PCT", "30"))  # top-10 combined (excl. DEX/locked) > X% → reject
LP_HOLDER_MAX_PCT    = float(os.getenv("LP_HOLDER_MAX_PCT",    "30"))  # reject if any unlocked wallet holds >X% of LP
MIN_HOLDER_COUNT     = int(os.getenv("MIN_HOLDER_COUNT",       "50"))  # min token holder count (GoPlus)

# ── Token quality filters ─────────────────────────────────────────────────────
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD",   "50000"))    # min market cap at buy time
MIN_FDV_USD        = float(os.getenv("MIN_FDV_USD",          "300000"))   # min fully-diluted value
MAX_FDV_USD        = float(os.getenv("MAX_FDV_USD",          "10000000")) # max FDV (avoid huge caps)
MIN_VOLUME_5M_USD  = float(os.getenv("MIN_VOLUME_5M_USD",    "3000"))     # DexScreener 5-min volume
MAX_TOKEN_AGE_DAYS = int(os.getenv("MAX_TOKEN_AGE_DAYS",     "7"))        # reject tokens older than this

# ── Moon bag ──────────────────────────────────────────────────────────────────
# When trade size >= MOON_BAG_MIN_USD, keep MOON_BAG_PCT% of tokens as a
# long-term hold that is NOT sold at TP/SL — manual sell only (potential 100x).
MOON_BAG_MIN_USD = float(os.getenv("MOON_BAG_MIN_USD", "100"))  # activate when trade >= $100
MOON_BAG_PCT     = float(os.getenv("MOON_BAG_PCT",     "5"))    # % of bought tokens to keep

# ── BSC contract addresses ────────────────────────────────────────────────────
WBNB  = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
BUSD  = "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"
USDT  = "0x55d398326f99059fF775485246999027B3197955"

PANCAKE_FACTORY_V2 = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_ROUTER_V2  = "0x10ED43C718714eb63d5aA57B78B54704E256024E"

# Tokens considered "base" — a new token must be paired with one of these
BASE_TOKENS = {WBNB.lower(), BUSD.lower(), USDT.lower()}
