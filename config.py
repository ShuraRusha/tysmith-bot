import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID", "")

# ── Wallet ────────────────────────────────────────────────────────────────────
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")

# ── BSC RPC ───────────────────────────────────────────────────────────────────
BSC_WS_RPC          = os.getenv("BSC_WS_RPC",          "wss://bsc-ws-node.nariox.org")
BSC_HTTP_RPC        = os.getenv("BSC_HTTP_RPC",        "https://bsc-dataseed.binance.org/")
BSC_HTTP_RPC_BACKUP = os.getenv("BSC_HTTP_RPC_BACKUP", "https://bsc-dataseed1.defibit.io/")

# ── Trading params ────────────────────────────────────────────────────────────
# Dynamic position sizing: buy BUY_PCT_OF_BALANCE % of wallet per trade
# Tiers (auto-applied):  balance ≤ 1 BNB → 5% | 1-5 BNB → 3% | >5 BNB → 2%
# Trade is skipped if calculated amount < BUY_MIN_BNB (gas would eat too much profit)
BUY_PCT_OF_BALANCE = float(os.getenv("BUY_PCT_OF_BALANCE", "0"))    # 0 = auto-tier
BUY_MIN_BNB        = float(os.getenv("BUY_MIN_BNB",        "0.03")) # skip trade if below
BUY_MAX_BNB        = float(os.getenv("BUY_MAX_BNB",        "0.5"))  # hard cap per trade
GAS_RESERVE_BNB    = float(os.getenv("GAS_RESERVE_BNB",    "0.015"))# always keep in wallet

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "10000"))
MAX_BUY_TAX       = float(os.getenv("MAX_BUY_TAX",       "5"))
MAX_SELL_TAX      = float(os.getenv("MAX_SELL_TAX",      "5"))

# ── Entry/Exit strategy ───────────────────────────────────────────────────────
# Phase 1 — fixed TP: sell TAKE_PROFIT_1_PCT% at TAKE_PROFIT_1% gain
TAKE_PROFIT_1     = float(os.getenv("TAKE_PROFIT_1",     "50"))   # % gain → partial exit
TAKE_PROFIT_1_PCT = float(os.getenv("TAKE_PROFIT_1_PCT", "25"))   # % of tokens to sell at TP1

# Phase 2 — trailing stop on remaining position after TP1
# Sells all remaining tokens if price drops TRAILING_STOP_PCT% from peak
TRAILING_STOP_PCT = float(os.getenv("TRAILING_STOP_PCT", "10"))   # % drop from peak → full exit

# Fixed stop loss before TP1 is reached
STOP_LOSS         = float(os.getenv("STOP_LOSS",         "15"))

# ── Execution params ──────────────────────────────────────────────────────────
SLIPPAGE_BUY      = float(os.getenv("SLIPPAGE_BUY",      "5"))    # % slippage tolerance on buy
SLIPPAGE_SELL     = float(os.getenv("SLIPPAGE_SELL",      "8"))    # % slippage tolerance on sell
GAS_MULTIPLIER    = float(os.getenv("GAS_MULTIPLIER",     "1.3"))
TX_DEADLINE_SEC   = int(os.getenv("TX_DEADLINE_SEC",      "60"))   # tx expiry in seconds

# ── Bot behaviour ─────────────────────────────────────────────────────────────
MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS",        "3"))    # max open positions at once
PENDING_TTL       = int(os.getenv("PENDING_TTL",          "60"))   # seconds before alert expires

# ── Safety filters ────────────────────────────────────────────────────────────
TOP_HOLDER_MAX_PCT = float(os.getenv("TOP_HOLDER_MAX_PCT", "30"))  # reject if single wallet > X%

# ── BSC contract addresses ────────────────────────────────────────────────────
WBNB  = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
BUSD  = "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"
USDT  = "0x55d398326f99059fF775485246999027B3197955"

PANCAKE_FACTORY_V2 = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_ROUTER_V2  = "0x10ED43C718714eb63d5aA57B78B54704E256024E"

# Tokens considered "base" — a new token must be paired with one of these
BASE_TOKENS = {WBNB.lower(), BUSD.lower(), USDT.lower()}
