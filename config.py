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
BUY_AMOUNT_BNB    = float(os.getenv("BUY_AMOUNT_BNB",    "0.02"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "10000"))
MAX_BUY_TAX       = float(os.getenv("MAX_BUY_TAX",       "5"))
MAX_SELL_TAX      = float(os.getenv("MAX_SELL_TAX",      "5"))
# Dual take-profit: sell TP1_PCT% of position at TP1, rest at TP2
TAKE_PROFIT_1     = float(os.getenv("TAKE_PROFIT_1",     "80"))   # % gain → first exit
TAKE_PROFIT_1_PCT = float(os.getenv("TAKE_PROFIT_1_PCT", "50"))   # % of tokens to sell
TAKE_PROFIT_2     = float(os.getenv("TAKE_PROFIT_2",     "200"))  # % gain → full exit
STOP_LOSS         = float(os.getenv("STOP_LOSS",         "20"))
SLIPPAGE          = float(os.getenv("SLIPPAGE",          "15"))
GAS_MULTIPLIER    = float(os.getenv("GAS_MULTIPLIER",    "1.3"))

# ── BSC contract addresses ────────────────────────────────────────────────────
WBNB  = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
BUSD  = "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"
USDT  = "0x55d398326f99059fF775485246999027B3197955"

PANCAKE_FACTORY_V2 = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_ROUTER_V2  = "0x10ED43C718714eb63d5aA57B78B54704E256024E"

# Tokens considered "base" — a new token must be paired with one of these
BASE_TOKENS = {WBNB.lower(), BUSD.lower(), USDT.lower()}
