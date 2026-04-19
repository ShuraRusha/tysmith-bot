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

BSC_WS_RPC          = os.getenv("BSC_WS_RPC",          _NR_WS or "wss://bsc.publicnode.com")
BSC_HTTP_RPC        = os.getenv("BSC_HTTP_RPC",        _NR_HTTP or "https://bsc-dataseed.binance.org/")
BSC_HTTP_RPC_BACKUP = os.getenv("BSC_HTTP_RPC_BACKUP", "https://bsc-dataseed1.defibit.io/")
# All HTTP endpoints for round-robin fallback (filtered to non-empty)
BSC_HTTP_RPCS = [u for u in [BSC_HTTP_RPC, BSC_HTTP_RPC_BACKUP,
                              "https://bsc-dataseed2.binance.org/",
                              "https://bsc-dataseed3.binance.org/"] if u]
# All WS endpoints for failover — multiple endpoints for redundancy
# bsc.publicnode.com is the most reliable free public BSC WS node
BSC_WS_RPCS = [u for u in [
    BSC_WS_RPC,
    "wss://bsc.publicnode.com",
    "wss://bsc-rpc.publicnode.com",
    "wss://bsc-ws-node.nariox.org",
] if u]
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

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "10000"))  # pool liquidity floor
MAX_BUY_TAX       = float(os.getenv("MAX_BUY_TAX",       "10"))   # 10% — honeypot.is blocks anything above (real honeypots are 20-99%)
MAX_SELL_TAX      = float(os.getenv("MAX_SELL_TAX",      "10"))   # 10% — GoPlus also checks; real honeypots won't pass sell simulation

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

# ── External APIs ────────────────────────────────────────────────────────────
# BSCScan API key — used for deployer history check (serial scam deployer detection)
# Free tier at bscscan.com/myapikey — 5 req/s, enough for this bot
# If not set, deployer check is skipped (fail-open).
BSCSCAN_API_KEY          = os.getenv("BSCSCAN_API_KEY", "")
# Basescan API key for Base chain deployer history (free at basescan.org/myapikey)
BASESCAN_API_KEY         = os.getenv("BASESCAN_API_KEY", "")
MAX_DEPLOYER_TOKENS_30D  = int(os.getenv("MAX_DEPLOYER_TOKENS_30D", "3"))  # >N contracts/30d = serial scammer

# ── Position monitoring ──────────────────────────────────────────────────────
MONITOR_INTERVAL_SEC    = float(os.getenv("MONITOR_INTERVAL_SEC",    "1"))   # price check frequency (was 5s, now 1s for memecoins)

# ── Gas escalation on sell (RBF — Replace-By-Fee) ────────────────────────────
# If a sell tx is stuck in the mempool, the bot resends it with the same nonce
# but a higher gas price every GAS_SELL_ESCALATION_SEC seconds.
# Schedule: gas*1.5 → gas*3.0 → GAS_SELL_MAX_GWEI (3 attempts total).
GAS_SELL_MAX_GWEI       = float(os.getenv("GAS_SELL_MAX_GWEI",      "30"))   # gwei ceiling for 3rd escalation attempt
GAS_SELL_ESCALATION_SEC = float(os.getenv("GAS_SELL_ESCALATION_SEC", "15"))  # seconds between escalation attempts

# ── Mempool monitoring ────────────────────────────────────────────────────────
# Subscribe to newPendingTransactions to detect createPair() calls before
# they are mined. Pre-analysis caches results so on_pair_found has 0 delay.
# Disabled by default — requires a node with full mempool access (e.g. NodeReal).
MEMPOOL_ENABLED = os.getenv("MEMPOOL_ENABLED", "false").lower() == "true"

# ── Bot behaviour ─────────────────────────────────────────────────────────────
MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS",     "3"))    # manual mode cap
PENDING_TTL       = int(os.getenv("PENDING_TTL",       "60"))   # seconds before alert expires

# Auto-buy mode: bot buys immediately without user confirmation
# DEFAULT=true — bot starts in auto mode; set AUTO_BUY=false to start in manual mode
# MAX_AUTO_POSITIONS=0 → calculated automatically from balance tier (recommended)
AUTO_BUY           = os.getenv("AUTO_BUY",           "true").lower() == "true"
MAX_AUTO_POSITIONS = int(os.getenv("MAX_AUTO_POSITIONS", "0"))  # 0 = auto formula

# ── Safety filters ────────────────────────────────────────────────────────────
TOP_HOLDER_MAX_PCT   = float(os.getenv("TOP_HOLDER_MAX_PCT",   "30"))  # reject if single wallet > X%
MAX_TOP10_HOLDER_PCT = float(os.getenv("MAX_TOP10_HOLDER_PCT", "30"))  # top-10 combined (excl. DEX/locked) > X% → reject
LP_HOLDER_MAX_PCT    = float(os.getenv("LP_HOLDER_MAX_PCT",    "30"))  # reject if any unlocked wallet holds >X% of LP
MIN_HOLDER_COUNT     = int(os.getenv("MIN_HOLDER_COUNT",       "0"))   # 0 = disabled — sniper enters first, 1-3 holders is normal at launch

# ── Token quality filters ─────────────────────────────────────────────────────
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD",   "10000"))    # min market cap at buy time
MIN_FDV_USD        = float(os.getenv("MIN_FDV_USD",          "50000"))    # min fully-diluted value (real safety = honeypot sim + LP check)
MAX_FDV_USD        = float(os.getenv("MAX_FDV_USD",          "10000000")) # max FDV (avoid huge caps)
MIN_VOLUME_5M_USD  = float(os.getenv("MIN_VOLUME_5M_USD",    "500"))      # DexScreener 5-min volume (low — fresh tokens haven't accumulated volume yet)
MAX_TOKEN_AGE_DAYS = int(os.getenv("MAX_TOKEN_AGE_DAYS",     "7"))        # reject tokens older than this

# ── Moon bag ──────────────────────────────────────────────────────────────────
# When trade size >= MOON_BAG_MIN_USD, keep MOON_BAG_PCT% of tokens as a
# long-term hold that is NOT sold at TP/SL — manual sell only (potential 100x).
MOON_BAG_MIN_USD = float(os.getenv("MOON_BAG_MIN_USD", "100"))  # activate when trade >= $100
MOON_BAG_PCT     = float(os.getenv("MOON_BAG_PCT",     "5"))    # % of bought tokens to keep

# ── BiSwap V2 (BSC) ──────────────────────────────────────────────────────────
# Enable via BISWAP_ENABLED=true; runs in parallel with PancakeSwap
BISWAP_ENABLED  = os.getenv("BISWAP_ENABLED", "false").lower() == "true"
BISWAP_FACTORY  = "0x858E3312ed3A876947EA49d572A7C42DE08af7EE"
BISWAP_ROUTER   = "0x3a6d8cA21D1CF76F653A67577FA0D27453350dD8"
# BiSwap V2 INIT_CODE_HASH (used only by mempool watcher for CREATE2 prediction)
BISWAP_INIT_CODE_HASH = "fea293c909d87cd4153593f077b76bb7e94340200f4ee84211ae8e4f9bd7ffdf"

# ── BaseSwap V2 (Base) ────────────────────────────────────────────────────────
# Enable via BASESWAP_ENABLED=true; requires BASE_CHAIN_ENABLED=true
BASESWAP_ENABLED      = os.getenv("BASESWAP_ENABLED", "false").lower() == "true"
BASESWAP_FACTORY_BASE = "0xFDa619b6d20975be80A10332cD39b9a4b0FAa8BB"
BASESWAP_ROUTER_BASE  = "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86"
# BaseSwap init code hash (Uniswap V2 fork, different bytecode from UniswapV2)
BASESWAP_INIT_CODE_HASH_BASE = "b618a2730fae167f5f8ac7bd659dd8436d571872655bcb6fd11f2158c8a64a3b"

# ── BSC contract addresses ────────────────────────────────────────────────────
WBNB  = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
BUSD  = "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"
USDT  = "0x55d398326f99059fF775485246999027B3197955"

PANCAKE_FACTORY_V2 = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_ROUTER_V2  = "0x10ED43C718714eb63d5aA57B78B54704E256024E"

# Tokens considered "base" — a new token must be paired with one of these
BASE_TOKENS = {WBNB.lower(), BUSD.lower(), USDT.lower()}

# ── Base chain (Uniswap V2) ───────────────────────────────────────────────────
# Enable via BASE_CHAIN_ENABLED=true env var; requires BASE_WS_RPC (public or Alchemy)
BASE_CHAIN_ENABLED = os.getenv("BASE_CHAIN_ENABLED", "false").lower() == "true"
BASE_CHAIN_ID      = 8453

# RPC endpoints for Base
BASE_HTTP_RPC        = os.getenv("BASE_HTTP_RPC",        "https://mainnet.base.org")
BASE_HTTP_RPC_BACKUP = os.getenv("BASE_HTTP_RPC_BACKUP", "https://base.publicnode.com")
BASE_WS_RPC          = os.getenv("BASE_WS_RPC",          "wss://base.publicnode.com/websocket")

BASE_HTTP_RPCS = list(dict.fromkeys(
    [u for u in [BASE_HTTP_RPC, BASE_HTTP_RPC_BACKUP,
                 "https://base-mainnet.g.alchemy.com/v2/demo"] if u]
))
BASE_WS_RPCS = [BASE_WS_RPC] if BASE_WS_RPC else []

# Base native token + main stablecoin
WETH_BASE = "0x4200000000000000000000000000000000000006"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

# Uniswap V2 on Base (same interface as PancakeSwap V2)
UNISWAP_V2_FACTORY_BASE       = "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"
UNISWAP_V2_ROUTER_BASE        = "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24"
UNISWAP_V2_INIT_CODE_HASH_BASE = "96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f"

# Base tokens paired against new tokens on Base
BASE_TOKENS_BASE = {WETH_BASE.lower(), USDC_BASE.lower()}
