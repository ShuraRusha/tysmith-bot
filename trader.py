import time
import logging
import threading

from web3 import Web3

import config
from config import PANCAKE_ROUTER_V2, WBNB, SLIPPAGE_BUY, SLIPPAGE_SELL, TX_DEADLINE_SEC

log = logging.getLogger(__name__)

ROUTER_ADDRESS = Web3.to_checksum_address(PANCAKE_ROUTER_V2)
WBNB_ADDRESS   = Web3.to_checksum_address(WBNB)

ROUTER_ABI = [
    {
        "name": "swapExactETHForTokensSupportingFeeOnTransferTokens",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path",         "type": "address[]"},
            {"name": "to",           "type": "address"},
            {"name": "deadline",     "type": "uint256"},
        ],
        "outputs": [],
    },
    {
        "name": "swapExactTokensForETHSupportingFeeOnTransferTokens",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn",     "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path",         "type": "address[]"},
            {"name": "to",           "type": "address"},
            {"name": "deadline",     "type": "uint256"},
        ],
        "outputs": [],
    },
    {
        "name": "getAmountsOut",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path",     "type": "address[]"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
]

ERC20_ABI = [
    {
        "name": "approve",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount",  "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs":  [{"name": "account", "type": "address"}],
        "outputs": [{"name": "",        "type": "uint256"}],
    },
    {
        "name": "decimals",
        "type": "function",
        "stateMutability": "view",
        "inputs":  [],
        "outputs": [{"name": "", "type": "uint8"}],
    },
    {
        "name": "allowance",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "owner",   "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]


class Trader:
    def __init__(self, w3: Web3, private_key: str, gas_multiplier: float):
        self.w3             = w3
        self.account        = w3.eth.account.from_key(private_key)
        self.wallet         = self.account.address
        self.router         = w3.eth.contract(address=ROUTER_ADDRESS, abi=ROUTER_ABI)
        self.gas_multiplier = gas_multiplier
        self._nonce_lock    = threading.Lock()
        self._nonce_val     = None   # cached nonce, None = fetch fresh on first use

    # ── Gas helpers ──────────────────────────────────────────────────────────

    def _gas_price_buy(self) -> int:
        """Aggressive gas for buy — speed is critical to get in first."""
        if config.GAS_BUY_GWEI > 0:
            return Web3.to_wei(config.GAS_BUY_GWEI, "gwei")
        return int(self.w3.eth.gas_price * self.gas_multiplier)

    def _gas_price_normal(self) -> int:
        """Standard gas for sell/approve — speed is less critical."""
        return int(self.w3.eth.gas_price * 1.1)

    def _nonce(self) -> int:
        """
        Thread-safe nonce manager.
        Fetches from network on first call, then increments locally.
        Resyncs from network if a transaction fails (call reset_nonce()).
        Prevents nonce conflicts when multiple tokens are bought in parallel.
        """
        with self._nonce_lock:
            if self._nonce_val is None:
                self._nonce_val = self.w3.eth.get_transaction_count(
                    self.wallet, "pending"
                )
            nonce = self._nonce_val
            self._nonce_val += 1
            return nonce

    def reset_nonce(self):
        """Force re-fetch nonce from network on next transaction (call after errors)."""
        with self._nonce_lock:
            self._nonce_val = None

    def _deadline(self) -> int:
        return int(time.time()) + TX_DEADLINE_SEC

    # ── Price ─────────────────────────────────────────────────────────────────

    def get_price(self, token_address: str, base: str = None) -> float:
        """Return price of 1 token in BNB. Synchronous — use asyncio.to_thread."""
        if base is None:
            base = WBNB_ADDRESS
        try:
            token    = self.w3.eth.contract(
                address=Web3.to_checksum_address(token_address), abi=ERC20_ABI
            )
            decimals = token.functions.decimals().call()
            amounts  = self.router.functions.getAmountsOut(
                10 ** decimals,
                [Web3.to_checksum_address(token_address), Web3.to_checksum_address(base)],
            ).call()
            return amounts[1] / 1e18
        except Exception as e:
            log.error(f"get_price({token_address}): {e}")
            return 0.0

    # ── Balance check ─────────────────────────────────────────────────────────

    def has_enough_bnb(self, amount_bnb: float, gas_reserve: float = 0.005) -> bool:
        balance = self.w3.eth.get_balance(self.wallet) / 1e18
        return balance >= amount_bnb + gas_reserve

    # ── Pre-approve router ────────────────────────────────────────────────────

    def approve_token(self, token_address: str) -> dict:
        """
        Approve PancakeSwap router to spend this token. Synchronous.
        Returns {"ok": True} or {"ok": False, "reason": "..."}
        """
        try:
            token_address = Web3.to_checksum_address(token_address)
            token     = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
            allowance = token.functions.allowance(self.wallet, ROUTER_ADDRESS).call()
            if allowance > 0:
                return {"ok": True}
            approve_tx = token.functions.approve(
                ROUTER_ADDRESS, 2 ** 256 - 1
            ).build_transaction({
                "from":     self.wallet,
                "gas":      config.GAS_LIMIT_APPROVE,
                "gasPrice": self._gas_price_buy(),
                "nonce":    self._nonce(),
                "chainId":  56,
            })
            signed  = self.account.sign_transaction(approve_tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            log.info(f"Approved {token_address}")
            return {"ok": True}
        except Exception as e:
            log.error(f"approve_token({token_address}): {e}")
            self.reset_nonce()
            return {"ok": False, "reason": str(e)}

    # ── Buy ───────────────────────────────────────────────────────────────────

    def buy(self, token_address: str, amount_bnb: float) -> dict:
        """
        Buy token with BNB via PancakeSwap V2.
        Uses aggressive gas price + high slippage for sniper speed.
        Synchronous — call via asyncio.to_thread from async code.
        """
        try:
            token_address = Web3.to_checksum_address(token_address)
            amount_wei    = Web3.to_wei(amount_bnb, "ether")

            amounts  = self.router.functions.getAmountsOut(
                amount_wei, [WBNB_ADDRESS, token_address]
            ).call()
            min_out  = int(amounts[1] * (1 - SLIPPAGE_BUY / 100))

            gas_price = self._gas_price_buy()
            log.info(f"Buy {token_address}: {amount_bnb} BNB, "
                     f"gasPrice={gas_price / 1e9:.1f} gwei, "
                     f"slippage={SLIPPAGE_BUY}%")

            tx = self.router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
                min_out,
                [WBNB_ADDRESS, token_address],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "value":    amount_wei,
                "gas":      config.GAS_LIMIT_BUY,
                "gasPrice": gas_price,
                "nonce":    self._nonce(),
                "chainId":  56,
            })

            signed  = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

            if receipt.status != 1:
                return {"ok": False, "reason": "Транзакция отклонена сетью (status=0)"}

            token    = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
            balance  = token.functions.balanceOf(self.wallet).call()
            decimals = token.functions.decimals().call()

            gas_used_bnb = receipt.gasUsed * gas_price / 1e18
            log.info(f"Buy OK: {token_address}, received={balance}, "
                     f"gas={gas_used_bnb:.4f} BNB, tx={tx_hash.hex()}")
            return {
                "ok":              True,
                "tx_hash":         tx_hash.hex(),
                "tokens_received": balance,
                "decimals":        decimals,
            }
        except Exception as e:
            log.error(f"buy({token_address}): {e}")
            self.reset_nonce()
            return {"ok": False, "reason": str(e)}

    # ── Sell ──────────────────────────────────────────────────────────────────

    def sell(self, token_address: str, amount_tokens: int,
             slippage_pct: float = None) -> dict:
        """
        Sell exact token amount back to BNB via PancakeSwap V2.
        On status=0 failure, automatically retries with 2x slippage.
        Synchronous — call via asyncio.to_thread from async code.
        """
        if slippage_pct is None:
            slippage_pct = SLIPPAGE_SELL

        try:
            token_address = Web3.to_checksum_address(token_address)

            if amount_tokens <= 0:
                return {"ok": False, "reason": "Нет токенов для продажи"}

            token = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)

            allowance = token.functions.allowance(self.wallet, ROUTER_ADDRESS).call()
            if allowance < amount_tokens:
                approve_tx = token.functions.approve(
                    ROUTER_ADDRESS, 2 ** 256 - 1
                ).build_transaction({
                    "from":     self.wallet,
                    "gas":      config.GAS_LIMIT_APPROVE,
                    "gasPrice": self._gas_price_normal(),
                    "nonce":    self._nonce(),
                    "chainId":  56,
                })
                signed  = self.account.sign_transaction(approve_tx)
                tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
                self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                log.info(f"Approved {token_address}")

            amounts = self.router.functions.getAmountsOut(
                amount_tokens, [token_address, WBNB_ADDRESS]
            ).call()
            min_out = int(amounts[1] * (1 - slippage_pct / 100))

            tx = self.router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
                amount_tokens,
                min_out,
                [token_address, WBNB_ADDRESS],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "gas":      config.GAS_LIMIT_SELL,
                "gasPrice": self._gas_price_normal(),
                "nonce":    self._nonce(),
                "chainId":  56,
            })

            signed  = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

            if receipt.status != 1:
                # One retry at max slippage (49%) — avoids burning 3 tx fees for honeypots.
                # Intermediate steps (15%→30%→49%) waste gas without helping.
                if slippage_pct < 49:
                    log.warning(f"Sell status=0 at {slippage_pct}%, retrying at 49%")
                    return self.sell(token_address, amount_tokens, slippage_pct=49)
                return {"ok": False, "reason": f"Продажа отклонена сетью (slip={slippage_pct:.0f}%)"}

            log.info(f"Sell OK: {token_address}, slippage={slippage_pct}%, tx={tx_hash.hex()}")
            return {"ok": True, "tx_hash": tx_hash.hex()}

        except Exception as e:
            log.error(f"sell({token_address}): {e}")
            self.reset_nonce()
            return {"ok": False, "reason": str(e)}
