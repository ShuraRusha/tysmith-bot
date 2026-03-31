import time
import logging

from web3 import Web3

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

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _gas_price(self) -> int:
        return int(self.w3.eth.gas_price * self.gas_multiplier)

    def _nonce(self) -> int:
        return self.w3.eth.get_transaction_count(self.wallet, "pending")

    def _deadline(self) -> int:
        return int(time.time()) + TX_DEADLINE_SEC  # short window — reject stale txs

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
        """Return True if wallet has enough BNB for the trade + gas reserve."""
        balance = self.w3.eth.get_balance(self.wallet) / 1e18
        return balance >= amount_bnb + gas_reserve

    # ── Pre-approve router (call right after buy to save time on sell) ─────────

    def approve_token(self, token_address: str) -> bool:
        """Approve PancakeSwap router to spend this token. Synchronous."""
        try:
            token_address = Web3.to_checksum_address(token_address)
            token     = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
            allowance = token.functions.allowance(self.wallet, ROUTER_ADDRESS).call()
            if allowance > 0:
                return True  # already approved
            approve_tx = token.functions.approve(
                ROUTER_ADDRESS, 2 ** 256 - 1
            ).build_transaction({
                "from":     self.wallet,
                "gas":      100_000,
                "gasPrice": self._gas_price(),
                "nonce":    self._nonce(),
                "chainId":  56,
            })
            signed  = self.account.sign_transaction(approve_tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            log.info(f"Pre-approved {token_address}")
            return True
        except Exception as e:
            log.error(f"approve_token({token_address}): {e}")
            return False

    # ── Buy ───────────────────────────────────────────────────────────────────

    def buy(self, token_address: str, amount_bnb: float) -> dict:
        """
        Buy token with BNB via PancakeSwap V2.
        Synchronous — call via asyncio.to_thread from async code.

        Returns:
            {"ok": True,  "tx_hash": "...", "tokens_received": int, "decimals": int}
            {"ok": False, "reason": "..."}
        """
        try:
            token_address = Web3.to_checksum_address(token_address)
            amount_wei    = Web3.to_wei(amount_bnb, "ether")

            # Expected output with buy slippage tolerance
            amounts  = self.router.functions.getAmountsOut(
                amount_wei, [WBNB_ADDRESS, token_address]
            ).call()
            min_out  = int(amounts[1] * (1 - SLIPPAGE_BUY / 100))

            tx = self.router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
                min_out,
                [WBNB_ADDRESS, token_address],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "value":    amount_wei,
                "gas":      300_000,
                "gasPrice": self._gas_price(),
                "nonce":    self._nonce(),
                "chainId":  56,
            })

            signed  = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status != 1:
                return {"ok": False, "reason": "Транзакция отклонена сетью (status=0)"}

            token    = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
            balance  = token.functions.balanceOf(self.wallet).call()
            decimals = token.functions.decimals().call()

            log.info(f"Buy OK: {token_address}, received={balance}, tx={tx_hash.hex()}")
            return {
                "ok":              True,
                "tx_hash":         tx_hash.hex(),
                "tokens_received": balance,
                "decimals":        decimals,
            }
        except Exception as e:
            log.error(f"buy({token_address}): {e}")
            return {"ok": False, "reason": str(e)}

    # ── Sell ──────────────────────────────────────────────────────────────────

    def sell(self, token_address: str, amount_tokens: int) -> dict:
        """
        Sell exact token amount back to BNB via PancakeSwap V2.
        Handles approve automatically.
        Synchronous — call via asyncio.to_thread from async code.

        Returns:
            {"ok": True,  "tx_hash": "..."}
            {"ok": False, "reason": "..."}
        """
        try:
            token_address = Web3.to_checksum_address(token_address)

            if amount_tokens <= 0:
                return {"ok": False, "reason": "Нет токенов для продажи"}

            token = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)

            # Approve router if allowance is insufficient
            allowance = token.functions.allowance(self.wallet, ROUTER_ADDRESS).call()
            if allowance < amount_tokens:
                approve_tx = token.functions.approve(
                    ROUTER_ADDRESS, 2 ** 256 - 1
                ).build_transaction({
                    "from":     self.wallet,
                    "gas":      100_000,
                    "gasPrice": self._gas_price(),
                    "nonce":    self._nonce(),
                    "chainId":  56,
                })
                signed  = self.account.sign_transaction(approve_tx)
                tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                log.info(f"Approved {token_address}")

            # Expected BNB output with sell slippage tolerance (tighter than buy)
            amounts = self.router.functions.getAmountsOut(
                amount_tokens, [token_address, WBNB_ADDRESS]
            ).call()
            min_out = int(amounts[1] * (1 - SLIPPAGE_SELL / 100))

            tx = self.router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
                amount_tokens,
                min_out,
                [token_address, WBNB_ADDRESS],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "gas":      300_000,
                "gasPrice": self._gas_price(),
                "nonce":    self._nonce(),
                "chainId":  56,
            })

            signed  = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status != 1:
                return {"ok": False, "reason": "Продажа отклонена сетью (status=0)"}

            log.info(f"Sell OK: {token_address}, tx={tx_hash.hex()}")
            return {"ok": True, "tx_hash": tx_hash.hex()}

        except Exception as e:
            log.error(f"sell({token_address}): {e}")
            return {"ok": False, "reason": str(e)}
