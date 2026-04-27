import time
import logging
import threading

from web3 import Web3

import config
from config import PANCAKE_ROUTER_V2, WBNB, SLIPPAGE_BUY, SLIPPAGE_SELL, TX_DEADLINE_SEC

log = logging.getLogger(__name__)

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
    def __init__(
        self,
        w3: Web3,
        private_key: str,
        gas_multiplier: float,
        chain_id: int = 56,
        router_address: str = None,
        native_token: str = None,
    ):
        self.w3             = w3
        self.chain_id       = chain_id
        self.router_address = Web3.to_checksum_address(router_address or PANCAKE_ROUTER_V2)
        self.native_token   = Web3.to_checksum_address(native_token or WBNB)
        self.account        = w3.eth.account.from_key(private_key)
        self.wallet         = self.account.address
        self.router         = w3.eth.contract(address=self.router_address, abi=ROUTER_ABI)
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
        """Return price of 1 token in native token (BNB/ETH). Synchronous — use asyncio.to_thread."""
        if base is None:
            base = self.native_token
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
            log.debug(f"get_price({token_address}): {e}")
            return 0.0

    # ── Balance check ─────────────────────────────────────────────────────────

    def has_enough_bnb(self, amount_bnb: float, gas_reserve: float = 0.005) -> bool:
        balance = self.w3.eth.get_balance(self.wallet) / 1e18
        return balance >= amount_bnb + gas_reserve

    # ── Pre-approve router ────────────────────────────────────────────────────

    def approve_token(self, token_address: str) -> dict:
        """
        Approve router to spend this token. Synchronous.
        Returns {"ok": True} or {"ok": False, "reason": "..."}
        """
        try:
            token_address = Web3.to_checksum_address(token_address)
            token     = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
            allowance = token.functions.allowance(self.wallet, self.router_address).call()
            if allowance > 0:
                return {"ok": True}
            approve_tx = token.functions.approve(
                self.router_address, 2 ** 256 - 1
            ).build_transaction({
                "from":     self.wallet,
                "gas":      config.GAS_LIMIT_APPROVE,
                "gasPrice": self._gas_price_buy(),
                "nonce":    self._nonce(),
                "chainId":  self.chain_id,
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
                amount_wei, [self.native_token, token_address]
            ).call()
            min_out  = int(amounts[1] * (1 - SLIPPAGE_BUY / 100))

            gas_price = self._gas_price_buy()
            log.info(f"Buy {token_address}: {amount_bnb} native, "
                     f"gasPrice={gas_price / 1e9:.1f} gwei, "
                     f"slippage={SLIPPAGE_BUY}%")

            tx = self.router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
                min_out,
                [self.native_token, token_address],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "value":    amount_wei,
                "gas":      config.GAS_LIMIT_BUY,
                "gasPrice": gas_price,
                "nonce":    self._nonce(),
                "chainId":  self.chain_id,
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
                "block_number":    receipt.blockNumber,
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

            allowance = token.functions.allowance(self.wallet, self.router_address).call()
            if allowance < amount_tokens:
                approve_tx = token.functions.approve(
                    self.router_address, 2 ** 256 - 1
                ).build_transaction({
                    "from":     self.wallet,
                    "gas":      config.GAS_LIMIT_APPROVE,
                    "gasPrice": self._gas_price_normal(),
                    "nonce":    self._nonce(),
                    "chainId":  self.chain_id,
                })
                signed  = self.account.sign_transaction(approve_tx)
                tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
                self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                log.info(f"Approved {token_address}")

            amounts = self.router.functions.getAmountsOut(
                amount_tokens, [token_address, self.native_token]
            ).call()
            min_out = int(amounts[1] * (1 - slippage_pct / 100))

            tx = self.router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
                amount_tokens,
                min_out,
                [token_address, self.native_token],
                self.wallet,
                self._deadline(),
            ).build_transaction({
                "from":     self.wallet,
                "gas":      config.GAS_LIMIT_SELL,
                "gasPrice": self._gas_price_normal(),
                "nonce":    self._nonce(),
                "chainId":  self.chain_id,
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

    # ── Sell with RBF gas escalation ─────────────────────────────────────────

    def sell_escalating(self, token_address: str, amount_tokens: int) -> dict:
        """
        Sell with Replace-By-Fee gas escalation for stuck transactions.

        Uses a single nonce across up to 3 attempts with increasing gas+slippage:
          Attempt 1 (t=0):   gas*1.5  + SLIPPAGE_SELL%
          Attempt 2 (t+15s): gas*3.0  + min(SLIPPAGE_SELL*2, 30%)
          Attempt 3 (t+30s): GAS_SELL_MAX_GWEI + 49%

        If a tx reverts (status=0), it's a contract-level reject (honeypot) —
        escalating gas won't help, so we return failure immediately.
        Synchronous — call via asyncio.to_thread from async code.
        """
        try:
            token_address = Web3.to_checksum_address(token_address)

            if amount_tokens <= 0:
                return {"ok": False, "reason": "Нет токенов для продажи"}

            token = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)

            # Ensure approval first (uses its own nonce)
            allowance = token.functions.allowance(self.wallet, self.router_address).call()
            if allowance < amount_tokens:
                approve_tx = token.functions.approve(
                    self.router_address, 2 ** 256 - 1
                ).build_transaction({
                    "from":     self.wallet,
                    "gas":      config.GAS_LIMIT_APPROVE,
                    "gasPrice": self._gas_price_buy(),
                    "nonce":    self._nonce(),
                    "chainId":  self.chain_id,
                })
                signed  = self.account.sign_transaction(approve_tx)
                tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
                self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                log.info(f"Approved {token_address}")

            # Reserve ONE nonce for all RBF attempts
            sell_nonce = self._nonce()
            market_gas = int(self.w3.eth.gas_price)
            max_gas    = max(
                Web3.to_wei(config.GAS_SELL_MAX_GWEI, "gwei"),
                int(market_gas * 5),
            )

            schedule = [
                (int(market_gas * 1.5),  SLIPPAGE_SELL),
                (int(market_gas * 3.0),  min(SLIPPAGE_SELL * 2, 30.0)),
                (max_gas,                49.0),
            ]

            escalation_sec = config.GAS_SELL_ESCALATION_SEC
            last_reason    = ""

            for attempt, (gas_price, slippage) in enumerate(schedule):
                try:
                    amounts = self.router.functions.getAmountsOut(
                        amount_tokens, [token_address, self.native_token]
                    ).call()
                    min_out = int(amounts[1] * (1 - slippage / 100))

                    tx = self.router.functions \
                        .swapExactTokensForETHSupportingFeeOnTransferTokens(
                            amount_tokens,
                            min_out,
                            [token_address, self.native_token],
                            self.wallet,
                            self._deadline(),
                        ).build_transaction({
                            "from":     self.wallet,
                            "gas":      config.GAS_LIMIT_SELL,
                            "gasPrice": gas_price,
                            "nonce":    sell_nonce,
                            "chainId":  self.chain_id,
                        })

                    signed = self.account.sign_transaction(tx)
                    tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
                    log.info(
                        f"sell_escalating #{attempt+1}/3: {token_address}, "
                        f"gas={gas_price/1e9:.1f}gwei, slip={slippage:.0f}%, "
                        f"tx={tx_hash.hex()}"
                    )

                except Exception as send_err:
                    err_msg = str(send_err).lower()
                    # "nonce too low" means a previous RBF attempt was already mined
                    if "nonce too low" in err_msg or "already known" in err_msg:
                        log.info(f"sell_escalating #{attempt+1}: previous tx already mined")
                        # Try to get receipt for latest known tx
                        break
                    log.warning(f"sell_escalating #{attempt+1} send error: {send_err}")
                    last_reason = str(send_err)
                    if attempt < 2:
                        time.sleep(escalation_sec)
                    continue

                # Wait for confirmation with a timeout
                wait_timeout = escalation_sec if attempt < 2 else 60
                try:
                    receipt = self.w3.eth.wait_for_transaction_receipt(
                        tx_hash, timeout=wait_timeout
                    )
                    if receipt.status == 1:
                        log.info(
                            f"sell_escalating OK at attempt #{attempt+1}: "
                            f"{token_address}, tx={tx_hash.hex()}"
                        )
                        return {"ok": True, "tx_hash": tx_hash.hex()}
                    else:
                        # status=0 = contract revert (honeypot), gas escalation won't help
                        return {
                            "ok": False,
                            "reason": f"Продажа отклонена контрактом (slip={slippage:.0f}%)",
                        }
                except Exception:
                    # Timeout — escalate to next level
                    if attempt < 2:
                        log.info(
                            f"sell_escalating #{attempt+1} timeout after "
                            f"{wait_timeout}s — escalating gas…"
                        )
                    else:
                        return {
                            "ok": False,
                            "reason": "Таймаут продажи после 3 попыток эскалации газа",
                        }

            return {"ok": False, "reason": last_reason or "Продажа не выполнена"}

        except Exception as e:
            log.error(f"sell_escalating({token_address}): {e}")
            self.reset_nonce()
            return {"ok": False, "reason": str(e)}
