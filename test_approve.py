"""
Test on-chain USDC.e approval + Polymarket CLOB allowance sync.
Run locally BEFORE deploying: python3 test_approve.py

Uses ~/.clawdbot.env for credentials. No fake behavior — real txs.
"""

import os
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from py_clob_client.config import get_contract_config

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

PRIVATE_KEY = os.environ["POLY_PRIVATE_KEY"]
ADDRESS     = os.environ["POLY_ADDRESS"]
CLOB_HOST   = "https://clob.polymarket.com"
CHAIN_ID    = POLYGON

POLYGON_RPCS = [
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.drpc.org",
    "https://polygon-mainnet.public.blastapi.io",
]
ERC20_ABI = [
    {"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
     "name":"approve","outputs":[{"name":"","type":"bool"}],
     "stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
     "name":"allowance","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
]

def main():
    cfg     = get_contract_config(CHAIN_ID, neg_risk=False)
    cfg_neg = get_contract_config(CHAIN_ID, neg_risk=True)
    collateral = Web3.to_checksum_address(cfg.collateral)
    spenders   = {
        "CTF Exchange":      Web3.to_checksum_address(cfg.exchange),
        "NegRisk Exchange":  Web3.to_checksum_address(cfg_neg.exchange),
    }

    print(f"\nCollateral (USDC.e): {collateral}")
    for name, addr in spenders.items():
        print(f"  {name}: {addr}")

    # ── Connect RPC ───────────────────────────────────────────────────────────
    w3 = None
    for rpc in POLYGON_RPCS:
        try:
            _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            _w3.eth.block_number
            w3 = _w3
            print(f"\n[RPC] Connected: {rpc}")
            break
        except Exception as e:
            print(f"[RPC] {rpc}: {e}")
    if w3 is None:
        print("[FAIL] No working Polygon RPC")
        return

    acct = Account.from_key(PRIVATE_KEY)
    print(f"[Wallet] {acct.address}")

    pol_balance = w3.eth.get_balance(acct.address)
    print(f"[Wallet] POL balance: {w3.from_wei(pol_balance, 'ether'):.4f} POL")

    # ── Check current on-chain allowances ─────────────────────────────────────
    usdc = w3.eth.contract(address=collateral, abi=ERC20_ABI)
    print("\n[On-chain allowances BEFORE approve]")
    for name, spender in spenders.items():
        current = usdc.functions.allowance(acct.address, spender).call()
        print(f"  {name}: {current / 1e6:.2f} USDC.e")

    # ── Do ERC20 approve on-chain ─────────────────────────────────────────────
    max_int = 2**256 - 1
    for name, spender in spenders.items():
        current = usdc.functions.allowance(acct.address, spender).call()
        if current > 10**24:
            print(f"\n[APPROVE] {name}: already approved, skipping")
            continue
        print(f"\n[APPROVE] Sending approve tx for {name}...")
        try:
            nonce  = w3.eth.get_transaction_count(acct.address)
            tx     = usdc.functions.approve(spender, max_int).build_transaction({
                "from": acct.address, "nonce": nonce,
                "gas": 100_000, "gasPrice": w3.eth.gas_price,
            })
            signed  = acct.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"[APPROVE] tx sent: {tx_hash.hex()}")
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            status  = "OK" if receipt.status == 1 else "FAILED"
            print(f"[APPROVE] {name}: {status} (block {receipt.blockNumber})")
        except Exception as e:
            print(f"[APPROVE] {name}: ERROR — {e}")

    # ── Check allowances after ────────────────────────────────────────────────
    print("\n[On-chain allowances AFTER approve]")
    for name, spender in spenders.items():
        current = usdc.functions.allowance(acct.address, spender).call()
        print(f"  {name}: {current / 1e6:.2f} USDC.e")

    # ── Init CLOB client + sync backend ──────────────────────────────────────
    print("\n[CLOB] Initializing client...")
    clob = ClobClient(
        host=CLOB_HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID,
        signature_type=0, funder=ADDRESS,
    )
    creds = clob.create_or_derive_api_creds()
    clob.set_api_creds(creds)
    print(f"[CLOB] Creds OK: {creds.api_key[:8]}...")

    try:
        resp = clob.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        print(f"[CLOB] Backend allowance synced: {resp or 'OK'}")
    except Exception as e:
        print(f"[CLOB] Backend sync error: {e}")

    try:
        bal   = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        usdc_bal   = float(bal.get("balance", 0)) / 1e6
        allowances = bal.get("allowances", {})
        usdc_allow = max((float(v) for v in allowances.values()), default=0) / 1e6
        print(f"\n[CLOB] USDC balance:   ${usdc_bal:.2f}")
        print(f"[CLOB] USDC allowance: ${usdc_allow:.2f} (max across all exchanges)")
        print(f"[CLOB] Per-exchange:   {allowances}")
        if usdc_allow > 0:
            print("[RESULT] SUCCESS — allowance set, bot should place orders correctly")
        else:
            print("[RESULT] FAIL — allowance still 0 after approve")
    except Exception as e:
        print(f"[CLOB] Balance check error: {e}")


if __name__ == "__main__":
    main()
