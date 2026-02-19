"""
One-shot: redeem ALL currently redeemable winning positions from bot wallet.
Fetches positions live from Polymarket API — no hardcoded IDs.
"""

import os, requests
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from py_clob_client.config import get_contract_config
from py_clob_client.constants import POLYGON

load_dotenv(os.path.expanduser("~/.clawdbot.env"))
PRIVATE_KEY = os.environ["POLY_PRIVATE_KEY"]
ADDRESS     = os.environ["POLY_ADDRESS"]
CHAIN_ID    = POLYGON

POLYGON_RPCS = [
    "https://polygon-rpc.com",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon.drpc.org",
]

CTF_ABI_FULL = [
    {"inputs":[{"name":"collateralToken","type":"address"},
               {"name":"parentCollectionId","type":"bytes32"},
               {"name":"conditionId","type":"bytes32"},
               {"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"conditionId","type":"bytes32"}],
     "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"index","type":"uint256"}],
     "name":"payoutNumerators","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
]

def get_w3():
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            w3.eth.block_number
            print(f"[RPC] Connected: {rpc}")
            return w3
        except Exception as e:
            print(f"[RPC] {rpc}: {e}")
    return None

def redeem(w3, ctf, collateral, acct, cid, side, val):
    cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))
    index_set = 1 if side == "Up" else 2
    nonce = w3.eth.get_transaction_count(acct.address)
    # EIP-1559 gas pricing
    latest   = w3.eth.get_block("latest")
    base_fee = latest["baseFeePerGas"]
    pri_fee  = w3.to_wei(40, "gwei")
    max_fee  = base_fee * 2 + pri_fee
    tx = ctf.functions.redeemPositions(
        collateral, b'\x00' * 32, cid_bytes, [index_set]
    ).build_transaction({
        "from": acct.address, "nonce": nonce,
        "gas": 200_000,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": pri_fee,
        "chainId": 137,
    })
    signed  = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  tx: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    ok = receipt.status == 1
    print(f"  {'OK' if ok else 'FAILED'} — block {receipt.blockNumber}")
    return ok

def main():
    print(f"Wallet: {ADDRESS}\n")

    w3 = get_w3()
    if not w3:
        print("No RPC available"); return

    cfg      = get_contract_config(CHAIN_ID, neg_risk=False)
    ctf_addr = Web3.to_checksum_address(cfg.conditional_tokens)
    collat   = Web3.to_checksum_address(cfg.collateral)
    acct     = Account.from_key(PRIVATE_KEY)
    ctf      = w3.eth.contract(address=ctf_addr, abi=CTF_ABI_FULL)

    print(f"CTF contract: {ctf_addr}")
    print(f"Collateral:   {collat}\n")

    # Fetch all positions from Polymarket API
    r = requests.get(
        "https://data-api.polymarket.com/positions",
        params={"user": ADDRESS, "sizeThreshold": "0.01"},
        timeout=10
    )
    positions = r.json()
    print(f"Found {len(positions)} position(s) total\n")

    redeemable = [p for p in positions if p.get("redeemable") and float(p.get("currentValue", 0)) >= 0.01]
    if not redeemable:
        print("No redeemable positions found."); return

    print(f"{len(redeemable)} redeemable position(s):\n")
    success = 0
    for pos in redeemable:
        cid     = pos["conditionId"]
        side    = pos.get("outcome", "")
        val     = float(pos.get("currentValue", 0))
        title   = pos.get("title", "")[:50]
        print(f"[REDEEM] {title} | {side} | ~${val:.2f}")

        # Verify on-chain that it's resolved and we won
        try:
            cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))
            denom = ctf.functions.payoutDenominator(cid_bytes).call()
            if denom == 0:
                print(f"  Not yet resolved on-chain — skipping"); continue
            n0 = ctf.functions.payoutNumerators(cid_bytes, 0).call()
            winner = "Up" if n0 > 0 else "Down"
            if winner != side:
                print(f"  On-chain winner={winner}, we bet {side} — LOSS, nothing to redeem"); continue
        except Exception as e:
            print(f"  On-chain check failed: {e}"); continue

        try:
            ok = redeem(w3, ctf, collat, acct, cid, side, val)
            if ok: success += 1
        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nDone — {success}/{len(redeemable)} redeemed successfully.")
    print(f"Check balance: https://polygonscan.com/address/{ADDRESS}")

if __name__ == "__main__":
    main()
