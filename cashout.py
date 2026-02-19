"""
cashout.py — Auto-sweep USDC.e from bot wallet to cold wallet.

Logic:
  - Check on-chain USDC.e balance of bot wallet
  - If balance >= $400: send (balance - $200) to cold wallet
  - Bot always keeps $200, excess goes to cold storage
  - Run as a Northflank cron job every 10 minutes

Env vars required (same as bot):
  POLY_PRIVATE_KEY  — bot wallet private key
  POLY_ADDRESS      — bot wallet address
"""

import os, sys, time
from web3 import Web3
from eth_account import Account
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

# ── CONFIG ────────────────────────────────────────────────────────────────────
PRIVATE_KEY  = os.environ["POLY_PRIVATE_KEY"]
BOT_ADDRESS  = Web3.to_checksum_address(os.environ["POLY_ADDRESS"])
COLD_WALLET  = Web3.to_checksum_address("0xC95CDE072975245E736B192E2972bD6a9A5b37fc")

USDC_E       = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
THRESHOLD    = 400 * 10**6   # $400 in USDC.e (6 decimals)
KEEP         = 200 * 10**6   # $200 to keep in bot wallet

POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
]

ERC20_ABI = [
    {"inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "transfer", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]

G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"; B = "\033[94m"; RS = "\033[0m"

# ── CONNECT ───────────────────────────────────────────────────────────────────
def connect_rpc():
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            w3.eth.block_number
            print(f"{B}[RPC] Connected: {rpc}{RS}")
            return w3
        except Exception:
            continue
    print(f"{R}[ERROR] No working Polygon RPC{RS}")
    sys.exit(1)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{B}╔══════════════════════════════════════════╗")
    print(f"║        ClawdBot Cashout Service          ║")
    print(f"║  Bot:  {BOT_ADDRESS[:20]}...  ║")
    print(f"║  Cold: {COLD_WALLET[:20]}...  ║")
    print(f"╚══════════════════════════════════════════╝{RS}\n")

    w3     = connect_rpc()
    usdc_e = w3.eth.contract(address=USDC_E, abi=ERC20_ABI)
    acct   = Account.from_key(PRIVATE_KEY)

    # ── Sanity check: verify bot address matches key
    if acct.address.lower() != BOT_ADDRESS.lower():
        print(f"{R}[ERROR] Private key does not match POLY_ADDRESS{RS}")
        sys.exit(1)

    # ── On-chain balance checks
    matic_wei = w3.eth.get_balance(BOT_ADDRESS)
    matic     = w3.from_wei(matic_wei, "ether")
    usdc_raw  = usdc_e.functions.balanceOf(BOT_ADDRESS).call()
    usdc_amt  = usdc_raw / 1e6

    print(f"  MATIC balance : {matic:.4f} MATIC")
    print(f"  USDC.e balance: ${usdc_amt:.2f}")
    print(f"  Threshold     : $400.00  →  keep $200.00, sweep rest")

    # ── MATIC safety check
    if matic_wei < w3.to_wei(0.01, "ether"):
        print(f"{R}[ABORT] Not enough MATIC for gas ({matic:.4f}){RS}")
        sys.exit(1)

    # ── Balance check
    if usdc_raw < THRESHOLD:
        print(f"\n{Y}[SKIP] Balance ${usdc_amt:.2f} < $400 threshold — nothing to sweep{RS}")
        return

    # ── Calculate sweep amount
    sweep_raw = usdc_raw - KEEP
    sweep_amt = sweep_raw / 1e6
    print(f"\n{G}[SWEEP] ${usdc_amt:.2f} >= $400 — sending ${sweep_amt:.2f} to cold wallet{RS}")
    print(f"  From: {BOT_ADDRESS}")
    print(f"  To:   {COLD_WALLET}")
    print(f"  Amount: ${sweep_amt:.2f} USDC.e")

    # ── Build transaction
    nonce    = w3.eth.get_transaction_count(BOT_ADDRESS)
    gas_price = w3.eth.gas_price

    tx = usdc_e.functions.transfer(COLD_WALLET, sweep_raw).build_transaction({
        "from":     BOT_ADDRESS,
        "nonce":    nonce,
        "gas":      100_000,
        "gasPrice": int(gas_price * 1.1),   # +10% for faster inclusion
        "chainId":  137,                     # Polygon mainnet
    })

    # ── Sign and send
    signed  = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"\n{B}[TX] Sent: {tx_hash.hex()}{RS}")
    print(f"  Waiting for confirmation...")

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    if receipt.status == 1:
        remaining = usdc_e.functions.balanceOf(BOT_ADDRESS).call() / 1e6
        print(f"\n{G}[OK] Transfer confirmed!{RS}")
        print(f"  Swept   : ${sweep_amt:.2f} USDC.e → cold wallet")
        print(f"  Bot left: ${remaining:.2f} USDC.e")
        print(f"  Tx hash : {tx_hash.hex()}")
        print(f"  Explorer: https://polygonscan.com/tx/{tx_hash.hex()}")
    else:
        print(f"{R}[ERROR] Transaction failed — check tx: {tx_hash.hex()}{RS}")
        sys.exit(1)


if __name__ == "__main__":
    main()
