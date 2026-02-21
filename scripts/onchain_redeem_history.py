#!/usr/bin/env python3
"""
Sweep historical Polymarket activity -> derive CID/side universe -> redeem on-chain.

Why this is correct:
1) CID discovery comes from your own trade history (activity API).
2) Claimability is decided only on-chain:
   - payoutDenominator / payoutNumerators
   - balanceOf(wallet, tokenId(cid, side))
3) Redeem is submitted on-chain per claimable CID/side.
"""

import argparse
import json
import os
import time
from collections import defaultdict

import requests
from eth_account import Account
from web3 import Web3


CTF_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDCE_ADDR = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ACTIVITY_URL = "https://data-api.polymarket.com/activity"

CTF_ABI = [
    {
        "inputs": [{"name": "owner", "type": "address"}, {"name": "id", "type": "uint256"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}, {"name": "index", "type": "uint256"}],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


def _load_env(path: str) -> None:
    if not path or not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def _fetch_activity(address: str, pages: int, page_size: int):
    rows = []
    seen_ids = set()
    offset = 0
    for _ in range(max(1, pages)):
        try:
            r = requests.get(
                ACTIVITY_URL,
                params={"user": address, "limit": str(page_size), "offset": str(offset)},
                timeout=15,
            )
            data = r.json()
        except Exception:
            break
        if not isinstance(data, list) or not data:
            break
        new_count = 0
        for ev in data:
            evt_id = ev.get("id") or ev.get("transactionHash") or json.dumps(ev, sort_keys=True)[:120]
            if evt_id in seen_ids:
                continue
            seen_ids.add(evt_id)
            rows.append(ev)
            new_count += 1
        if new_count == 0:
            break
        if len(data) < page_size:
            break
        offset += page_size
        time.sleep(0.15)
    return rows


def _extract_cid_sides(activity_rows):
    by_cid = defaultdict(set)
    for ev in activity_rows:
        typ = str(ev.get("type") or "").upper()
        if typ not in ("BUY", "TRADE", "PURCHASE", "SELL"):
            continue
        cid = ev.get("conditionId") or (ev.get("market") or {}).get("conditionId") or ""
        # Activity API often reports side=BUY/SELL and outcome=Up/Down.
        # Prefer outcome for market direction extraction.
        outcome = str(ev.get("outcome") or "").strip().title()
        side_raw = str(ev.get("side") or "").strip().title()
        side = outcome if outcome in ("Up", "Down") else side_raw
        if cid and side in ("Up", "Down"):
            by_cid[cid].add(side)
    return by_cid


def _extract_from_pending(path: str):
    by_cid = defaultdict(set)
    if not path or not os.path.exists(path):
        return by_cid
    try:
        raw = json.load(open(path, "r", encoding="utf-8"))
    except Exception:
        return by_cid
    if not isinstance(raw, dict):
        return by_cid
    for cid, val in raw.items():
        side = ""
        if isinstance(val, list) and len(val) > 1 and isinstance(val[1], dict):
            side = str(val[1].get("side", "")).strip().title()
        if cid and side in ("Up", "Down"):
            by_cid[cid].add(side)
    return by_cid


def _extract_from_metrics(path: str):
    by_cid = defaultdict(set)
    if not path or not os.path.exists(path):
        return by_cid
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("event") not in ("ENTRY", "QUEUE_REDEEM"):
                    continue
                cid = r.get("condition_id", "")
                side = str(r.get("side", "")).strip().title()
                if cid and side in ("Up", "Down"):
                    by_cid[cid].add(side)
    except Exception:
        return by_cid
    return by_cid


def _cid_bytes(cid: str):
    h = cid.lower().replace("0x", "")
    if len(h) != 64:
        return None
    try:
        return bytes.fromhex(h)
    except Exception:
        return None


def _token_id_for_side(w3: Web3, cid_b: bytes, side: str):
    idx = 1 if side == "Up" else 2
    # Binary markets with parentCollectionId=0x0:
    # collectionId = keccak256(conditionId, indexSet)
    # positionId   = keccak256(collateralToken, collectionId)
    collection_id = w3.solidity_keccak(["bytes32", "uint256"], [cid_b, idx])
    position_id = w3.solidity_keccak(["address", "bytes32"], [Web3.to_checksum_address(USDCE_ADDR), collection_id])
    return int.from_bytes(position_id, "big"), idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=os.path.expanduser("~/.clawdbot.env"))
    ap.add_argument("--rpc", default="https://polygon-bor-rpc.publicnode.com")
    ap.add_argument("--pages", type=int, default=30, help="activity pages to scan")
    ap.add_argument("--page-size", type=int, default=200, help="rows per page")
    ap.add_argument("--dry-run", action="store_true", help="do not submit tx")
    ap.add_argument("--report", default="onchain_redeem_history_report.json")
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    args = ap.parse_args()

    _load_env(args.env_file)
    address = os.environ.get("POLY_ADDRESS", "").strip()
    private_key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    if not address or not private_key:
        raise SystemExit("Missing POLY_ADDRESS/POLY_PRIVATE_KEY in env")

    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 20}))
    if not w3.is_connected():
        raise SystemExit(f"RPC not connected: {args.rpc}")

    acct = Account.from_key(private_key)
    if acct.address.lower() != Web3.to_checksum_address(address).lower():
        raise SystemExit("POLY_PRIVATE_KEY does not match POLY_ADDRESS")

    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDR), abi=CTF_ABI)
    address_cs = Web3.to_checksum_address(address)

    activity = _fetch_activity(address, args.pages, args.page_size)
    cid_sides = defaultdict(set)
    for source in (
        _extract_cid_sides(activity),
        _extract_from_pending(os.path.join(args.data_dir, "clawdbot_pending.json")),
        _extract_from_metrics(os.path.join(args.data_dir, "clawdbot_onchain_metrics.jsonl")),
    ):
        for cid, sides in source.items():
            cid_sides[cid].update(sides)

    report = {
        "ts": int(time.time()),
        "address": address_cs,
        "rpc": args.rpc,
        "activity_rows": len(activity),
        "cids_found": len(cid_sides),
        "sources": {
            "activity": len(_extract_cid_sides(activity)),
            "pending_file": len(_extract_from_pending(os.path.join(args.data_dir, "clawdbot_pending.json"))),
            "metrics_file": len(_extract_from_metrics(os.path.join(args.data_dir, "clawdbot_onchain_metrics.jsonl"))),
        },
        "dry_run": bool(args.dry_run),
        "claimable": [],
        "redeemed": [],
        "errors": [],
    }

    nonce = w3.eth.get_transaction_count(address_cs, "pending")
    for cid, sides in cid_sides.items():
        cid_b = _cid_bytes(cid)
        if cid_b is None:
            continue
        try:
            denom = int(ctf.functions.payoutDenominator(cid_b).call())
        except Exception as e:
            report["errors"].append({"cid": cid, "error": f"denom:{str(e)[:200]}"})
            continue
        if denom <= 0:
            continue
        try:
            n0 = int(ctf.functions.payoutNumerators(cid_b, 0).call())
            n1 = int(ctf.functions.payoutNumerators(cid_b, 1).call())
        except Exception as e:
            report["errors"].append({"cid": cid, "error": f"numerators:{str(e)[:200]}"})
            continue
        winner = "Up" if (n0 > 0 and n1 == 0) else ("Down" if (n1 > 0 and n0 == 0) else "")
        if winner not in ("Up", "Down"):
            continue

        for side in sorted(sides):
            if side != winner:
                continue
            try:
                token_id, idx = _token_id_for_side(w3, cid_b, side)
                bal_raw = int(ctf.functions.balanceOf(address_cs, token_id).call())
                if bal_raw <= 0:
                    continue
                claim = {
                    "cid": cid,
                    "side": side,
                    "winner": winner,
                    "token_id": str(token_id),
                    "qty": bal_raw / 1e6,
                }
                report["claimable"].append(claim)
                if args.dry_run:
                    continue
                latest = w3.eth.get_block("latest")
                base_fee = int(latest.get("baseFeePerGas", w3.to_wei(35, "gwei")))
                pri_fee = int(w3.to_wei(40, "gwei"))
                tx = ctf.functions.redeemPositions(
                    Web3.to_checksum_address(USDCE_ADDR),
                    b"\x00" * 32,
                    cid_b,
                    [idx],
                ).build_transaction(
                    {
                        "from": address_cs,
                        "nonce": nonce,
                        "gas": 240_000,
                        "maxFeePerGas": base_fee * 2 + pri_fee,
                        "maxPriorityFeePerGas": pri_fee,
                        "chainId": 137,
                    }
                )
                signed = acct.sign_transaction(tx)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                rc = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                report["redeemed"].append(
                    {
                        "cid": cid,
                        "side": side,
                        "tx_hash": tx_hash.hex(),
                        "status": int(rc.status),
                        "block": int(rc.blockNumber),
                    }
                )
                nonce += 1
            except Exception as e:
                report["errors"].append({"cid": cid, "side": side, "error": str(e)[:250]})

    report["claimable_count"] = len(report["claimable"])
    report["redeemed_count"] = sum(1 for r in report["redeemed"] if r.get("status") == 1)

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(
        json.dumps(
            {
                "cids_found": report["cids_found"],
                "claimable_count": report["claimable_count"],
                "redeemed_count": report["redeemed_count"],
                "errors": len(report["errors"]),
                "report": os.path.abspath(args.report),
            }
        )
    )


if __name__ == "__main__":
    main()
