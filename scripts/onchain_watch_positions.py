#!/usr/bin/env python3
"""
Lightweight external on-chain-linked watcher for Polymarket positions.

Purpose:
- show open/redeemable counts and values every N seconds
- emit alert on any change
- can run once for ad-hoc checks
"""

import argparse
import os
import subprocess
import time
from datetime import datetime, timezone

import requests


API_URL = "https://data-api.polymarket.com/positions"


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_positions(wallet: str, timeout: int):
    r = requests.get(API_URL, params={"user": wallet, "sizeThreshold": "0.01"}, timeout=timeout)
    r.raise_for_status()
    rows = r.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected response type: {type(rows).__name__}")
    return rows


def summarize(rows):
    open_rows = [
        p
        for p in rows
        if (not p.get("redeemable"))
        and p.get("outcome")
        and float(p.get("currentValue", 0.0) or 0.0) >= 0.01
    ]
    redeem_rows = [
        p
        for p in rows
        if p.get("redeemable")
        and p.get("outcome")
        and float(p.get("currentValue", 0.0) or 0.0) >= 0.01
    ]
    open_val = round(sum(float(p.get("currentValue", 0.0) or 0.0) for p in open_rows), 4)
    redeem_val = round(sum(float(p.get("currentValue", 0.0) or 0.0) for p in redeem_rows), 4)
    return open_rows, redeem_rows, open_val, redeem_val


def print_details(tag: str, rows, limit: int):
    if not rows:
        return
    top = sorted(rows, key=lambda x: float(x.get("currentValue", 0.0) or 0.0), reverse=True)[:limit]
    for p in top:
        cid = str(p.get("conditionId", "") or "")
        side = str(p.get("outcome", "") or "")
        val = float(p.get("currentValue", 0.0) or 0.0)
        title = str(p.get("title", "") or "")
        print(f"  {tag} {cid[:12]} {side:<4} ${val:>8.4f} | {title[:90]}")


def run_auto_redeem(dry_run: bool) -> bool:
    if dry_run:
        print(f"[{now_utc()}] [AUTO-REDEEM] dry-run: skipped")
        return False
    cmd = ["python3", "redeem_wins.py"]
    print(f"[{now_utc()}] [AUTO-REDEEM] running: {' '.join(cmd)}")
    try:
        p = subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(__file__)), text=True, capture_output=True, timeout=180)
        if p.stdout:
            print(p.stdout.rstrip())
        if p.stderr:
            print(p.stderr.rstrip())
        print(f"[{now_utc()}] [AUTO-REDEEM] exit_code={p.returncode}")
        return p.returncode == 0
    except Exception as e:
        print(f"[{now_utc()}] [AUTO-REDEEM] error: {type(e).__name__}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet", default=os.environ.get("POLY_ADDRESS") or os.environ.get("ADDRESS") or "")
    ap.add_argument("--interval", type=float, default=60.0, help="seconds between checks")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--detail-limit", type=int, default=8)
    ap.add_argument("--auto-redeem", action="store_true", help="trigger redeem_wins.py when redeemable > 0")
    ap.add_argument("--redeem-cooldown", type=float, default=90.0, help="minimum seconds between auto-redeem attempts")
    ap.add_argument("--dry-run", action="store_true", help="with --auto-redeem, only log actions")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    wallet = (args.wallet or "").strip()
    if not wallet:
        raise SystemExit("missing --wallet or POLY_ADDRESS/ADDRESS env")

    last_sig = None
    last_redeem_try = 0.0
    print(f"[{now_utc()}] WATCH start wallet={wallet} interval={args.interval:.1f}s")
    while True:
        try:
            rows = fetch_positions(wallet, args.timeout)
            open_rows, redeem_rows, open_val, redeem_val = summarize(rows)
            sig = (len(open_rows), open_val, len(redeem_rows), redeem_val)

            line = (
                f"[{now_utc()}] open={len(open_rows)} (${open_val:.4f}) "
                f"redeemable={len(redeem_rows)} (${redeem_val:.4f}) total_rows={len(rows)}"
            )
            if sig != last_sig:
                print(line + "  <-- changed")
                if redeem_rows:
                    print_details("REDEEM", redeem_rows, args.detail_limit)
                if open_rows:
                    print_details("OPEN  ", open_rows, args.detail_limit)
            else:
                print(line)

            if args.auto_redeem and redeem_rows:
                now_ts = time.time()
                if (now_ts - last_redeem_try) >= max(5.0, args.redeem_cooldown):
                    run_auto_redeem(args.dry_run)
                    last_redeem_try = now_ts
                else:
                    wait_left = max(0.0, args.redeem_cooldown - (now_ts - last_redeem_try))
                    print(f"[{now_utc()}] [AUTO-REDEEM] cooldown {wait_left:.1f}s")
            last_sig = sig
        except Exception as e:
            print(f"[{now_utc()}] error: {type(e).__name__}: {e}")

        if args.once:
            break
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    main()
