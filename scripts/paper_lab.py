#!/usr/bin/env python3
"""
Paper trading strategy lab for ClawdBot.

Runs clawdbot_live.py in DRY_RUN mode with a cloned bankroll (default: $100),
tests multiple strategy profiles, and ranks them by realized paper results.
"""

import argparse
import csv
import json
import os
import signal
import statistics
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Profile:
    name: str
    env: dict


PROFILES = [
    Profile(
        "quality_15m_only",
        {
            "ENABLE_5M": "false",
            "QUALITY_MODE": "true",
            "STRICT_PM_SOURCE": "true",
            "MIN_SCORE_GATE_15M": "9",
            "MIN_PAYOUT_MULT": "2.25",
            "MIN_EV_NET": "0.05",
            "MAX_ENTRY_PRICE": "0.42",
            "ENTRY_HARD_CAP_15M": "0.50",
            "FAST_EXEC_ENABLED": "false",
        },
    ),
    Profile(
        "quality_5m15m",
        {
            "ENABLE_5M": "true",
            "QUALITY_MODE": "true",
            "STRICT_PM_SOURCE": "true",
            "MIN_SCORE_GATE_5M": "10",
            "MIN_SCORE_GATE_15M": "9",
            "MIN_PAYOUT_MULT_5M": "2.0",
            "MIN_EV_NET_5M": "0.04",
            "MAX_ENTRY_PRICE_5M": "0.45",
            "ENTRY_HARD_CAP_5M": "0.48",
            "FAST_EXEC_ENABLED": "false",
        },
    ),
    Profile(
        "balanced_15m",
        {
            "ENABLE_5M": "false",
            "QUALITY_MODE": "false",
            "STRICT_PM_SOURCE": "true",
            "MIN_SCORE_GATE_15M": "8",
            "MIN_PAYOUT_MULT": "2.20",
            "MIN_EV_NET": "0.04",
            "MAX_ENTRY_PRICE": "0.44",
            "ENTRY_HARD_CAP_15M": "0.52",
            "FAST_EXEC_ENABLED": "false",
        },
    ),
]


def _parse_results(csv_path: Path, start_bankroll: float):
    rows = []
    if not csv_path.exists():
        return {"trades": 0, "wins": 0, "wr": 0.0, "pnl": 0.0, "max_dd": 0.0}

    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            result = (r.get("result") or "").upper()
            if result not in ("WIN", "LOSS"):
                continue
            try:
                pnl = float(r.get("pnl", 0) or 0)
            except Exception:
                pnl = 0.0
            rows.append({"result": result, "pnl": pnl})

    trades = len(rows)
    wins = sum(1 for r in rows if r["result"] == "WIN")
    pnl_total = round(sum(r["pnl"] for r in rows), 2)
    wr = (wins / trades * 100.0) if trades else 0.0

    eq = start_bankroll
    peak = start_bankroll
    max_dd = 0.0
    for r in rows:
        eq += r["pnl"]
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)

    return {
        "trades": trades,
        "wins": wins,
        "wr": round(wr, 2),
        "pnl": pnl_total,
        "max_dd": round(max_dd, 2),
    }


def _run_profile(repo_dir: Path, out_root: Path, profile: Profile, minutes: int, bankroll: float):
    run_dir = out_root / profile.name
    run_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "DRY_RUN": "true",
            "BANKROLL": str(bankroll),
            "DATA_DIR": str(run_dir),
            "PYTHONUNBUFFERED": "1",
        }
    )
    env.update(profile.env)

    log_path = run_dir / "stdout.log"
    with log_path.open("w", encoding="utf-8") as logf:
        proc = subprocess.Popen(
            ["python3", "-u", str(repo_dir / "clawdbot_live.py")],
            cwd=str(repo_dir),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
        )
        end_ts = time.time() + minutes * 60
        while time.time() < end_ts and proc.poll() is None:
            time.sleep(1)
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

    metrics = _parse_results(run_dir / "clawdbot_live_trades.csv", bankroll)
    metrics["profile"] = profile.name
    metrics["log"] = str(log_path)
    metrics["data_dir"] = str(run_dir)
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=45, help="runtime per profile")
    ap.add_argument("--bankroll", type=float, default=100.0, help="paper bankroll")
    ap.add_argument("--out-dir", default="paper_lab_runs", help="output root dir")
    ap.add_argument("--profiles", default="all", help="comma list or 'all'")
    args = ap.parse_args()

    repo_dir = Path(__file__).resolve().parents[1]
    out_root = (repo_dir / args.out_dir / time.strftime("%Y%m%d_%H%M%S")).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    if args.profiles == "all":
        selected = PROFILES
    else:
        wanted = {x.strip() for x in args.profiles.split(",") if x.strip()}
        selected = [p for p in PROFILES if p.name in wanted]
    if not selected:
        raise SystemExit("No profiles selected.")

    results = []
    for p in selected:
        print(f"[LAB] running {p.name} for {args.minutes} min (bankroll=${args.bankroll:.2f})")
        results.append(_run_profile(repo_dir, out_root, p, args.minutes, args.bankroll))

    results.sort(key=lambda r: (r["pnl"], r["wr"], -r["max_dd"]), reverse=True)
    report_path = out_root / "summary.json"
    report_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("\n=== PAPER LAB RANKING ===")
    for i, r in enumerate(results, 1):
        print(
            f"{i}. {r['profile']}: trades={r['trades']} wr={r['wr']:.2f}% "
            f"pnl=${r['pnl']:+.2f} max_dd=${r['max_dd']:.2f}"
        )
    print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
