#!/usr/bin/env python3
"""
On-chain-first validation report for ClawdBot.

Inputs:
1) Structured jsonl metrics from clawdbot_live.py (preferred)
2) Legacy paper log with [WIN]/[LOSS] lines (fallback)
"""

import argparse
import json
import math
import os
import re
from collections import defaultdict
from statistics import mean


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _std(values):
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return math.sqrt(sum((x - m) ** 2 for x in values) / (len(values) - 1))


def _sharpe(returns):
    s = _std(returns)
    if s == 0:
        return 0.0
    return (mean(returns) / s) * math.sqrt(len(returns))


def _sortino(returns):
    downs = [r for r in returns if r < 0]
    if not downs:
        return 99.0
    dstd = _std(downs)
    if dstd == 0:
        return 0.0
    return (mean(returns) / dstd) * math.sqrt(len(returns))


def _max_drawdown_from_pnl(pnls):
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        eq += pnl
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _bucket_age(age):
    if age is None:
        return "missing"
    if age <= 15:
        return "0-15s"
    if age <= 45:
        return "15-45s"
    if age <= 90:
        return "45-90s"
    return "90s+"


def _bucket_score(score):
    if score is None:
        return "missing"
    if score < 6:
        return "<6"
    if score < 10:
        return "6-9"
    if score < 14:
        return "10-13"
    return "14+"


def _load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _report_from_jsonl(rows):
    entries = {}
    resolves = []
    for r in rows:
        evt = r.get("event")
        cid = r.get("condition_id", "")
        if evt == "ENTRY":
            entries[cid] = r
        elif evt == "RESOLVE":
            resolves.append(r)

    if not resolves:
        print("No RESOLVE events found in metrics jsonl.")
        return

    joined = []
    for r in resolves:
        e = entries.get(r.get("condition_id", ""), {})
        j = dict(r)
        for key in ("score", "cl_agree", "open_price_source", "chainlink_age_s", "onchain_score_adj"):
            if key not in j and key in e:
                j[key] = e[key]
        joined.append(j)

    wins = sum(1 for r in joined if r.get("result") == "WIN")
    pnls = [_safe_float(r.get("pnl")) for r in joined]
    rets = []
    for r in joined:
        b = _safe_float(r.get("bankroll_after"), 0.0)
        pnl = _safe_float(r.get("pnl"), 0.0)
        if b > 0:
            rets.append(pnl / max(1e-9, b - pnl))
    print("=== On-Chain Strategy Validation (JSONL) ===")
    print(f"Resolved trades: {len(joined)}")
    print(f"Win rate:        {wins/len(joined)*100:.2f}%")
    print(f"Total PnL:       ${sum(pnls):+.2f}")
    print(f"Avg PnL/trade:   ${mean(pnls):+.2f}")
    print(f"Sharpe:          {_sharpe(rets):.3f}")
    print(f"Sortino:         {_sortino(rets):.3f}")
    print(f"Max drawdown:    ${_max_drawdown_from_pnl(pnls):.2f}")

    by_src = defaultdict(lambda: [0, 0, 0.0])
    by_age = defaultdict(lambda: [0, 0, 0.0])
    by_score = defaultdict(lambda: [0, 0, 0.0])
    for r in joined:
        res = 1 if r.get("result") == "WIN" else 0
        pnl = _safe_float(r.get("pnl"))
        src = r.get("open_price_source", "missing")
        age_b = _bucket_age(r.get("chainlink_age_s"))
        sc_b = _bucket_score(r.get("score"))
        by_src[src][0] += 1
        by_src[src][1] += res
        by_src[src][2] += pnl
        by_age[age_b][0] += 1
        by_age[age_b][1] += res
        by_age[age_b][2] += pnl
        by_score[sc_b][0] += 1
        by_score[sc_b][1] += res
        by_score[sc_b][2] += pnl

    print("\nBy open-price source:")
    for k, v in sorted(by_src.items()):
        print(f"  {k:>10}  n={v[0]:4d}  wr={v[1]/v[0]*100:6.2f}%  pnl=${v[2]:+8.2f}")
    print("\nBy Chainlink age bucket:")
    for k, v in sorted(by_age.items()):
        print(f"  {k:>10}  n={v[0]:4d}  wr={v[1]/v[0]*100:6.2f}%  pnl=${v[2]:+8.2f}")
    print("\nBy score bucket:")
    for k, v in sorted(by_score.items()):
        print(f"  {k:>10}  n={v[0]:4d}  wr={v[1]/v[0]*100:6.2f}%  pnl=${v[2]:+8.2f}")


def _report_from_paper_log(path):
    ansi = re.compile(r"\x1B\[[0-9;]*m")
    patt = re.compile(r"\[(WIN|LOSS)\].*\|\s*\$([+-]?\d+(?:\.\d+)?)\s*\|\s*Bank")
    pnls = []
    wins = 0
    total = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = ansi.sub("", line)
            m = patt.search(line)
            if not m:
                continue
            total += 1
            if m.group(1) == "WIN":
                wins += 1
            pnls.append(_safe_float(m.group(2)))
    if not total:
        print("No [WIN]/[LOSS] lines found in paper log.")
        return
    rets = [p / 1000.0 for p in pnls]
    print("=== Strategy Validation (Paper Log Fallback) ===")
    print(f"Resolved trades: {total}")
    print(f"Win rate:        {wins/total*100:.2f}%")
    print(f"Total PnL:       ${sum(pnls):+.2f}")
    print(f"Avg PnL/trade:   ${mean(pnls):+.2f}")
    print(f"Sharpe:          {_sharpe(rets):.3f}")
    print(f"Sortino:         {_sortino(rets):.3f}")
    print(f"Max drawdown:    ${_max_drawdown_from_pnl(pnls):.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-jsonl", default="~/clawdbot_onchain_metrics.jsonl")
    ap.add_argument("--paper-log", default="")
    args = ap.parse_args()

    metrics_path = os.path.expanduser(args.metrics_jsonl)
    paper_log = os.path.expanduser(args.paper_log) if args.paper_log else ""

    try:
        rows = _load_jsonl(metrics_path)
    except Exception:
        rows = []

    if rows:
        _report_from_jsonl(rows)
        return

    if paper_log:
        _report_from_paper_log(paper_log)
        return

    print("No metrics jsonl found and no --paper-log provided.")


if __name__ == "__main__":
    main()
