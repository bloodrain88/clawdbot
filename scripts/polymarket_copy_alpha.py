#!/usr/bin/env python3
"""
Build a copy-alpha snapshot from on-chain Polymarket activity.

Pipeline:
1) Get your recent trades (same markets you play).
2) Find other wallets active on those same conditionIds.
3) Score wallets by settled win quality + ROI + same-side affinity.
4) Build per-market Up/Down side-flow from the top leader wallets.
5) Write JSON file for clawdbot consumption.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

API = "https://data-api.polymarket.com"


@dataclass
class LeaderScore:
    wallet: str
    score: float
    settled: int
    wins: int
    wr: float
    roi: float
    same_side_ratio: float
    same_market_trades: int
    same_market_usdc: float


def _norm_wallet(w: str) -> str:
    return (w or "").lower()


async def _get_json(session: aiohttp.ClientSession, path: str, params: dict[str, Any], timeout: float = 12.0):
    async with session.get(
        f"{API}{path}",
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as r:
        r.raise_for_status()
        return await r.json()


async def _fetch_own_trades(session: aiohttp.ClientSession, wallet: str, limit: int):
    rows = await _get_json(session, "/trades", {"user": wallet, "limit": str(limit)})
    if not isinstance(rows, list):
        return []
    return rows


async def _fetch_market_trades(
    session: aiohttp.ClientSession,
    cid: str,
    per_market_limit: int,
    sem: asyncio.Semaphore,
):
    async with sem:
        try:
            rows = await _get_json(session, "/trades", {"market": cid, "limit": str(per_market_limit)})
            return cid, rows if isinstance(rows, list) else []
        except Exception:
            return cid, []


async def _fetch_wallet_activity(
    session: aiohttp.ClientSession,
    wallet: str,
    limit: int,
    sem: asyncio.Semaphore,
):
    async with sem:
        try:
            rows = await _get_json(session, "/activity", {"user": wallet, "limit": str(limit)})
            return wallet, rows if isinstance(rows, list) else []
        except Exception:
            return wallet, []


def _score_wallet_from_activity(
    rows: list[dict[str, Any]],
    now_ts: int,
    settlement_lag_sec: int,
    min_bet_usdc: float,
):
    by_cid: dict[str, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "redeem": 0.0, "last_trade_ts": 0.0})
    for e in rows:
        cid = e.get("conditionId") or ""
        if not cid:
            continue
        typ = (e.get("type") or "").upper()
        usdc = float(e.get("usdcSize") or 0.0)
        ts = float(e.get("timestamp") or 0.0)
        if typ in ("TRADE", "BUY", "PURCHASE") and (e.get("side") or "BUY").upper() == "BUY":
            by_cid[cid]["buy"] += max(0.0, usdc)
            by_cid[cid]["last_trade_ts"] = max(by_cid[cid]["last_trade_ts"], ts)
        elif typ == "REDEEM":
            by_cid[cid]["redeem"] += max(0.0, usdc)

    settled = wins = 0
    buy_sum = redeem_sum = 0.0
    for d in by_cid.values():
        buy = d["buy"]
        if buy < min_bet_usdc:
            continue
        if now_ts - d["last_trade_ts"] < settlement_lag_sec:
            continue
        settled += 1
        buy_sum += buy
        redeem_sum += d["redeem"]
        if d["redeem"] > 0:
            wins += 1
    wr = (wins / settled) if settled > 0 else 0.0
    roi = ((redeem_sum - buy_sum) / buy_sum) if buy_sum > 0 else -1.0
    return settled, wins, wr, roi


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


async def build_copy_alpha(
    wallet: str,
    lookback_trades: int,
    activity_limit: int,
    per_market_limit: int,
    candidate_limit: int,
    top_leaders: int,
    settlement_lag_sec: int,
    min_bet_usdc: float,
    min_roi: float,
):
    sem = asyncio.Semaphore(8)
    now_ts = int(time.time())
    async with aiohttp.ClientSession() as session:
        own_trades = await _fetch_own_trades(session, wallet, lookback_trades)
        own_wallet = _norm_wallet(wallet)
        own_conditions: dict[str, str] = {}
        for t in own_trades:
            cid = t.get("conditionId") or ""
            out = t.get("outcome") or ""
            side = (t.get("side") or "").upper()
            if cid and out and side == "BUY":
                own_conditions[cid] = out
        cids = list(own_conditions.keys())
        if not cids:
            return {"leaders": [], "market_side_flow": {}, "meta": {"reason": "no own trades"}}

        market_tasks = [
            _fetch_market_trades(session, cid, per_market_limit, sem)
            for cid in cids
        ]
        market_rows = await asyncio.gather(*market_tasks)

        peer_agg: dict[str, dict[str, float]] = defaultdict(
            lambda: {"same_market_trades": 0.0, "same_market_usdc": 0.0, "same_side": 0.0}
        )
        market_trades_by_cid: dict[str, list[dict[str, Any]]] = {}
        for cid, rows in market_rows:
            market_trades_by_cid[cid] = rows
            my_side = own_conditions.get(cid, "")
            for tr in rows:
                w = _norm_wallet(tr.get("proxyWallet") or "")
                if not w or w == own_wallet:
                    continue
                side = (tr.get("side") or "").upper()
                if side != "BUY":
                    continue
                usdc = float(tr.get("price") or 0.0) * float(tr.get("size") or 0.0)
                out = tr.get("outcome") or ""
                peer_agg[w]["same_market_trades"] += 1.0
                peer_agg[w]["same_market_usdc"] += max(0.0, usdc)
                if out and out == my_side:
                    peer_agg[w]["same_side"] += 1.0

        candidates = sorted(
            peer_agg.items(),
            key=lambda kv: (kv[1]["same_market_trades"], kv[1]["same_market_usdc"]),
            reverse=True,
        )[:candidate_limit]

        act_tasks = [
            _fetch_wallet_activity(session, w, activity_limit, sem)
            for w, _ in candidates
        ]
        activity_rows = await asyncio.gather(*act_tasks)
        activity_by_wallet = {w: rows for w, rows in activity_rows}

        leaders: list[LeaderScore] = []
        for w, agg in candidates:
            settled, wins, wr, roi = _score_wallet_from_activity(
                activity_by_wallet.get(w, []),
                now_ts=now_ts,
                settlement_lag_sec=settlement_lag_sec,
                min_bet_usdc=min_bet_usdc,
            )
            if settled < 8:
                continue
            if roi < min_roi:
                continue
            same_trades = int(agg["same_market_trades"])
            same_side_ratio = (agg["same_side"] / agg["same_market_trades"]) if agg["same_market_trades"] > 0 else 0.5
            # Composite growth score: outcome quality + pnl efficiency + market affinity.
            wr_n = _clamp((wr - 0.45) / 0.20, 0.0, 1.0)
            roi_n = _clamp((roi + 0.20) / 0.60, 0.0, 1.0)
            aff_n = _clamp((same_side_ratio - 0.45) / 0.25, 0.0, 1.0)
            sample_n = _clamp(math.log1p(settled) / math.log1p(60), 0.0, 1.0)
            score = wr_n * 0.45 + roi_n * 0.25 + aff_n * 0.20 + sample_n * 0.10
            leaders.append(
                LeaderScore(
                    wallet=w,
                    score=score,
                    settled=settled,
                    wins=wins,
                    wr=wr,
                    roi=roi,
                    same_side_ratio=same_side_ratio,
                    same_market_trades=same_trades,
                    same_market_usdc=float(agg["same_market_usdc"]),
                )
            )

        leaders = sorted(leaders, key=lambda x: x.score, reverse=True)[:top_leaders]
        leader_set = {x.wallet for x in leaders}
        leader_weight = {x.wallet: x.score for x in leaders}

        market_side_flow: dict[str, dict[str, float]] = {}
        for cid, rows in market_trades_by_cid.items():
            up = down = 0.0
            for tr in rows:
                w = _norm_wallet(tr.get("proxyWallet") or "")
                if w not in leader_set:
                    continue
                if (tr.get("side") or "").upper() != "BUY":
                    continue
                out = tr.get("outcome") or ""
                wt = leader_weight.get(w, 0.0)
                if out == "Up":
                    up += wt
                elif out == "Down":
                    down += wt
            s = up + down
            if s <= 0:
                continue
            market_side_flow[cid] = {"Up": round(up / s, 4), "Down": round(down / s, 4), "n": int(round(s * 10))}

        return {
            "generated_at": now_ts,
            "wallet": wallet.lower(),
            "leaders": [
                {
                    "wallet": x.wallet,
                    "score": round(x.score, 4),
                    "settled": x.settled,
                    "wins": x.wins,
                    "wr": round(x.wr, 4),
                    "roi": round(x.roi, 4),
                    "same_side_ratio": round(x.same_side_ratio, 4),
                    "same_market_trades": x.same_market_trades,
                    "same_market_usdc": round(x.same_market_usdc, 2),
                }
                for x in leaders
            ],
            "market_side_flow": market_side_flow,
            "meta": {
                "own_markets": len(cids),
                "candidates": len(candidates),
                "leaders": len(leaders),
                "settlement_lag_sec": settlement_lag_sec,
            },
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet", required=True, help="wallet address to analyze around")
    ap.add_argument("--out", default="~/clawdbot_copyflow.json", help="output JSON path")
    ap.add_argument("--lookback-trades", type=int, default=280)
    ap.add_argument("--activity-limit", type=int, default=450)
    ap.add_argument("--per-market-limit", type=int, default=120)
    ap.add_argument("--candidate-limit", type=int, default=80)
    ap.add_argument("--top-leaders", type=int, default=18)
    ap.add_argument("--settlement-lag-sec", type=int, default=3600)
    ap.add_argument("--min-bet-usdc", type=float, default=2.0)
    ap.add_argument("--min-roi", type=float, default=-0.05, help="drop leaders with roi below this")
    args = ap.parse_args()

    payload = asyncio.run(
        build_copy_alpha(
            wallet=args.wallet,
            lookback_trades=args.lookback_trades,
            activity_limit=args.activity_limit,
            per_market_limit=args.per_market_limit,
            candidate_limit=args.candidate_limit,
            top_leaders=args.top_leaders,
            settlement_lag_sec=args.settlement_lag_sec,
            min_bet_usdc=args.min_bet_usdc,
            min_roi=args.min_roi,
        )
    )
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    leaders = payload.get("leaders", [])
    print(f"leaders={len(leaders)} markets_with_flow={len(payload.get('market_side_flow', {}))} out={out}")
    for i, row in enumerate(leaders[:10], start=1):
        print(
            f"{i:>2}. {row['wallet']} score={row['score']:.3f} "
            f"wr={row['wr']*100:.1f}% roi={row['roi']*100:.1f}% "
            f"same_side={row['same_side_ratio']*100:.1f}% settled={row['settled']}"
        )


if __name__ == "__main__":
    main()
