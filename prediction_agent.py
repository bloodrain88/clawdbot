import math
import time
from collections import deque


class PredictionAgent:
    """Dedicated prediction agent with empirical + market-state ensemble."""

    def __init__(self, min_samples=24, max_hist=4000):
        self.min_samples = int(min_samples)
        self._pred_hist = deque(maxlen=max_hist)

    @staticmethod
    def _wilson_lb(wins: int, n: int, z: float = 1.0) -> float:
        if n <= 0:
            return 0.5
        phat = wins / n
        den = 1.0 + z * z / n
        center = phat + z * z / (2 * n)
        margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)
        return max(0.0, min(1.0, (center - margin) / den))

    def _sample_prior(self, resolved_samples, asset: str, duration: int, side: str):
        wins = 0
        n = 0
        pnl = 0.0
        for s in reversed(resolved_samples):
            if str(s.get("asset", "")) != asset:
                continue
            if int(s.get("duration", 0) or 0) != duration:
                continue
            if str(s.get("side", "")) != side:
                continue
            n += 1
            if bool(s.get("won", False)):
                wins += 1
            pnl += float(s.get("pnl", 0.0) or 0.0)
            if n >= 200:
                break
        if n <= 0:
            return 0.5, 0.5, 0.0, 0
        wr = wins / n
        wr_lb = self._wilson_lb(wins, n, z=1.0)
        exp = pnl / max(1, n)
        return wr, wr_lb, exp, n

    def _bucket_prior(self, bucket_rows: dict, key: str):
        r = dict((bucket_rows or {}).get(key) or {})
        if not r:
            return 0.5, 0.0, 0
        outcomes = int(r.get("outcomes", r.get("wins", 0) + r.get("losses", 0)) or 0)
        wins = int(r.get("wins", 0) or 0)
        pnl = float(r.get("pnl", 0.0) or 0.0)
        if outcomes <= 0:
            return 0.5, 0.0, 0
        wr_lb = self._wilson_lb(wins, outcomes, z=1.0)
        exp = pnl / max(1, outcomes)
        return wr_lb, exp, outcomes

    def predict(self, context: dict, resolved_samples, bucket_rows: dict):
        asset = str(context.get("asset", "") or "")
        duration = int(context.get("duration", 0) or 0)
        side = str(context.get("side", "") or "")
        current = float(context.get("current", 0.0) or 0.0)
        open_price = float(context.get("open_price", 0.0) or 0.0)
        mins_left = float(context.get("mins_left", 0.0) or 0.0)
        base_prob = float(context.get("base_prob", 0.5) or 0.5)
        base_edge = float(context.get("base_edge", 0.0) or 0.0)
        score = int(context.get("score", 0) or 0)
        bucket_key = str(context.get("bucket_key", "") or "")

        wr, wr_lb, exp, n_side = self._sample_prior(resolved_samples, asset, duration, side)
        b_wr_lb, b_exp, b_n = self._bucket_prior(bucket_rows, bucket_key)

        move_pct = 0.0
        if open_price > 0 and current > 0:
            move_pct = (current - open_price) / open_price
        dir_ok = (move_pct >= 0 and side == "Up") or (move_pct < 0 and side == "Down")
        trend_conf = min(1.0, abs(move_pct) / 0.0025)  # 0.25% move => full trend confidence
        trend_prob = 0.5 + (0.08 * trend_conf if dir_ok else -0.08 * trend_conf)

        time_weight = 0.65 if duration >= 15 else 0.55
        if mins_left <= 2.5:
            time_weight += 0.10
        elif mins_left >= (10.0 if duration >= 15 else 3.0):
            time_weight -= 0.05
        time_weight = max(0.35, min(0.80, time_weight))

        # Empirical prior from realized outcomes.
        emp_prob = 0.5
        if n_side >= self.min_samples:
            emp_prob = 0.5 + (wr_lb - 0.5) * 0.9
            if exp > 0.20:
                emp_prob += 0.015
            elif exp < -0.20:
                emp_prob -= 0.015
        # Bucket prior from realized expectancy.
        buck_prob = 0.5
        if b_n >= self.min_samples // 2:
            buck_prob = 0.5 + (b_wr_lb - 0.5) * 0.7
            if b_exp > 0.20:
                buck_prob += 0.010
            elif b_exp < -0.20:
                buck_prob -= 0.010

        # Ensemble.
        prob = (
            base_prob * time_weight
            + trend_prob * (0.20 if duration >= 15 else 0.25)
            + emp_prob * 0.10
            + buck_prob * 0.05
        )
        prob = max(0.05, min(0.95, prob))
        prob_adj = prob - base_prob

        edge_adj = prob_adj * 0.65
        score_adj = 0
        if prob_adj >= 0.03:
            score_adj += 1
        elif prob_adj <= -0.03:
            score_adj -= 1
        if n_side >= 40 and wr_lb >= 0.56 and exp > 0:
            score_adj += 1
        if n_side >= 40 and wr_lb <= 0.46 and exp < 0:
            score_adj -= 1
        if score >= 14 and prob_adj > 0:
            score_adj += 1

        out = {
            "prob": prob,
            "prob_adj": prob_adj,
            "edge_adj": edge_adj,
            "score_adj": int(max(-2, min(2, score_adj))),
            "samples_side": n_side,
            "samples_bucket": b_n,
            "wr_lb_side": wr_lb,
            "exp_side": exp,
        }
        self._pred_hist.append({"ts": time.time(), "ctx": context, "out": out})
        return out
