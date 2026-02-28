import math
import time
from collections import deque


class PredictionAgent:
    """Dedicated prediction agent with online learning + shadow model selection."""

    def __init__(self, min_samples=24, max_hist=4000):
        self.min_samples = int(min_samples)
        self._pred_hist = deque(maxlen=max_hist)
        self._variant_stats = {
            "balanced": {"n": 0, "wins": 0, "pnl": 0.0, "brier": 0.0, "cal_err_ema": 0.25},
            "momentum": {"n": 0, "wins": 0, "pnl": 0.0, "brier": 0.0, "cal_err_ema": 0.25},
            "empirical": {"n": 0, "wins": 0, "pnl": 0.0, "brier": 0.0, "cal_err_ema": 0.25},
        }
        self._variant_reliability = {
            "balanced": 1.0,
            "momentum": 1.0,
            "empirical": 1.0,
        }
        self._active_variant = "balanced"
        self._last_switch_ts = 0.0

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

    @staticmethod
    def _clamp_prob(p: float) -> float:
        return max(0.05, min(0.95, float(p)))

    def _variant_probs(self, base_prob, trend_prob, emp_prob, buck_prob, duration):
        # Three parallel predictors with different inductive bias.
        p_bal = (
            base_prob * (0.62 if duration >= 15 else 0.56)
            + trend_prob * (0.23 if duration >= 15 else 0.28)
            + emp_prob * 0.10
            + buck_prob * 0.05
        )
        p_mom = (
            base_prob * (0.50 if duration >= 15 else 0.45)
            + trend_prob * (0.35 if duration >= 15 else 0.42)
            + emp_prob * 0.10
            + buck_prob * 0.05
        )
        p_emp = (
            base_prob * (0.52 if duration >= 15 else 0.48)
            + trend_prob * (0.16 if duration >= 15 else 0.20)
            + emp_prob * 0.22
            + buck_prob * 0.10
        )
        return {
            "balanced": self._clamp_prob(p_bal),
            "momentum": self._clamp_prob(p_mom),
            "empirical": self._clamp_prob(p_emp),
        }

    def _variant_score(self, name: str):
        st = self._variant_stats.get(name, {})
        n = int(st.get("n", 0) or 0)
        if n <= 0:
            return 0.0
        wins = int(st.get("wins", 0) or 0)
        pnl = float(st.get("pnl", 0.0) or 0.0)
        brier = float(st.get("brier", 0.0) or 0.0) / max(1, n)
        wr_lb = self._wilson_lb(wins, n, z=1.0)
        pnl_per = pnl / max(1, n)
        # Maximize realized quality: pnl per trade + conservative wr - calibration penalty.
        return (pnl_per * 0.18) + ((wr_lb - 0.5) * 0.65) - (brier * 0.10)

    def _variant_weights(self, scores: dict):
        keys = list(self._variant_stats.keys())
        total_n = sum(int((self._variant_stats.get(k, {}) or {}).get("n", 0) or 0) for k in keys)
        if total_n < max(6, self.min_samples // 2):
            w = 1.0 / max(1, len(keys))
            return {k: w for k in keys}
        raw = {}
        for k in keys:
            st = self._variant_stats.get(k, {}) or {}
            n = int(st.get("n", 0) or 0)
            rel = float(self._variant_reliability.get(k, 1.0) or 1.0)
            s = float(scores.get(k, 0.0) or 0.0)
            warmup_pen = max(0.0, float(8 - n)) * 0.03
            raw[k] = max(0.02, (rel * 0.7) + (s * 2.2) + 0.45 - warmup_pen)
        z = sum(raw.values())
        if z <= 1e-9:
            w = 1.0 / max(1, len(keys))
            return {k: w for k in keys}
        return {k: float(v) / z for k, v in raw.items()}

    def _calibrate_prob(self, p: float, active: str):
        st = self._variant_stats.get(active, {}) or {}
        n = int(st.get("n", 0) or 0)
        if n < self.min_samples:
            return self._clamp_prob(p)
        cal_err_ema = float(st.get("cal_err_ema", 0.25) or 0.25)
        # When recent calibration error rises, softly shrink confidence toward 0.5.
        shrink = max(0.0, min(0.45, (cal_err_ema - 0.20) * 1.10))
        out = 0.5 + (float(p) - 0.5) * (1.0 - shrink)
        return self._clamp_prob(out)

    def _pick_active_variant(self):
        scores = {k: self._variant_score(k) for k in self._variant_stats.keys()}
        best = max(scores.items(), key=lambda kv: kv[1])[0]
        now = time.time()
        # Avoid model flip-flop.
        if best != self._active_variant and (now - self._last_switch_ts) >= 300:
            self._active_variant = best
            self._last_switch_ts = now
        return self._active_variant, scores

    def predict(self, context: dict, resolved_samples, bucket_rows: dict):
        asset = str(context.get("asset", "") or "")
        duration = int(context.get("duration", 0) or 0)
        side = str(context.get("side", "") or "")
        current = float(context.get("current", 0.0) or 0.0)
        open_price = float(context.get("open_price", 0.0) or 0.0)
        mins_left = float(context.get("mins_left", 0.0) or 0.0)
        base_prob = float(context.get("base_prob", 0.5) or 0.5)
        _ = float(context.get("base_edge", 0.0) or 0.0)
        score = int(context.get("score", 0) or 0)
        bucket_key = str(context.get("bucket_key", "") or "")
        quote_age_ms = float(context.get("quote_age_ms", 9e9) or 9e9)
        analysis_quality = float(context.get("analysis_quality", 0.5) or 0.5)

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

        probs = self._variant_probs(
            base_prob=base_prob * time_weight + (0.5 * (1.0 - time_weight)),
            trend_prob=trend_prob,
            emp_prob=emp_prob,
            buck_prob=buck_prob,
            duration=duration,
        )
        active, scores = self._pick_active_variant()
        weights = self._variant_weights(scores)
        blend_prob = 0.0
        for name, w in weights.items():
            blend_prob += float(probs.get(name, 0.5) or 0.5) * float(w)
        active_prob = float(probs.get(active, base_prob))
        prob = (blend_prob * 0.70) + (active_prob * 0.30)
        prob = self._calibrate_prob(prob, active)

        if quote_age_ms > 2200:
            prob = 0.5 + (prob - 0.5) * 0.80
        elif quote_age_ms > 1400:
            prob = 0.5 + (prob - 0.5) * 0.90
        elif quote_age_ms <= 700:
            prob = 0.5 + (prob - 0.5) * 1.03

        if analysis_quality < 0.45:
            prob = 0.5 + (prob - 0.5) * 0.92
        prob = self._clamp_prob(prob)
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
        if quote_age_ms > 2200:
            score_adj -= 1
        elif quote_age_ms <= 700 and analysis_quality >= 0.60 and prob_adj > 0:
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
            "variant": active,
            "variant_probs": probs,
            "variant_weights": weights,
            "variant_scores": scores,
            "quote_age_ms": quote_age_ms,
        }
        self._pred_hist.append({"ts": time.time(), "ctx": context, "out": out})
        return out

    def observe_outcome(self, trade: dict, won: bool, pnl: float):
        """Online update of shadow variant performance from settled outcomes."""
        try:
            probs = trade.get("pred_variant_probs", {}) or {}
            if not isinstance(probs, dict):
                probs = {}
            y = 1.0 if bool(won) else 0.0
            for name in self._variant_stats.keys():
                p = float(probs.get(name, 0.5) or 0.5)
                st = self._variant_stats[name]
                st["n"] = int(st.get("n", 0) or 0) + 1
                st["wins"] = int(st.get("wins", 0) or 0) + (1 if won else 0)
                st["pnl"] = float(st.get("pnl", 0.0) or 0.0) + float(pnl or 0.0)
                err = (p - y) ** 2
                st["brier"] = float(st.get("brier", 0.0) or 0.0) + err
                prev_cal = float(st.get("cal_err_ema", 0.25) or 0.25)
                st["cal_err_ema"] = (prev_cal * 0.96) + (err * 0.04)
                hit = 1.0 if ((p >= 0.5) == bool(won)) else 0.0
                rel_prev = float(self._variant_reliability.get(name, 1.0) or 1.0)
                rel_new = (rel_prev * 0.97) + ((0.60 + 0.80 * hit) * 0.03)
                self._variant_reliability[name] = max(0.35, min(1.65, rel_new))
            # Re-pick best variant after each settled trade.
            self._pick_active_variant()
        except Exception:
            pass
