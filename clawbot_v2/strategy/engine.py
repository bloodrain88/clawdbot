from __future__ import annotations

from dataclasses import dataclass

from clawbot_v2.domain import Signal, TradeIntent
from clawbot_v2.strategy.gates import pass_core_gates


@dataclass(frozen=True)
class StrategyConfig:
    min_score_15m: int = 0
    min_score_5m: int = 0
    min_ev_15m: float = 0.010
    min_ev_5m: float = 0.015
    min_payout_15m: float = 1.72
    min_payout_5m: float = 1.75
    risk_fraction: float = 0.025
    min_notional: float = 1.0


class StrategyEngine:
    """Pure strategy-to-intent conversion, side-effect free."""

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg

    def decide(self, signal: Signal, *, bankroll_usdc: float) -> tuple[TradeIntent | None, str]:
        is_5m = int(signal.duration) <= 5
        min_score = self.cfg.min_score_5m if is_5m else self.cfg.min_score_15m
        min_ev = self.cfg.min_ev_5m if is_5m else self.cfg.min_ev_15m
        min_payout = self.cfg.min_payout_5m if is_5m else self.cfg.min_payout_15m

        ok, reason = pass_core_gates(
            signal,
            min_payout=min_payout,
            min_ev=min_ev,
            min_score=min_score,
        )
        if not ok:
            return None, reason

        notional = max(self.cfg.min_notional, float(bankroll_usdc) * self.cfg.risk_fraction)
        intent = TradeIntent(
            signal=signal,
            notional_usdc=notional,
            max_entry=float(signal.entry),
            force_taker=False,
        )
        return intent, "ok"
