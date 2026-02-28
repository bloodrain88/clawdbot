from __future__ import annotations

from clawbot_v2.domain import Signal


def pass_core_gates(
    signal: Signal,
    *,
    min_payout: float,
    min_ev: float,
    min_score: int,
) -> tuple[bool, str]:
    if signal.score < min_score:
        return False, "score_below"
    if signal.payout_mult < min_payout:
        return False, "payout_below"
    if signal.execution_ev < min_ev:
        return False, "ev_below"
    return True, "ok"
