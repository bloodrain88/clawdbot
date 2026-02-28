from clawbot_v2.domain.models import Signal
from clawbot_v2.strategy.gates import pass_core_gates


def test_gate_payout_below() -> None:
    sig = Signal(
        asset="BTC",
        duration=15,
        side="Up",
        score=12,
        true_prob=0.61,
        entry=0.59,
        payout_mult=1.69,
        execution_ev=0.03,
    )
    ok, reason = pass_core_gates(sig, min_payout=1.72, min_ev=0.01, min_score=10)
    assert not ok
    assert reason == "payout_below"
