from clawbot_v2.domain.models import Signal
from clawbot_v2.strategy.engine import StrategyConfig, StrategyEngine


def test_decide_ok_15m() -> None:
    engine = StrategyEngine(StrategyConfig())
    sig = Signal(
        asset="ETH",
        duration=15,
        side="Down",
        score=12,
        true_prob=0.62,
        entry=0.55,
        payout_mult=1.82,
        execution_ev=0.028,
    )
    intent, reason = engine.decide(sig, bankroll_usdc=100.0)
    assert reason == "ok"
    assert intent is not None
    assert intent.notional_usdc > 0


def test_decide_blocked_by_ev() -> None:
    engine = StrategyEngine(StrategyConfig())
    sig = Signal(
        asset="BTC",
        duration=15,
        side="Up",
        score=12,
        true_prob=0.57,
        entry=0.56,
        payout_mult=1.78,
        execution_ev=0.001,
    )
    intent, reason = engine.decide(sig, bankroll_usdc=100.0)
    assert intent is None
    assert reason == "ev_below"
