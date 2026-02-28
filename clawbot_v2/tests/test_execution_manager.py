import asyncio

from clawbot_v2.domain.models import Signal, TradeIntent
from clawbot_v2.execution.manager import ExecutionManager


def test_execution_dry_run() -> None:
    manager = ExecutionManager(dry_run=True)
    sig = Signal(
        asset="BTC",
        duration=15,
        side="Up",
        score=11,
        true_prob=0.6,
        entry=0.52,
        payout_mult=1.92,
        execution_ev=0.02,
    )
    intent = TradeIntent(signal=sig, notional_usdc=2.5, max_entry=0.52)
    out = asyncio.run(manager.place(intent))
    assert out.ok
    assert out.reason == "dry_run"
