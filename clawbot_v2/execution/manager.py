from __future__ import annotations

from dataclasses import dataclass

from clawbot_v2.domain import TradeIntent


@dataclass(frozen=True)
class ExecutionResult:
    ok: bool
    reason: str
    fill_price: float = 0.0
    notional_usdc: float = 0.0
    order_id: str = ""


class ExecutionManager:
    """Execution boundary. Keeps order-routing out of strategy/scoring code."""

    def __init__(self, *, dry_run: bool = True):
        self.dry_run = dry_run

    async def place(self, intent: TradeIntent) -> ExecutionResult:
        if self.dry_run:
            return ExecutionResult(
                ok=True,
                reason="dry_run",
                fill_price=float(intent.signal.entry),
                notional_usdc=float(intent.notional_usdc),
                order_id="dry-run-order",
            )
        return ExecutionResult(ok=False, reason="live execution not migrated yet")
