from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SettlementResult:
    ok: bool
    redeemed_usdc: float
    message: str


class SettlementManager:
    """Settlement/redeem boundary for migrated v2 flow."""

    async def reconcile_and_redeem(self) -> SettlementResult:
        return SettlementResult(ok=True, redeemed_usdc=0.0, message="noop")
