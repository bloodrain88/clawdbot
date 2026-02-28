from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Signal:
    asset: str
    duration: int
    side: str
    score: int
    true_prob: float
    entry: float
    payout_mult: float
    execution_ev: float


@dataclass(frozen=True)
class TradeIntent:
    signal: Signal
    notional_usdc: float
    max_entry: float
    force_taker: bool = False
