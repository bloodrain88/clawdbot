import asyncio
from collections import defaultdict


class NonceManager:
    """Thread-safe nonce allocator with pending nonce source-of-truth."""

    def __init__(self, web3, address):
        self.w3 = web3
        self.address = address
        self._lock = asyncio.Lock()
        self._next_nonce = None

    async def next_nonce(self, loop):
        async with self._lock:
            chain_nonce = await loop.run_in_executor(
                None, lambda: self.w3.eth.get_transaction_count(self.address, "pending")
            )
            if self._next_nonce is None or self._next_nonce < chain_nonce:
                self._next_nonce = chain_nonce
            out = self._next_nonce
            self._next_nonce += 1
            return out

    async def reset_from_chain(self, loop):
        async with self._lock:
            self._next_nonce = await loop.run_in_executor(
                None, lambda: self.w3.eth.get_transaction_count(self.address, "pending")
            )


class ErrorTracker:
    """Lightweight error counters with periodic surfacing."""

    def __init__(self):
        self.counts = defaultdict(int)

    def tick(self, key: str, log_fn, err=None, every: int = 25):
        self.counts[key] += 1
        n = self.counts[key]
        if n % every == 0:
            suffix = f" last={err}" if err else ""
            log_fn(f"[WARN] {key} repeated {n}x{suffix}")


class BucketStats:
    """Bucketed execution/outcome tracker."""

    def __init__(self):
        self.rows = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0, "slip_bps": 0.0, "fills": 0})

    def add_fill(self, bucket: str, slip_bps: float):
        r = self.rows[bucket]
        r["n"] += 1
        r["fills"] += 1
        r["slip_bps"] += slip_bps

    def add_outcome(self, bucket: str, won: bool, pnl: float):
        r = self.rows[bucket]
        r["n"] += 1
        if won:
            r["wins"] += 1
        r["pnl"] += pnl
