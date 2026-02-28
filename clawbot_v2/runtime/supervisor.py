from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class LoopSupervisor:
    """Restarts managed async loops after failure with bounded backoff."""

    def __init__(self, *, base_delay: float = 2.0, max_delay: float = 20.0):
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def run_forever(self, name: str, fn: Callable[[], Awaitable[None]], log) -> None:
        delay = self.base_delay
        while True:
            try:
                await fn()
                log.warning("loop %s exited cleanly; restarting", name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("loop %s crashed: %s", name, exc)
            await asyncio.sleep(delay)
            delay = min(self.max_delay, max(self.base_delay, delay * 1.5))
