from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from clawbot_v2.config import Settings
from clawbot_v2.data.snapshot_store import SnapshotStore
from clawbot_v2.infra import RuntimeEventLogger


@dataclass
class LoopHealth:
    name: str
    restarts: int = 0
    last_error: str = ""
    alive: bool = False


@dataclass
class RuntimeHealth:
    loops: dict[str, LoopHealth] = field(default_factory=dict)

    def touch(self, name: str, *, alive: bool | None = None, err: str = "") -> None:
        h = self.loops.get(name)
        if h is None:
            h = LoopHealth(name=name)
            self.loops[name] = h
        if alive is not None:
            h.alive = alive
        if err:
            h.last_error = err

    def restarted(self, name: str, err: Exception) -> None:
        self.touch(name, alive=False, err=str(err))
        self.loops[name].restarts += 1

    def summary(self) -> str:
        if not self.loops:
            return "loops=0"
        up = sum(1 for h in self.loops.values() if h.alive)
        total = len(self.loops)
        restarts = sum(int(h.restarts) for h in self.loops.values())
        return f"loops={up}/{total} restarts={restarts}"


class ModularEngine:
    """Primary v2 runtime: explicit supervised loops (no monolithic run path)."""

    LOOP_NAMES = (
        "stream_rtds",
        "_stream_binance_spot",
        "_stream_binance_futures",
        "_stream_binance_aggtrade",
        "_stream_clob_market_book",
        "vol_loop",
        "scan_loop",
        "_status_loop",
        "_refresh_balance",
        "_redeem_loop",
        "_heartbeat_loop",
        "_user_events_loop",
        "_force_redeem_backfill_loop",
        "chainlink_loop",
        "chainlink_ws_loop",
        "_copyflow_refresh_loop",
        "_copyflow_live_loop",
        "_copyflow_intel_loop",
        "_rpc_optimizer_loop",
        "_redeemable_scan",
        "_position_sync_loop",
        "_stream_binance_liquidations",
        "_oi_ls_loop",
        "_dashboard_loop",
    )

    def __init__(self, settings: Settings, log):
        self.settings = settings
        self.log = log
        self.health = RuntimeHealth()
        self.events = RuntimeEventLogger(settings.data_dir)

    async def _snapshot_loop(self, trader, store: SnapshotStore) -> None:
        while True:
            try:
                payload = trader._dashboard_data()
                store.write(payload)
                self.events.emit("snapshot.write", open_positions=len(payload.get("positions", []) or []))
            except Exception as exc:
                self.log.warning("snapshot loop error: %s", exc)
                self.events.emit("snapshot.error", error=str(exc))
            await asyncio.sleep(1.0)

    async def _health_loop(self) -> None:
        while True:
            self.log.info("runtime-health %s", self.health.summary())
            await asyncio.sleep(30.0)

    async def _supervise_loop(self, name: str, fn: Callable[[], Awaitable[None]]) -> None:
        while True:
            try:
                self.health.touch(name, alive=True)
                self.events.emit("loop.start", name=name)
                await fn()
                self.health.touch(name, alive=False)
                self.events.emit("loop.exit", name=name)
                return
            except Exception as exc:
                self.health.restarted(name, exc)
                self.log.error("loop-crash name=%s err=%s", name, exc)
                self.events.emit("loop.crash", name=name, error=str(exc), restarts=self.health.loops[name].restarts)
                await asyncio.sleep(10.0)

    async def _bootstrap(self, trader) -> None:
        self.log.info("bootstrap: self-check")
        self.events.emit("bootstrap.start")
        trader._startup_self_check()

        while True:
            try:
                trader.init_clob()
                break
            except Exception as exc:
                self.log.error("bootstrap: init_clob failed err=%s", exc)
                self.events.emit("bootstrap.init_clob_retry", error=str(exc))
                await asyncio.sleep(20.0)

        trader._sync_redeemable()
        await trader._sync_stats_from_api()
        await trader._warmup_active_cid_cache()
        await trader._seed_binance_cache()
        self.events.emit("bootstrap.ready")

    async def run(self) -> None:
        from clawbot_v2.engine.live_trader import ADDRESS, DRY_RUN, NETWORK, LiveTrader

        trader = LiveTrader()

        self.log.info(
            "starting modular engine network=%s wallet=%s dry_run=%s",
            NETWORK,
            f"{ADDRESS[:10]}...",
            DRY_RUN,
        )
        self.events.emit("engine.start", network=NETWORK, wallet=f"{ADDRESS[:10]}...", dry_run=bool(DRY_RUN))

        if self.settings.dashboard_mode == "external":
            self.log.info("dashboard mode external: enabling snapshot writer")
            store = SnapshotStore(self.settings.data_dir)
            trader._dashboard_loop = lambda: self._snapshot_loop(trader, store)

        await self._bootstrap(trader)

        tasks: list[asyncio.Task] = [asyncio.create_task(self._health_loop(), name="runtime-health")]
        for name in self.LOOP_NAMES:
            fn = getattr(trader, name, None)
            if fn is None:
                self.log.warning("missing-loop name=%s", name)
                self.events.emit("loop.missing", name=name)
                continue
            tasks.append(asyncio.create_task(self._supervise_loop(name, fn), name=f"loop:{name}"))

        await asyncio.gather(*tasks)
