from __future__ import annotations

import asyncio

from clawbot_v2.data.snapshot_store import SnapshotStore


class LegacyEngine:
    """Compatibility adapter that runs existing monolithic LiveTrader."""

    def __init__(self, settings, log):
        self.settings = settings
        self.log = log

    async def _legacy_snapshot_loop(self, trader, store: SnapshotStore) -> None:
        while True:
            try:
                payload = trader._dashboard_data()
                store.write(payload)
            except Exception as exc:
                self.log.warning("legacy snapshot loop error: %s", exc)
            await asyncio.sleep(1.0)

    async def run(self) -> None:
        self.log.warning(
            "running legacy engine through v2 adapter; migration in progress"
        )
        from clawbot_v2.engine.live_trader import LiveTrader

        trader = LiveTrader()
        if self.settings.dashboard_mode == "external":
            self.log.info("legacy dashboard mode=external (snapshot writer enabled)")
            store = SnapshotStore(self.settings.data_dir)
            trader._dashboard_loop = lambda: self._legacy_snapshot_loop(trader, store)
        await trader.run()
