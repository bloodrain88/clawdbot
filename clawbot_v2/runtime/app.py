from __future__ import annotations

import asyncio

from clawbot_v2.adapters.legacy_engine import LegacyEngine
from clawbot_v2.config import Settings
from clawbot_v2.dashboard import run_dashboard
from clawbot_v2.infra import get_logger


class App:
    """Top-level orchestrator for v2 runtime."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.log = get_logger("clawbot-v2", settings.log_level)

    async def run(self) -> None:
        self.log.info(
            "starting v2 app engine=%s dry_run=%s network=%s",
            self.settings.bot_engine,
            self.settings.dry_run,
            self.settings.network,
        )

        if self.settings.bot_engine == "legacy":
            engine = LegacyEngine(self.settings, self.log)
            if self.settings.dashboard_enabled and self.settings.dashboard_mode == "external":
                await asyncio.gather(
                    engine.run(),
                    run_dashboard(
                        data_dir=self.settings.data_dir,
                        port=self.settings.dashboard_port,
                        log_level=self.settings.log_level,
                    ),
                )
            else:
                await engine.run()
            return

        raise RuntimeError(
            f"Unsupported BOT_ENGINE={self.settings.bot_engine}. "
            "Use BOT_ENGINE=legacy until v2 engine migration is complete."
        )


def run_main(settings: Settings) -> None:
    asyncio.run(App(settings).run())
