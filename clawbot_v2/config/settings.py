from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, min_value: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None:
        value = default
    else:
        value = int(raw)
    if min_value is not None:
        value = max(min_value, value)
    return value


def _env_float(name: str, default: float, min_value: float | None = None) -> float:
    raw = os.environ.get(name)
    if raw is None:
        value = default
    else:
        value = float(raw)
    if min_value is not None:
        value = max(min_value, value)
    return value


@dataclass(frozen=True)
class Settings:
    bot_engine: str
    network: str
    dry_run: bool
    data_dir: str
    log_level: str
    dashboard_enabled: bool
    dashboard_mode: str
    dashboard_port: int
    strict_mode: bool
    force_trade_every_round: bool
    enable_5m: bool
    enable_15m: bool
    min_payout_15m: float
    min_payout_5m: float


def load_settings() -> Settings:
    return Settings(
        bot_engine=os.environ.get("BOT_ENGINE", "legacy").strip().lower(),
        network=os.environ.get("POLY_NETWORK", "polygon").strip().lower(),
        dry_run=_env_bool("DRY_RUN", True),
        data_dir=os.environ.get("DATA_DIR", "/data"),
        log_level=os.environ.get("LOG_LEVEL", "INFO").strip().upper(),
        dashboard_enabled=_env_bool("DASHBOARD_ENABLED", True),
        dashboard_mode=os.environ.get("DASHBOARD_MODE", "embedded").strip().lower(),
        dashboard_port=_env_int("DASHBOARD_PORT", 8080, min_value=1),
        strict_mode=_env_bool("STRICT_MODE", True),
        force_trade_every_round=_env_bool("FORCE_TRADE_EVERY_ROUND", True),
        enable_5m=_env_bool("ENABLE_5M", False),
        enable_15m=_env_bool("ENABLE_15M", True),
        min_payout_15m=_env_float("MIN_PAYOUT_MULT", 1.72, min_value=1.0),
        min_payout_5m=_env_float("MIN_PAYOUT_MULT_5M", 1.75, min_value=1.0),
    )
