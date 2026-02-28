from clawbot_v2.config.settings import Settings


def test_settings_type() -> None:
    s = Settings(
        bot_engine="legacy",
        network="polygon",
        dry_run=True,
        data_dir="/data",
        log_level="INFO",
        dashboard_enabled=True,
        dashboard_mode="embedded",
        dashboard_port=8080,
        strict_mode=True,
        force_trade_every_round=True,
        enable_5m=False,
        enable_15m=True,
        min_payout_15m=1.72,
        min_payout_5m=1.75,
    )
    assert s.min_payout_15m >= 1.0
