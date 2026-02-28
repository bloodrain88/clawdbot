"""Compatibility shim: all live runtime moved to clawbot_v2.engine.live_trader."""

from clawbot_v2.engine.live_trader import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        _install_runtime_json_log()
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print("[PERF] uvloop enabled — faster async event loop")
        except ImportError:
            pass
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
