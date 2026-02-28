from __future__ import annotations

from clawbot_v2.config import load_settings
from clawbot_v2.runtime.app import run_main


if __name__ == "__main__":
    run_main(load_settings())
