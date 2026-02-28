from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any


class RuntimeEventLogger:
    """Append-only structured event logger for runtime observability."""

    def __init__(self, data_dir: str, filename: str = "runtime_events.jsonl"):
        self.path = Path(data_dir) / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def emit(self, event: str, **fields: Any) -> None:
        payload = {
            "ts": time.time(),
            "event": event,
            **fields,
        }
        row = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(row + "\n")
