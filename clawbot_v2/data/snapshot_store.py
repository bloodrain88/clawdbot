from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SnapshotStore:
    """Shared JSON snapshot storage for dashboard/process decoupling."""

    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "dashboard_snapshot.json"

    def write(self, payload: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        tmp.replace(self.path)

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "ok": True,
                "positions": [],
                "stats": {},
                "message": "snapshot not ready",
            }
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {
                "ok": False,
                "positions": [],
                "stats": {},
                "message": "snapshot parse error",
            }
