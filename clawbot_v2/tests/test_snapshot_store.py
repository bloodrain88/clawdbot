from pathlib import Path

from clawbot_v2.data.snapshot_store import SnapshotStore


def test_snapshot_store_roundtrip(tmp_path: Path) -> None:
    store = SnapshotStore(str(tmp_path))
    payload = {"ok": True, "positions": [{"asset": "ETH"}]}
    store.write(payload)
    out = store.read()
    assert out["ok"] is True
    assert out["positions"][0]["asset"] == "ETH"
