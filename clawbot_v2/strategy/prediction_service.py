from __future__ import annotations

from typing import Any


class PredictionService:
    """Thin wrapper around PredictionAgent to decouple strategy from runtime."""

    def __init__(self):
        from prediction_agent import PredictionAgent

        self._agent = PredictionAgent()

    def predict(self, context: dict[str, Any], resolved_samples, bucket_rows: dict):
        return self._agent.predict(context, resolved_samples, bucket_rows)

    def ingest_outcome(self, row: dict[str, Any]) -> None:
        self._agent.ingest_outcome(row)
