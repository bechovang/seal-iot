"""Anomaly episode model and key generation (AD-2/AD-4 header)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def episode_key(signal_group: str, ts_epoch_ms: int, bucket_ms: int = 60_000) -> str:
    """``<signal-group>@<event-ts-bucket>`` in event time (AD-2)."""
    bucket = (ts_epoch_ms // bucket_ms) * bucket_ms
    return f"{signal_group}@{bucket}"


@dataclass
class Episode:
    """An anomaly episode emitted by PERCEIVE with its evidence (AD-4)."""

    episode_key: str
    signal_id: str
    score: float
    filter_innovation: float
    window_stats: dict[str, float]
    ts_epoch_ms: int
    detector: str = "ema_adwin"
    extra: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "episode_key": self.episode_key,
            "signal_id": self.signal_id,
            "score": self.score,
            "filter_innovation": self.filter_innovation,
            "window_stats": self.window_stats,
            "ts_epoch_ms": self.ts_epoch_ms,
            "detector": self.detector,
        }