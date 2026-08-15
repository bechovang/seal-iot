"""Versioned event-envelope schemas and topic helpers for the MQTT bus."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

SCHEMA_VERSION = 1

TELEMETRY_TOPIC_PREFIX = "tele/"
STAGE_TOPIC_PREFIX = "ops/"


def tele_topic(signal_id: str) -> str:
    """Canonical telemetry topic: ``tele/<signal_id>``."""
    return f"{TELEMETRY_TOPIC_PREFIX}{signal_id}"


def stage_topic(stage: str) -> str:
    """Canonical stage-event topic: ``ops/<stage>``."""
    return f"{STAGE_TOPIC_PREFIX}{stage}"


@dataclass
class TelemetryEnvelope:
    """Telemetry message on ``tele/<signal_id>`` (QoS 1, non-retained)."""

    signal_id: str
    ts: str  # ISO-8601 event time, assigned by the adapter from source
    value: float
    unit: str = ""
    quality: str = "ok"
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = self.schema_version
        return d


@dataclass
class StageEvent:
    """Envelope on ``ops/<stage>`` carried across the loop."""

    event: str
    source: str
    ts: str
    payload: dict[str, Any]
    episode_key: str | None = None
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = self.schema_version
        return d