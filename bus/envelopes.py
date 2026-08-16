"""Versioned event-envelope schemas and topic helpers for the MQTT bus."""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Any

SCHEMA_VERSION = 1

TELEMETRY_TOPIC_PREFIX = "tele/"
STAGE_TOPIC_PREFIX = "ops/"

# Per-family schema versions (AD-5): telemetry stays v1; task/tool start at v1 with
# their own counters so a new family never bumps the global one.
TASK_SCHEMA_VERSION = 1
TOOL_SCHEMA_VERSION = 1
REQUEST_TOPIC = "request/in"

# Closed task stage menu — DIFFERENTIAL from the ops/ stage names on purpose (AD-3).
TASK_STAGES = (
    "observe", "analyze", "adjudicate", "plan", "act", "verify", "report",
)


def tele_topic(signal_id: str) -> str:
    """Canonical telemetry topic: ``tele/<signal_id>``."""
    return f"{TELEMETRY_TOPIC_PREFIX}{signal_id}"


def stage_topic(stage: str) -> str:
    """Canonical stage-event topic: ``ops/<stage>``."""
    return f"{STAGE_TOPIC_PREFIX}{stage}"


def command_topic(command_name: str) -> str:
    """Canonical actuator command topic: ``cmd/<command_name>`` (AD-7). Published ONLY
    by the ActExecutor; the virtual plant and hardware kit both consume it."""
    return f"cmd/{command_name}"


# ── Track C topic families (AD-5: one publisher per family) ────────────────
def task_topic(task_id: str, event: str) -> str:
    """``task/<task_id>/<event>`` — sole publisher: the supervisor."""
    return f"task/{task_id}/{event}"


def tool_topic(tool: str, event: str) -> str:
    """``tool/<tool>/<event>`` — sole publisher: the Tool port."""
    return f"tool/{tool}/{event}"


def request_topic() -> str:
    """``request/in`` — sole publisher: the dashboard HTTP server."""
    return REQUEST_TOPIC


def approval_topic(task_id: str) -> str:
    """``approval/<task_id>`` — sole publisher: the dashboard HTTP server."""
    return f"approval/{task_id}"


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


@dataclass
class TaskEvent:
    """Envelope on ``task/<task_id>/<event>`` (AD-5). Coordination fields are TOP-LEVEL;
    ``stage_name`` comes from the closed TASK_STAGES enum (AD-3)."""

    event: str
    task_id: str
    stage_name: str = ""        # observe|analyze|adjudicate|plan|act|verify|report
    agent: str = ""             # supervisor|observer|maintenance|production|safety|action
    priority: str = ""          # SAFETY|URGENT|ROUTINE
    payload: dict[str, Any] = field(default_factory=dict)
    ts: str = ""
    schema_version: int = TASK_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = self.schema_version
        return d


@dataclass
class ToolEvent:
    """Envelope on ``tool/<tool>/<event>`` (AD-6). Sole publisher: the Tool port."""

    event: str
    tool: str
    task_id: str = ""
    stage_name: str = ""
    agent: str = ""
    priority: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    ts: str = ""
    schema_version: int = TOOL_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = self.schema_version
        return d