"""Agent role base (AD-4): each role is an object with its OWN LLM conversation and
OWN ``LLMClient`` instance. A role never chooses the next hop and never holds a bus
or store handle — the supervisor is its only I/O, and it returns structured JSON.
Every role has a rehearsed degraded fallback so no content is ever fabricated from
inputs that don't support it (AD-10 / AD-13).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bus.envelopes import TASK_STAGES
from config import TrackCRegistry, TrackCRuntimeConfig


@dataclass
class Observation:
    """One device observation with a first-class staleness finding (AD-10) — the
    evidence block that downstream stages and work orders must carry."""

    device_id: str
    signal_id: str
    value: float | None
    event_ts: str
    age_seconds: float | None
    staleness: str            # fresh|stale|critical_stale|offline|missing_ts
    quality: str = "ok"
    unit: str = ""

    def to_evidence(self) -> dict[str, Any]:
        return {
            "device": self.device_id, "signal": self.signal_id, "value": self.value,
            "event_time": self.event_ts, "age": self.age_seconds, "staleness": self.staleness,
            "quality": self.quality, "unit": self.unit,
        }

    def to_dict(self) -> dict[str, Any]:
        """Canonical serialization (field names) for persistence / rebuild."""
        return {
            "device_id": self.device_id, "signal_id": self.signal_id, "value": self.value,
            "event_ts": self.event_ts, "age_seconds": self.age_seconds,
            "staleness": self.staleness, "quality": self.quality, "unit": self.unit,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Observation":
        return cls(
            device_id=d.get("device_id", ""), signal_id=d.get("signal_id", ""),
            value=d.get("value"), event_ts=d.get("event_ts", ""),
            age_seconds=d.get("age_seconds"), staleness=d.get("staleness", "fresh"),
            quality=d.get("quality", "ok"), unit=d.get("unit", ""),
        )


class AgentContext:
    """Everything a stage agent is allowed to see. Assembled per-stage by the
    supervisor; deliberately offers NO writes and NO bus/queue handles."""

    def __init__(self, *, task: Any, stage: Any, playbook: Any, registry: TrackCRegistry,
                 runtime: TrackCRuntimeConfig, history: Any = None,
                 observations: list[Observation] | None = None,
                 previous: dict[str, Any] | None = None,
                 cmms: Any = None, runbook_store: Any = None, mode: str = "live") -> None:
        self.task = task
        self.stage = stage
        self.playbook = playbook
        self.registry = registry
        self.runtime = runtime
        self.history = history
        self.observations = observations or []
        self.previous = previous or {}
        self.cmms = cmms
        self.runbook_store = runbook_store
        self.mode = mode

    @property
    def stage_name(self) -> str:
        return self.stage.name if hasattr(self.stage, "name") else str(self.stage)

    def device_ids(self) -> list[str]:
        return getattr(self.stage, "devices", []) or []


class BaseAgent:
    """One LLM context. Subclasses implement ``deterministic`` output + optional LLM
    narrative; ``run`` prefers structured core and always degrades safely."""

    role = "base"

    def __init__(self, llm_client: Any = None, registry: TrackCRegistry | None = None,
                 runtime: TrackCRuntimeConfig | None = None) -> None:
        self.llm = llm_client
        self.registry = registry
        self.runtime = runtime or TrackCRuntimeConfig()

    def run(self, ctx: AgentContext) -> dict[str, Any]:
        """Return a structured JSON result for this stage. Never raises on LLM
        failure — returns the deterministic output."""
        core = self.deterministic(ctx)
        narrative = self._narrative(ctx)   # optional LLM enrichment
        core["agent"] = self.role
        core["stage"] = ctx.stage_name
        core["priority"] = ctx.task.priority if hasattr(ctx.task, "priority") else "ROUTINE"
        if narrative:
            core["narrative"] = narrative
        return core

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        """Default: expose lineage. Roles override with real content."""
        return {"degraded": True, "note": f"{self.role} degraded output"}

    def _narrative(self, ctx: AgentContext) -> str | None:
        if self.llm is None:
            return None
        prompt = self._prompt(ctx)
        if not prompt:
            return None
        out = self.llm.complete_json(
            f"You are the '{self.role}' agent role in a multi-agent factory ops "
            "coordination task. Return ONLY a JSON object.",
            prompt,
        )
        if isinstance(out, dict):
            return out.get("summary") or out.get("explanation") or out.get("proposal") or ""
        return None

    def _prompt(self, ctx: AgentContext) -> str:
        return ""