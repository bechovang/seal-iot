"""Deterministic playbooks (AD-3): task type -> fixed sequence of stages from the
closed menu, each bound to one agent role, declaring priority, device sets (per
stage), approval marks, inputs, and declared back-edges (re-plan) with attempt caps.
The supervisor executes a playbook; agents fill content, never choose the next hop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bus.envelopes import TASK_STAGES

ALLOWED_STAGES = set(TASK_STAGES)
VALID_PRIORITIES = {"SAFETY", "URGENT", "ROUTINE"}

# The mandatory severity -> priority table (AD-1). Closes the mapping so an incident
# severity can never silently default or land outside the closed priority set.
SEVERITY_PRIORITY = {
    "critical": "SAFETY",
    "high": "URGENT",
    "medium": "URGENT",
    "low": "ROUTINE",
    "info": "ROUTINE",
}


@dataclass
class PlaybookStage:
    name: str                       # from TASK_STAGES
    agent: str                      # supervisor|observer|maintenance|production|safety|action
    devices: list[str] = field(default_factory=list)   # registry device_ids (AD-11)
    approval_marked: bool = False   # critical step -> AWAITING_APPROVAL (AD-8)
    inputs: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackEdge:
    stage: str          # failed stage that may re-enter an earlier stage
    target: str         # stage to re-enter
    cap: int            # max re-entries before -> PARTIAL/FAILED


@dataclass
class Playbook:
    id: str
    stages: list[PlaybookStage]
    priority: str = "ROUTINE"
    back_edges: list[BackEdge] = field(default_factory=list)

    def stage(self, name: str) -> PlaybookStage | None:
        for s in self.stages:
            if s.name == name:
                return s
        return None

    def stage_names(self) -> list[str]:
        return [s.name for s in self.stages]

    def back_edge_cap(self, failed_stage: str) -> int | None:
        for b in self.back_edges:
            if b.stage == failed_stage:
                return b.cap
        return None

    def validate(self) -> list[str]:
        errs: list[str] = []
        if self.priority not in VALID_PRIORITIES:
            errs.append(f"playbook {self.id}: priority {self.priority!r} invalid")
        seen = []
        for s in self.stages:
            if s.name not in ALLOWED_STAGES:
                errs.append(f"playbook {self.id}: illegal stage {s.name!r}")
            seen.append(s.name)
        # control flow must be code-owned: a stage may not skip ahead of the fixed
        # back-edge target, and back-edges must reference real earlier stages.
        order = {n: i for i, n in enumerate(seen)}
        for b in self.back_edges:
            if b.stage not in seen or b.target not in seen:
                errs.append(f"playbook {self.id}: back-edge {(b.stage, b.target)} names unknown stage")
            elif order[b.target] >= order[b.stage]:
                errs.append(f"playbook {self.id}: back-edge target must be EARLIER than failed stage")
        return errs


# ── Seed playbooks (from the plan) ──────────────────────────────────────────
# Scenario 1: prepare an inspection over a device + its related devices.
PREPARE_INSPECTION = Playbook(
    id="prepare_inspection",
    priority="ROUTINE",
    stages=[
        PlaybookStage("observe", "observer", devices=["motor_01"], inputs={"related": True}),
        PlaybookStage("plan",   "maintenance", devices=["motor_01"]),
        PlaybookStage("analyze", "safety", devices=["motor_01"]),
        PlaybookStage("adjudicate", "production", devices=["motor_01"],
                      approval_marked=True),   # conflict trade-off -> human picks (AD-8)
        PlaybookStage("act",   "action", devices=["motor_01"], approval_marked=True),
        PlaybookStage("verify", "observer", devices=["motor_01"], approval_marked=False),
        PlaybookStage("report", "supervisor"),
    ],
    back_edges=[
        BackEdge("act", "plan", cap=2),
        BackEdge("verify", "act", cap=2),
    ],
)

# Scenario 2: production vs safety conflict / urgent order.
CONFLICT_ASSESSMENT = Playbook(
    id="conflict_assessment",
    priority="URGENT",
    stages=[
        PlaybookStage("observe", "observer", devices=["line_01", "motor_01"]),
        PlaybookStage("analyze", "production", devices=["line_01"], inputs={"order_context": True}),
        PlaybookStage("analyze", "safety", devices=["line_01"]),
        PlaybookStage("adjudicate", "production", devices=["line_01"], approval_marked=True),
        PlaybookStage("act", "action", devices=["line_01"], approval_marked=True),
        PlaybookStage("verify", "observer", devices=["line_01"]),
        PlaybookStage("report", "supervisor"),
    ],
    back_edges=[BackEdge("adjudicate", "analyze", cap=1)],
)

# Scenario 3: a device going stale / timing out is observed and re-planned.
LINE_INSPECTION_TIMEOUT = Playbook(
    id="line_inspection_timeout",
    priority="URGENT",
    stages=[
        PlaybookStage("observe", "observer", devices=["line_01", "conveyor_01"]),
        PlaybookStage("plan", "maintenance", devices=["conveyor_01"]),
        PlaybookStage("act", "action", devices=["conveyor_01"]),
        PlaybookStage("verify", "observer", devices=["conveyor_01"]),
        PlaybookStage("report", "supervisor"),
    ],
    back_edges=[
        BackEdge("act", "observe", cap=3),
        BackEdge("verify", "act", cap=2),
    ],
)

# Fallback for operator requests matching no named playbook.
GENERIC = Playbook(
    id="generic",
    priority="ROUTINE",
    stages=[
        PlaybookStage("observe", "observer"),
        PlaybookStage("plan", "maintenance"),
        PlaybookStage("act", "action", approval_marked=True),
        PlaybookStage("verify", "observer"),
        PlaybookStage("report", "supervisor"),
    ],
    back_edges=[BackEdge("act", "plan", cap=2)],
)

PLAYBOOKS: dict[str, Playbook] = {p.id: p for p in
                                  (PREPARE_INSPECTION, CONFLICT_ASSESSMENT,
                                   LINE_INSPECTION_TIMEOUT, GENERIC)}


def get_playbook(playbook_id: str) -> Playbook | None:
    return PLAYBOOKS.get(playbook_id)


# stage_name -> default agent role (closed mapping).
_AGENT_FOR_STAGE = {
    "observe": "observer", "analyze": "safety", "adjudicate": "production",
    "plan": "maintenance", "act": "action", "verify": "observer", "report": "supervisor",
}


def agent_for_stage(stage_name: str) -> str:
    return _AGENT_FOR_STAGE.get(stage_name, "supervisor")