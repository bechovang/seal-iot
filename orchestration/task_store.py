"""Task store + FSM (AD-2): the SOLE writer of coordination task state, a persistent
SQLite (WAL) store that copies the incident-store discipline. Mints ``t-`` ids; a
unique live index ``(source_incident_id, playbook_id)`` keeps spawn idempotent
(AD-1); transitions are validated against one static table; the store resumes at the
last valid state on startup; wall-clock TTL escalates tasks parked in an approval /
clarification wait (AD-8 — the one admitted wall-clock exception).

A task is the unit of coordination and sits ABOVE incident: it references incidents
via ``source_incident_id`` (nullable for operator-requested tasks) and NEVER mutates
incident state.
"""

from __future__ import annotations

import time
import uuid
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Priority closed set (AD-1/AD-7). Order == precedence.
PRIORITIES = ("ROUTINE", "URGENT", "SAFETY")

STATES = {
    "RECEIVED", "PLANNING", "COORDINATING", "AWAITING_APPROVAL", "EXECUTING",
    "VERIFYING", "REPORTED", "AWAITING_CLARIFICATION", "PREEMPTED",
    "PARTIAL", "FAILED", "CANCELLED",
}
TERMINAL = {"REPORTED", "PARTIAL", "FAILED", "CANCELLED"}
ACTIVE = STATES - TERMINAL

# Normal requested lifecycle (AD-2).
MAIN_LIFECYCLE = (
    "RECEIVED", "PLANNING", "COORDINATING", "AWAITING_APPROVAL", "EXECUTING",
    "VERIFYING", "REPORTED",
)

# event -> {from_state: to_state}. Single transition table.
TRANSITIONS: dict[str, dict[str, str]] = {
    "start_planning": {"RECEIVED": "PLANNING", "AWAITING_CLARIFICATION": "PLANNING"},
    "plan_done": {"PLANNING": "COORDINATING"},
    "request_approval": {"COORDINATING": "AWAITING_APPROVAL", "EXECUTING": "AWAITING_APPROVAL"},
    "approve": {"AWAITING_APPROVAL": "COORDINATING"},
    "deny": {"AWAITING_APPROVAL": "PARTIAL"},
    "start_executing": {"COORDINATING": "EXECUTING", "AWAITING_APPROVAL": "EXECUTING"},
    "start_verifying": {"EXECUTING": "VERIFYING"},
    "report": {"VERIFYING": "REPORTED", "COORDINATING": "REPORTED"},
    "request_clarification": {"RECEIVED": "AWAITING_CLARIFICATION", "PLANNING": "AWAITING_CLARIFICATION"},
    "replan": {"RECEIVED": "PLANNING", "PLANNING": "PLANNING", "COORDINATING": "PLANNING",
               "EXECUTING": "PLANNING", "VERIFYING": "PLANNING", "AWAITING_CLARIFICATION": "PLANNING"},
    "preempt": {s: "PREEMPTED" for s in ACTIVE if s != "PREEMPTED"},
    "resume": {"PREEMPTED": "PLANNING"},
    "clarify_reply": {"AWAITING_CLARIFICATION": "RECEIVED"},
    "partial": {s: "PARTIAL" for s in ACTIVE},
    "failed": {s: "FAILED" for s in ACTIVE},
    "cancel": {"RECEIVED": "CANCELLED", "AWAITING_CLARIFICATION": "CANCELLED",
               "AWAITING_APPROVAL": "CANCELLED", "PLANNING": "CANCELLED"},
    "timeout_ttl": {"AWAITING_APPROVAL": "CANCELLED", "AWAITING_CLARIFICATION": "CANCELLED"},
}


@dataclass
class Task:
    task_id: str
    origin: str                 # human | auto
    request_text: str
    source_incident_id: str | None
    playbook_id: str
    priority: str
    state: str
    stage_cursor: str
    replan_count: int
    approval_request_id: str
    evidence_json: str
    fail_step: str
    created: float
    updated: float

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id, "origin": self.origin,
            "request_text": self.request_text, "source_incident_id": self.source_incident_id,
            "playbook_id": self.playbook_id, "priority": self.priority, "state": self.state,
            "stage_cursor": self.stage_cursor, "replan_count": self.replan_count,
            "approval_request_id": self.approval_request_id, "evidence_json": self.evidence_json,
            "fail_step": self.fail_step, "created": self.created, "updated": self.updated,
        }

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL


def _priority_rank(p: str) -> int:
    return PRIORITIES.index(p)


class TaskStore:
    """Persistent SQLite (WAL) single-writer store. Mints ``t-`` ids; dedupes live
    ``(incident_id, playbook_id)`` pairs."""

    def __init__(self, db_path="orchestration/tasks.db",
                 ttl_seconds: float = 3600.0) -> None:
        self.db_path = str(db_path)
        self.ttl_seconds = ttl_seconds
        if str(db_path) != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS tasks(
                task_id TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                request_text TEXT NOT NULL,
                source_incident_id TEXT,
                playbook_id TEXT NOT NULL,
                priority TEXT NOT NULL,
                state TEXT NOT NULL,
                stage_cursor TEXT NOT NULL DEFAULT '',
                replan_count INTEGER NOT NULL DEFAULT 0,
                approval_request_id TEXT NOT NULL DEFAULT '',
                evidence_json TEXT NOT NULL DEFAULT '',
                fail_step TEXT NOT NULL DEFAULT '',
                created REAL NOT NULL,
                updated REAL NOT NULL
            )"""
        )
        # AD-1 idempotent spawn: at most one live task per (incident, playbook).
        self._conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_live_pair
            ON tasks(source_incident_id, playbook_id)
            WHERE state NOT IN ('REPORTED','PARTIAL','FAILED','CANCELLED')
              AND source_incident_id IS NOT NULL"""
        )
        self._conn.commit()

    # -- rows -----------------------------------------------------------
    def _row_to_task(self, row) -> Task:
        return Task(*row)

    def mint(self, playbook_id: str, request_text: str, origin: str = "human",
             source_incident_id: str | None = None, priority: str = "ROUTINE",
             created: float | None = None) -> Task:
        if priority not in PRIORITIES:
            raise ValueError(f"priority {priority!r} not in {list(PRIORITIES)}")
        if origin not in ("human", "auto"):
            raise ValueError(f"origin {origin!r} must be human|auto")
        created = created if created is not None else time.time()
        task_id = "t-" + uuid.uuid4().hex[:8]
        try:
            self._conn.execute(
                "INSERT INTO tasks(task_id,origin,request_text,source_incident_id,playbook_id,"
                "priority,state,created,updated) VALUES(?,?,?,?,?,?,'RECEIVED',?,?)",
                (task_id, origin, request_text, source_incident_id, playbook_id,
                 priority, created, created),
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            # A live (source_incident_id, playbook_id) pair already exists -> find it so
            # the caller can raise its priority instead of minting a duplicate (AD-1).
            raise
        return self.get(task_id)

    # -- idempotent spawn (AD-1) ----------------------------------------
    def live_pair(self, source_incident_id: str, playbook_id: str) -> Optional["Task"]:
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE source_incident_id=? AND playbook_id=? "
            "AND state NOT IN ('REPORTED','PARTIAL','FAILED','CANCELLED')",
            (source_incident_id, playbook_id),
        ).fetchone()
        return self._row_to_task(row) if row else None

    def raise_priority_if_lower(self, task_id: str, priority: str) -> bool:
        """Re-spawn semantics: only ever *raise* (harden) a live pair's priority,
        never mint a duplicate. Returns True when raised."""
        t = self.get(task_id)
        if t is None or priority not in PRIORITIES:
            return False
        if _priority_rank(priority) > _priority_rank(t.priority):
            self._conn.execute(
                "UPDATE tasks SET priority=?, updated=? WHERE task_id=?",
                (priority, time.time(), task_id),
            )
            self._conn.commit()
            return True
        return False

    def get(self, task_id: str) -> Optional[Task]:
        row = self._conn.execute(
            "SELECT task_id,origin,request_text,source_incident_id,playbook_id,priority,state,"
            "stage_cursor,replan_count,approval_request_id,evidence_json,fail_step,created,updated "
            "FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        return self._row_to_task(row) if row else None

    def update_state(self, task_id: str, state: str, fail_step: str = "") -> Task:
        t = self.get(task_id)
        if t is None:
            raise KeyError(task_id)
        now = time.time()
        self._conn.execute(
            "UPDATE tasks SET state=?, updated=?, fail_step=? WHERE task_id=?",
            (state, now, fail_step or t.fail_step, task_id),
        )
        self._conn.commit()
        return self.get(task_id)

    def set_cursor(self, task_id: str, stage_cursor: str) -> Task:
        self._conn.execute(
            "UPDATE tasks SET stage_cursor=?, updated=? WHERE task_id=?",
            (stage_cursor, time.time(), task_id),
        )
        self._conn.commit()
        return self.get(task_id)

    def set_approval(self, task_id: str, approval_request_id: str) -> Task:
        self._conn.execute(
            "UPDATE tasks SET approval_request_id=?, updated=? WHERE task_id=?",
            (approval_request_id, time.time(), task_id),
        )
        self._conn.commit()
        return self.get(task_id)

    def set_evidence(self, task_id: str, evidence_json: str) -> Task:
        self._conn.execute(
            "UPDATE tasks SET evidence_json=?, updated=? WHERE task_id=?",
            (evidence_json, time.time(), task_id),
        )
        self._conn.commit()
        return self.get(task_id)

    def bump_replan(self, task_id: str) -> Task:
        t = self.get(task_id)
        if t is None:
            raise KeyError(task_id)
        self._conn.execute(
            "UPDATE tasks SET replan_count=replan_count+1, updated=? WHERE task_id=?",
            (time.time(), task_id),
        )
        self._conn.commit()
        return self.get(task_id)

    # -- FSM transition (single table) ----------------------------------
    def transition(self, task_id: str, event: str, fail_step: str = "") -> Task:
        t = self.get(task_id)
        if t is None:
            raise KeyError(task_id)
        to = TRANSITIONS.get(event, {}).get(t.state)
        if to is None:
            raise ValueError(f"invalid transition '{event}' from '{t.state}'")
        return self.update_state(task_id, to, fail_step)

    # -- resume / TTL ----------------------------------------------------
    def resume_unfinished(self, now: float | None = None) -> list[Task]:
        now = now if now is not None else time.time()
        rows = self._conn.execute(
            f"SELECT * FROM tasks WHERE state NOT IN ({','.join('?'*len(TERMINAL))})",
            tuple(TERMINAL),
        ).fetchall()
        tasks = [self._row_to_task(r) for r in rows]
        refreshed = []
        for t in tasks:
            if now - t.updated > self.ttl_seconds and t.state in (
                "AWAITING_APPROVAL", "AWAITING_CLARIFICATION"
            ):
                self.transition(t.task_id, "timeout_ttl")
                refreshed.append(self.get(t.task_id))
            else:
                refreshed.append(t)
        return refreshed

    def list_by(self, state: str | None = None) -> list[Task]:
        if state is None:
            rows = self._conn.execute("SELECT * FROM tasks ORDER BY created").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state=? ORDER BY created", (state,)
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def close(self) -> None:
        self._conn.close()


class TaskFSM:
    """Behavior gate around the store — minting, transitions, replan caps, TTL."""

    def __init__(self, store: TaskStore) -> None:
        self.store = store

    def create(self, playbook_id: str, request_text: str, origin: str = "human",
               source_incident_id: str | None = None, priority: str = "ROUTINE",
               created: float | None = None) -> Task:
        return self.store.mint(playbook_id, request_text, origin,
                               source_incident_id, priority, created)

    def transition(self, task_id: str, event: str, fail_step: str = "") -> Task:
        return self.store.transition(task_id, event, fail_step)