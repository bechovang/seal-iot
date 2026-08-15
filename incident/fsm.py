"""Incident FSM (AD-2): the SOLE minter of incident ids, persistent in SQLite (WAL),
resumable across restarts, TTL auto-escalates stalled incidents. Upstream events carry
episode_key only; the incident id is minted here.

States (single transition table): DETECTED -> DIAGNOSING -> PLANNING -> ACTING ->
VERIFYING -> RESOLVED | ESCALATED, with VERIFYING -> DIAGNOSING (bounded retries).
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

STATES = {
    "DETECTED", "DIAGNOSING", "PLANNING", "ACTING", "VERIFYING", "RESOLVED", "ESCALATED",
}
TERMINAL = {"RESOLVED", "ESCALATED"}

# event -> {from_state: to_state}
TRANSITIONS: dict[str, dict[str, str]] = {
    "start_detected": {"NONE": "DETECTED", "DETECTED": "DETECTED"},
    "diagnose": {"DETECTED": "DIAGNOSING"},
    "plan": {"DIAGNOSING": "PLANNING", "VERIFYING": "PLANNING"},
    "act": {"PLANNING": "ACTING"},
    "verify": {"ACTING": "VERIFYING"},
    "resolve": {"VERIFYING": "RESOLVED", "PLANNING": "RESOLVED"},
    "retry": {"VERIFYING": "DIAGNOSING"},
    "escalate": {"DETECTED": "ESCALATED", "DIAGNOSING": "ESCALATED", "PLANNING": "ESCALATED",
                 "ACTING": "ESCALATED", "VERIFYING": "ESCALATED"},
    "human_approve": {"ESCALATED": "PLANNING", "ACTING": "ACTING", "DETECTED": "ORPHAN"},
}


@dataclass
class Incident:
    incident_id: str
    episode_key: str
    state: str
    created: float
    updated: float
    retries: int = 0
    candidate_json: str = ""

    def to_dict(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "episode_key": self.episode_key,
            "state": self.state,
            "created": self.created,
            "updated": self.updated,
            "retries": self.retries,
            "candidate": self.candidate_json,
        }


class IncidentStore:
    """Persistent SQLite store with WAL; single-writer, resumable."""

    def __init__(self, db_path, retry_max: int = 3, ttl_seconds: float = 3600.0) -> None:
        self.db_path = str(db_path)
        self.retry_max = retry_max
        self.ttl_seconds = ttl_seconds
        if str(db_path) != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS incidents(
                incident_id TEXT PRIMARY KEY,
                episode_key TEXT NOT NULL,
                state TEXT NOT NULL,
                created REAL NOT NULL,
                updated REAL NOT NULL,
                retries INTEGER NOT NULL DEFAULT 0,
                candidate_json TEXT NOT NULL DEFAULT ''
            )"""
        )
        self._conn.commit()

    def mint(self, episode_key: str, created: float | None = None) -> str:
        created = created if created is not None else time.time()
        iid = uuid.uuid4().hex[:12]
        self._conn.execute(
            "INSERT INTO incidents(incident_id, episode_key, state, created, updated) "
            "VALUES(?,?,?,?,?)",
            (iid, episode_key, "DETECTED", created, created),
        )
        self._conn.commit()
        return iid

    def get(self, incident_id: str) -> Optional[Incident]:
        row = self._conn.execute(
            "SELECT incident_id, episode_key, state, created, updated, retries, candidate_json "
            "FROM incidents WHERE incident_id=?", (incident_id,)
        ).fetchone()
        if row is None:
            return None
        return Incident(*row)

    def update_state(self, incident_id: str, state: str, updated: float, retries: int | None = None) -> None:
        if retries is None:
            retries = (self.get(incident_id) or Incident("", "", "", 0, 0)).retries
        self._conn.execute(
            "UPDATE incidents SET state=?, updated=?, retries=? WHERE incident_id=?",
            (state, updated, retries, incident_id),
        )
        self._conn.commit()

    def set_candidate(self, incident_id: str, candidate_json: str) -> None:
        self._conn.execute(
            "UPDATE incidents SET candidate_json=? WHERE incident_id=?",
            (candidate_json, incident_id),
        )
        self._conn.commit()

    def resume_unfinished(self, now: float | None = None) -> list[Incident]:
        """Load non-terminal incidents so unfinished remediation is never lost."""
        now = now if now is not None else time.time()
        rows = self._conn.execute(
            "SELECT incident_id, episode_key, state, created, updated, retries, candidate_json "
            "FROM incidents WHERE state NOT IN (?,?)", tuple(TERMINAL)
        ).fetchall()
        incidents = [Incident(*r) for r in rows]
        for inc in incidents:
            if now - inc.updated > self.ttl_seconds:
                self.update_state(inc.incident_id, "ESCALATED", now)
                inc.state = "ESCALATED"
        return incidents

    def close(self) -> None:
        self._conn.close()


class IncidentFSM:
    """State machine with a single transition table. The sole behavior gate around
    the store (minting, transitions, retries, TTL escalation)."""

    def __init__(self, store: IncidentStore, retry_max: int | None = None,
                 distill_hook=None) -> None:
        self.store = store
        self.retry_max = retry_max if retry_max is not None else store.retry_max
        self.distill_hook = distill_hook  # called on RESOLVED inside the transaction

    def create(self, episode_key: str, ts: str = "") -> str:
        created = _event_seconds(ts)
        return self.store.mint(episode_key, created)

    def transition(self, incident_id: str, event: str, ts: str = "") -> Incident | None:
        inc = self.store.get(incident_id)
        if inc is None:
            return None
        now = _event_seconds(ts)
        to = TRANSITIONS.get(event, {}).get(inc.state)
        if to is None:
            raise ValueError(f"invalid transition '{event}' from '{inc.state}'")
        if to == "ORPHAN":
            # not a real destination; human disapprove path
            self.store.update_state(incident_id, "ESCALATED", now)
            return self.store.get(incident_id)
        self.store.update_state(incident_id, to, now)
        return self.store.get(incident_id)

    def retry_possible(self, incident_id: str) -> bool:
        inc = self.store.get(incident_id)
        return inc is not None and inc.retries < self.retry_max

    def verify_outcome(self, incident_id: str, outcome: str, ts: str = "") -> Incident:
        """AD-9: only 'improved' resolves; no_change/worsened retry (bounded) or escalate.
        On resolve, fires the distill hook inside the same transaction (AD-3) so a
        concurrent runbook read can never race a new write."""
        if outcome == "improved":
            inc = self.transition(incident_id, "resolve", ts)
            if self.distill_hook and inc is not None:
                self.distill_hook(inc)
            return inc
        else:
            inc = self.store.get(incident_id)
            if inc is not None and inc.retries < self.retry_max:
                self.store.update_state(incident_id, "DIAGNOSING", _event_seconds(ts), inc.retries + 1)
                inc = self.store.get(incident_id)
            else:
                inc = self.transition(incident_id, "escalate", ts)
        return inc


def _event_seconds(ts: str) -> float:
    if not ts:
        return time.time()
    # ISO-8601 with Z -> epoch seconds; wall clock fallback only for logs
    try:
        base = ts.replace("Z", "+00:00")
        from datetime import datetime

        return datetime.fromisoformat(base).timestamp()
    except ValueError:
        return time.time()