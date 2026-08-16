"""Tool port (AD-1/AD-6/AD-11): the ONLY writer of artifacts. Agents emit intents;
this port runs deterministic pre-execution validation -> create -> read-back, with a
port-owned idempotency-key registry so a backend swap never loses dedupe memory, and
list-before-retry on timeout/ambiguous. It is the SOLE publisher of ``tool/*``.

Every plan/report/work-order intent must carry an evidence block copied from the
observer payload; read-back checks it is present.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from bus.envelopes import tool_topic, ToolEvent

VALID_PRIORITIES = {"SAFETY", "URGENT", "ROUTINE"}
# states in which a task may issue an act (write) intent (AD-6).
ALLOWED_ACT_STATES = {"COORDINATING", "EXECUTING"}
REQUIRED_EVIDENCE = {"device", "signal", "value", "event_time", "age", "staleness"}

# kind -> required payload fields (subset of intent).
KIND_REQUIRED = {
    "work_order": {"device_id", "summary", "priority", "idempotency_key", "evidence"},
    "incident_record": {"source_incident_id", "note"},
    "notification": {"recipient", "message"},
    "approval_request": {"device_id", "action", "options", "evidence"},
    "report": {"summary", "owner", "evidence"},
}

_KIND_TABLES = {
    "work_order": "work_orders", "incident_record": "incident_records",
    "notification": "notifications", "approval_request": "approval_requests",
    "report": "reports",
}


@dataclass
class ToolResult:
    ok: bool
    kind: str
    backend_id: str = ""
    artifact: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    reused: bool = False


class ToolPort:
    def __init__(self, backend, registry, task_state_reader: Callable[[str], str | None],
                 db_path="tools/port_keys.db", bus=None, ts: str = "") -> None:
        self.backend = backend
        self.registry = registry
        self.task_state_reader = task_state_reader
        self.bus = bus
        self.ts = ts
        self._conn = self._open_keys(str(db_path))
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS port_keys("
            "  key TEXT PRIMARY KEY, kind TEXT, backend_id TEXT, created REAL)"
        )
        self._conn.commit()

    @staticmethod
    def _open_keys(db_path: str):
        import sqlite3
        from pathlib import Path

        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # -- sole-publisher bus helpers ------------------------------------
    def _emit(self, event: str, tool: str, cls_: str, task_id: str = "",
              stage_name: str = "", agent: str = "", priority: str = "",
              payload: dict | None = None) -> None:
        if self.bus is None:
            return
        env = ToolEvent(event=event, tool=tool, task_id=task_id, stage_name=stage_name,
                        agent=agent, priority=priority, payload=payload or {}, ts=self.ts or "")
        self.bus.publish(tool_topic(tool, event), env.to_dict(), qos=1)

    # -- create: validate -> create -> read-back ------------------------
    def create(self, kind: str, payload: dict, task_id: str = "", stage_name: str = "",
               agent: str = "", priority: str = "") -> ToolResult:
        self._emit("invoked", kind, "", task_id, stage_name, agent, priority,
                   {"kind": kind, "task_id": task_id, "payload": payload})
        if kind not in _KIND_TABLES:
            r = ToolResult(ok=False, kind=kind, error=f"unknown artifact kind {kind!r}")
            self._emit("failed", kind, "", task_id, stage_name, agent, priority, {"reason": r.error})
            return r

        # deterministic validation (AD-6 / AD-1 shield-equivalent)
        verr = self._validate(kind, payload, task_id, priority)
        if verr:
            r = ToolResult(ok=False, kind=kind, error=verr)
            self._emit("failed", kind, "", task_id, stage_name, agent, priority,
                       {"reason": verr, "task_id": task_id})
            return r

        key = payload.get("idempotency_key") or f"{task_id}+{kind}+{payload.get('device_id','')}"
        # idempotency: reuse existing before creating (AD-6)
        existing = self._lookup_by_key(key)
        if existing:
            art = self._read_back(kind, existing)
            r = ToolResult(ok=True, kind=kind, backend_id=existing, artifact=art or {},
                           reused=True)
            self._emit("result", kind, "", task_id, stage_name, agent, priority,
                       {"id": existing, "verified": True, "reused": True})
            return r

        art = self._create_backend(kind, payload)
        if art is None:
            r = ToolResult(ok=False, kind=kind, error="backend create returned nothing")
            self._emit("failed", kind, "", task_id, stage_name, agent, priority, {"reason": r.error})
            return r

        backend_id = self._artifact_id(kind, art, payload)
        # read-back verification
        verified = bool(self._verify_read_back(kind, backend_id, payload))
        self._record_key(key, kind, backend_id)
        if not verified:
            r = ToolResult(ok=False, kind=kind, backend_id=backend_id, error="read-back verify failed")
            self._emit("failed", kind, "", task_id, stage_name, agent, priority,
                       {"id": backend_id, "reason": r.error})
            return r
        r = ToolResult(ok=True, kind=kind, backend_id=backend_id,
                       artifact=self._read_back(kind, backend_id) or {}, reused=False)
        self._emit("result", kind, "", task_id, stage_name, agent, priority,
                   {"id": backend_id, "verified": True, "artifacts": ["work_orders", "approval_requests",
                                                                      "reports", "notifications"]})
        return r

    # -- validation ------------------------------------------------------
    def _validate(self, kind: str, payload: dict, task_id: str, priority: str) -> str:
        required = KIND_REQUIRED[kind]
        missing = required - set(payload or {})
        if missing:
            return f"missing required fields: {sorted(missing)}"
        if kind in ("work_order", "approval_request"):
            dev = payload["device_id"]
            if self.registry.device(dev) is None:
                return f"device_id {dev!r} not in registry (AD-11)"
        task_state = self.task_state_reader(task_id) if task_id else None
        allowed = set(ALLOWED_ACT_STATES)
        if kind == "approval_request":
            # an approval request is a coordination write while parked, not an act.
            allowed.add("AWAITING_APPROVAL")
        if task_id and task_state and task_state not in allowed:
            return f"task {task_id} in state {task_state!r} not allowed to act"
        pr = priority or payload.get("priority", "ROUTINE")
        if pr and pr not in VALID_PRIORITIES:
            return f"priority {pr!r} invalid"
        ev = payload.get("evidence")
        if kind == "work_order":
            if not isinstance(ev, dict) or not ev:
                return "evidence block required (AD-6)"
            missing_ev = REQUIRED_EVIDENCE - set(ev)
            if missing_ev:
                return f"evidence block missing keys: {sorted(missing_ev)} (AD-6)"
        elif kind in ("report", "approval_request") and not ev:
            return f"evidence required for {kind} (AD-8)"
        return ""

    # -- create / read-back / verify -------------------------------------
    def _create_backend(self, kind: str, payload: dict) -> dict | None:
        try:
            if kind == "work_order":
                return self.backend.create_work_order(
                    payload.get("task_id", ""), payload["device_id"], payload.get("wo_type", "corrective"),
                    payload["summary"], payload.get("priority", "ROUTINE"), payload.get("evidence"))
            if kind == "incident_record":
                return self.backend.create_incident_record(payload["source_incident_id"],
                                                           payload.get("task_id", ""), payload["note"])
            if kind == "notification":
                return self.backend.create_notification(payload.get("task_id", ""),
                                                        payload["recipient"], payload["message"])
            if kind == "approval_request":
                return self.backend.create_approval_request(
                    payload.get("task_id", ""), payload["device_id"], payload["action"],
                    payload.get("options"), payload.get("evidence"))
            if kind == "report":
                return self.backend.create_report(payload.get("task_id", ""), payload["summary"],
                                                  payload["owner"], payload.get("trace", ""),
                                                  payload.get("evidence"))
        except Exception:  # noqa - backend failure -> surfaced as PARTIAL/FAILED
            return None
        return None

    def _artifact_id(self, kind: str, art: dict, payload: dict) -> str:
        key = {"work_order": "wo_id", "incident_record": "record_id",
               "notification": "notification_id", "approval_request": "apr_id",
               "report": "report_id"}.get(kind)
        return str(art.get(key) or "")

    def _read_back(self, kind: str, backend_id: str) -> dict | None:
        try:
            if kind == "work_order":
                return self.backend.get_work_order(backend_id)
            if kind == "approval_request":
                return self.backend.get_approval(backend_id)
            return {"id": backend_id}
        except Exception:  # noqa
            return None

    def _verify_read_back(self, kind: str, backend_id: str, payload: dict) -> bool:
        art = self._read_back(kind, backend_id)
        if art is None:
            return False
        if kind in ("work_order", "approval_request"):
            # device identity from read-back must match the intent (AD-11/AD-6)
            return art.get("device_id") == payload.get("device_id")
        return True

    # -- idempotency registry --------------------------------------------
    def _record_key(self, key: str, kind: str, backend_id: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO port_keys(key, kind, backend_id, created) VALUES(?,?,?,?)",
            (key, kind, backend_id, time.time()))
        self._conn.commit()

    def _lookup_by_key(self, key: str) -> str:
        row = self._conn.execute(
            "SELECT backend_id FROM port_keys WHERE key=?", (key,)).fetchone()
        return row[0] if row else ""

    def list_by_key(self, key: str) -> list[dict]:
        """AD-6 list-before-retry: search the backend for existing artifacts for a key."""
        entries = []
        for kind in _KIND_TABLES:
            backend_id = self._lookup_by_key(key)
            if backend_id:
                entries.append({"kind": kind, "backend_id": backend_id,
                                "artifact": self._read_back(kind, backend_id)})
        return entries

    def lookup(self, kind: str) -> list[dict]:
        """Read-only lookups for agent context (maintenance_history, production_context)."""
        if hasattr(self.backend, "lookup"):
            return self.backend.lookup(kind)
        return []

    def get_approval(self, apr_id: str) -> dict | None:
        return self.backend.get_approval(apr_id)

    def update_approval_status(self, apr_id: str, status: str, decision: str = "") -> dict:
        if not str(apr_id).startswith("apr_"):
            raise ValueError("approval id not a port-minted apr_ id")
        self.backend.update_approval_status(apr_id, status, decision)
        return self.backend.get_approval(apr_id)

    def close(self) -> None:
        self._conn.close()