"""Simulated CMMS (AD-6/AD-11): SQLite store of work orders, incident RECORDS (keyed
on the harness incident_id — this store NEVER mints an incident identity), notifications,
approval requests, reports, maintenance history and production context.

Local ids are prefixed (``wo_``, ``ntf_``, ``apr_``, ``rpt_``, ``mxt_``); the CMMS
plays the "backend" role behind the Tool port and only ever create/read/list.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
import time
from pathlib import Path
from typing import Any


def _prefix_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class CMMSSim:
    def __init__(self, db_path="tools/cmms.db",
                 production_seed: list[dict] | None = None) -> None:
        self.db_path = str(db_path)
        if str(db_path) != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._seed_history()
        if production_seed:
            self._seed_production(production_seed)
        self._conn.commit()

    # -- schema --------------------------------------------------------
    def _init_schema(self) -> None:
        c = self._conn
        c.execute(
            """CREATE TABLE IF NOT EXISTS work_orders(
                wo_id TEXT PRIMARY KEY, task_id TEXT, device_id TEXT, wo_type TEXT,
                summary TEXT, priority TEXT, status TEXT DEFAULT 'OPEN',
                evidence_json TEXT DEFAULT '{}', created REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS incident_records(
                rec_id TEXT PRIMARY KEY, source_incident_id TEXT NOT NULL,
                task_id TEXT, note TEXT, created REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS notifications(
                ntf_id TEXT PRIMARY KEY, task_id TEXT, recipient TEXT, message TEXT,
                status TEXT DEFAULT 'SENT', created REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS approval_requests(
                apr_id TEXT PRIMARY KEY, task_id TEXT, device_id TEXT, action TEXT,
                options_json TEXT DEFAULT '[]', evidence_json TEXT DEFAULT '{}',
                status TEXT DEFAULT 'PENDING', decision TEXT DEFAULT '',
                created REAL, updated REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS reports(
                rpt_id TEXT PRIMARY KEY, task_id TEXT, summary TEXT, owner TEXT,
                state TEXT DEFAULT 'REPORTED', trace TEXT DEFAULT '',
                evidence_json TEXT DEFAULT '{}', created REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS maintenance_history(
                mxt_id TEXT PRIMARY KEY, device_id TEXT, work_type TEXT, result TEXT,
                created REAL)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS production_context(
                order_id TEXT PRIMARY KEY, priority TEXT, line TEXT, due TEXT, status TEXT)"""
        )

    # -- seeds ---------------------------------------------------------
    def _seed_history(self) -> None:
        if any(r[0] for r in self._conn.execute('SELECT 1 FROM maintenance_history LIMIT 1')):
            return
        seed = [
            ("motor_01", "inspection", "ok", time.time() - 3600),
            ("line_01", "repair", "ok", time.time() - 7200),
            ("press_01", "calibration", "ok", time.time() - 86400),
        ]
        for dev, wt, res, created in seed:
            self._conn.execute(
                "INSERT INTO maintenance_history(mxt_id, device_id, work_type, result, created)"
                " VALUES(?,?,?,?,?)", (_prefix_id("mxt"), dev, wt, res, created))

    def _seed_production(self, seed: list[dict]) -> None:
        for order in seed:
            self._conn.execute(
                "INSERT OR REPLACE INTO production_context(order_id, priority, line, due, status)"
                " VALUES(?,?,?,?,?)",
                (order.get("order_id"), order.get("priority", "ROUTINE"), order.get("line", ""),
                 order.get("due", ""), order.get("status", "active")))

    # -- writes (called ONLY by the Tool port) -------------------------
    def create_work_order(self, task_id: str, device_id: str, wo_type: str,
                          summary: str, priority: str, evidence: dict | None = None) -> dict:
        wo = _prefix_id("wo")
        now = time.time()
        self._conn.execute(
            "INSERT INTO work_orders(wo_id, task_id, device_id, wo_type, summary, priority,"
            " status, evidence_json, created) VALUES(?,?,?,?,?,?,'OPEN',?,?)",
            (wo, task_id, device_id, wo_type, summary, priority,
             json.dumps(evidence or {}), now))
        self._conn.commit()
        return self.get_work_order(wo)

    def create_incident_record(self, source_incident_id: str, task_id: str,
                               note: str) -> dict:
        rec = _prefix_id("ir")
        self._conn.execute(
            "INSERT INTO incident_records(rec_id, source_incident_id, task_id, note, created)"
            " VALUES(?,?,?,?,?)", (rec, source_incident_id, task_id, note, time.time()))
        self._conn.commit()
        return {"record_id": rec, "source_incident_id": source_incident_id,
                "task_id": task_id, "note": note}

    def create_notification(self, task_id: str, recipient: str, message: str) -> dict:
        ntf = _prefix_id("ntf")
        now = time.time()
        self._conn.execute(
            "INSERT INTO notifications(ntf_id, task_id, recipient, message, status, created)"
            " VALUES(?,?,?,?,'SENT',?)", (ntf, task_id, recipient, message, now))
        self._conn.commit()
        return {"notification_id": ntf, "task_id": task_id, "message": message,
                "status": "SENT"}

    def create_approval_request(self, task_id: str, device_id: str, action: str,
                                options: list | None = None,
                                evidence: dict | None = None) -> dict:
        apr = _prefix_id("apr")
        now = time.time()
        self._conn.execute(
            "INSERT INTO approval_requests(apr_id, task_id, device_id, action, options_json,"
            " evidence_json, status, created, updated) VALUES(?,?,?,?,?,?,'PENDING',?,?)",
            (apr, task_id, device_id, action, json.dumps(options or []),
             json.dumps(evidence or {}), now, now))
        self._conn.commit()
        return self.get_approval(apr)

    def update_approval_status(self, apr_id: str, status: str, decision: str = "") -> dict:
        self._conn.execute(
            "UPDATE approval_requests SET status=?, decision=?, updated=? WHERE apr_id=?",
            (status, decision, time.time(), apr_id))
        self._conn.commit()
        return self.get_approval(apr_id)

    def create_report(self, task_id: str, summary: str, owner: str,
                      trace: str = "", evidence: dict | None = None) -> dict:
        rpt = _prefix_id("rpt")
        now = time.time()
        self._conn.execute(
            "INSERT INTO reports(rpt_id, task_id, summary, owner, state, trace, evidence_json,"
            " created) VALUES(?,?,?,?,'REPORTED',?,?,?)",
            (rpt, task_id, summary, owner, trace, json.dumps(evidence or {}), now))
        self._conn.commit()
        return {"report_id": rpt, "task_id": task_id, "summary": summary,
                "owner": owner, "state": "REPORTED"}

    # -- reads ---------------------------------------------------------
    def get_work_order(self, wo_id: str) -> dict | None:
        r = self._conn.execute("SELECT * FROM work_orders WHERE wo_id=?", (wo_id,)).fetchone()
        if r is None:
            return None
        return dict(zip(("wo_id", "task_id", "device_id", "wo_type", "summary",
                         "priority", "status", "evidence_json", "created"), r))

    def get_approval(self, apr_id: str) -> dict | None:
        r = self._conn.execute(
            "SELECT * FROM approval_requests WHERE apr_id=?", (apr_id,)).fetchone()
        if r is None:
            return None
        return dict(zip(("apr_id", "task_id", "device_id", "action", "options_json",
                         "evidence_json", "status", "decision", "created", "updated"), r))

    def list_by_key(self, key: str, value: str) -> list[dict]:
        """Lookup helper (AD-6 list-before-retry): across artifact tables keyed by a
        column value (task_id / device_id)."""
        out = []
        col = "device_id" if key == "device_id" else "task_id"
        for table, idk in (("work_orders", "wo_id"), ("approval_requests", "apr_id"),
                           ("notifications", "ntf_id"), ("reports", "rpt_id")):
            try:
                rows = self._conn.execute(
                    f"SELECT * FROM {table} WHERE {col}=?", (value,)).fetchall()
            except sqlite3.Error:
                continue
            for r in rows:
                out.append({"table": table, "id": r[0]})
        return out

    def lookup(self, kind: str) -> list[dict]:
        """Read-only lookup by artifact kind (maintenance_history, production_context,
        work_orders, approval_requests, reports, notifications)."""
        if kind == "maintenance_history":
            return [dict(zip(("mxt_id", "device_id", "work_type", "result", "created"), r))
                    for r in self._conn.execute(
                        "SELECT * FROM maintenance_history ORDER BY created DESC LIMIT 50")]
        if kind == "production_context":
            return [dict(zip(("order_id", "priority", "line", "due", "status"), r))
                    for r in self._conn.execute(
                        "SELECT * FROM production_context WHERE status='active'")]
        if kind == "work_orders":
            return [self.get_work_order(r[0]) for r in self._conn.execute(
                "SELECT wo_id FROM work_orders ORDER BY created DESC LIMIT 200")]
        if kind == "approval_requests":
            rows = self._conn.execute(
                "SELECT apr_id FROM approval_requests ORDER BY created DESC LIMIT 200").fetchall()
            return [self.get_approval(r[0]) for r in rows]
        if kind == "reports":
            return [dict(zip(("rpt_id", "task_id", "summary", "owner", "state", "trace",
                              "evidence_json", "created"), r))
                    for r in self._conn.execute(
                        "SELECT * FROM reports ORDER BY created DESC LIMIT 100")]
        if kind == "notifications":
            return [dict(zip(("ntf_id", "task_id", "recipient", "message",
                              "status", "created"), r))
                    for r in self._conn.execute(
                        "SELECT * FROM notifications ORDER BY created DESC LIMIT 100")]
        return []

    def close(self) -> None:
        self._conn.close()