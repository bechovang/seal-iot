"""Subscribe-only demo dashboard (Epic 5.2 / AD-14) — control-room aggregation.

A zero-build, static/serve-only consumer: subscribes to ``ops/*``, ``cmd/*`` and
``tele/*`` and aggregates the full pipeline story (perceive -> diagnose -> decide ->
shield -> verify -> FSM -> learn) into a snapshot that the SPA (``ui/app.html``)
polls at ~1s. It is purely a consumer — with the dashboard killed the loop keeps
running. Latency is derived from the event timestamp column (single clock), not from
wall clock, so a natural HAI attack expiry is never mistaken for improvement (5.6).

Uses only the standard library.
"""

from __future__ import annotations

import socket
import threading
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

STAGES = ["perceive", "diagnose", "decide", "act", "verify", "incident"]


# match ephemeral 'incident_id' inside event payloads when present
def _lookup_id(payload: dict) -> str | None:
    for k in ("incident_id", "episode_key"):
        if k in payload:
            return str(payload[k])
    return None


def iso_ms(ts: str | int) -> int | None:
    """Parse '1970-01-01T00:00:05Z' (or a raw int epoch-ms) -> ms since epoch
    (single event-time clock). Purely a value, never wall-clock."""
    if ts is None or ts == "":
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    t = str(ts).replace("Z", "+00:00")
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(t)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


class Dashboard:
    """Collects events and answers a per-incident snapshot (thread-safe).

    Legacy keys (incidents, stage_counts, recent, pipeline_counts, elapsed_s) are
    unchanged; additive keys (variant, signals, markers, shield, decisions, verifies,
    fsm, learn, counters) power the control-room SPA.
    """

    def __init__(self, max_events: int = 200, series_points: int = 240,
                 thresholds: dict | None = None) -> None:
        # Track C runtime thresholds (AD-10): pushed to the SPA so the staleness bars
        # and approval TTL countdown match what the supervisor actually enforces.
        thr = thresholds or {}
        self._thresholds = {
            "stale_seconds": float(thr.get("stale_seconds", 30.0)),
            "critical_seconds": float(thr.get("critical_seconds", 120.0)),
            "approve_ttl_seconds": float(thr.get("approve_ttl_seconds", 300.0)),
            "clarify_ttl_seconds": float(thr.get("clarify_ttl_seconds", 600.0)),
        }
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []  # ring: ops/* and cmd/* only
        self.max_events = max_events
        self.series_points = series_points
        self.start_ts = time.time()

        self._stage_counts: dict[str, int] = {}
        self._series: dict[str, dict] = {}     # signal_id -> {unit, points deque}
        self._decisions: "OrderedDict[str, dict]" = OrderedDict()
        self._verifies: "OrderedDict[str, dict]" = OrderedDict()
        self._diagnoses: "OrderedDict[str, dict]" = OrderedDict()
        self._fsm: "OrderedDict[str, list]" = OrderedDict()
        self._shield: list[dict] = []
        self._learn: "OrderedDict[str, dict]" = OrderedDict()
        self._markers: list[dict] = []          # {ts_ms, kind, label}
        self._alias: dict[str, str] = {}        # episode_key -> incident_id
        self._counters = {"tele": 0, "cmd": 0, "ops": 0, "shield_blocked": 0,
                          "runbook_hits": 0, "rounds": 0}
        self._variant = ""
        # Track C (AD-1/AD-5): the dashboard is the SOLE publisher of request/in and
        # approval/<task_id>; it also aggregates task/* it consumes from the supervisor.
        self._pub = None        # set_publisher(busPublishable)
        self._tasks: "OrderedDict[str, dict]" = OrderedDict()
        self._tools: "OrderedDict[str, dict]" = OrderedDict()   # AD-6 tool port audit
        self._bridge = {"connected": False, "since": "", "last_seen": None}
        self._incfeed: list[dict] = []          # auto_incident feed (FE-1)
        self._policy = {"mode": "human", "delay_s": 0.0}
        self._last_tele_at = time.time()
        self._last_tele_ts_ms: int | None = None

    # -- normalization -------------------------------------------------------
    @staticmethod
    def _normalize(payload: dict) -> dict:
        """Unwrap a versioned envelope so downstream sees the inner event payload.
        A flat payload passes through unchanged. Best-effort: a schema_version or a
        dict-valued ``payload`` signals an envelope."""
        if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
            inner = payload.get("payload") or {}
            # keep the envelope's episode_key/ts as fallbacks
            if inner.get("episode_key") is None and payload.get("episode_key"):
                inner["episode_key"] = payload["episode_key"]
            return inner
        return payload

    # -- routing -------------------------------------------------------------
    def handler(self, topic: str, payload: dict) -> None:
        # task/tool/bridge envelopes keep their TOP-LEVEL fields (event/agent/stage_name/
        # ts/payload), so pass the RAW envelope to those ingress paths — _normalize would
        # strip them and lose approval_id/options/evidence (BE-4). tele/op unwrap as before.
        if topic.startswith("tele/"):
            inner = self._normalize(payload) or payload
            with self._lock:
                self._ingest_tele(topic, inner)
            return
        if topic.startswith("task/"):
            with self._lock:
                self._ingest_task(topic, payload)
            return
        if topic.startswith("tool/"):
            with self._lock:
                self._ingest_tool(topic, payload)
            return
        if topic.startswith("bridge/"):
            with self._lock:
                self._ingest_bridge(topic, payload)
            return
        inner = self._normalize(payload) or payload
        with self._lock:
            self._ingest_op(topic, payload, inner)

    # -- Track C: sole publisher of request/in + approval/<task_id> (AD-5) ---
    def set_publisher(self, pub) -> None:
        """Attach a bus-publishable (InMemoryBus, or a transport the harness wires)."""
        self._pub = pub

    def set_policy(self, mode: str, delay_s: float) -> None:
        """Approval policy hiển thị lên UI (FE-2): 'auto' demo / 'human' production."""
        self._policy = {"mode": mode, "delay_s": delay_s}

    def publish_request(self, text: str, playbook_id: str | None = None,
                        priority: str | None = None, in_reply_to: str | None = None) -> dict:
        """Render a human request into the request/in topic. The dashboard is the
        only publisher of this family (AD-5); the supervisor is the only consumer."""
        from bus.envelopes import request_topic

        if self._pub is None:
            return {"published": False, "reason": "no publisher attached"}
        payload: dict[str, Any] = {"text": text, "request_text": text}
        if playbook_id:
            payload["playbook_id"] = playbook_id
        if priority:
            payload["priority"] = priority
        if in_reply_to:
            payload["in_reply_to"] = in_reply_to
        self._pub.publish(request_topic(), payload, qos=1)
        return {"published": True}

    def publish_decision(self, task_id: str, approval_id: str, decision: str) -> dict:
        """Render an operator decision into approval/<task_id>. Sole publisher of this
        family as well (AD-5/AD-8)."""
        from bus.envelopes import approval_topic

        if self._pub is None:
            return {"published": False, "reason": "no publisher attached"}
        if decision not in ("APPROVED", "DENIED"):
            return {"published": False, "reason": "decision must be APPROVED|DENIED"}
        payload = {"approval_id": approval_id, "decision": decision}
        self._pub.publish(approval_topic(task_id), payload, qos=1)
        return {"published": True}

    def _ingest_task(self, topic: str, env: dict) -> None:
        """Aggregate task/* frames (the raw envelope) into a per-task control card.
        This is what the SPA renders for the FSM stepper + approval console + replan
        badge + PARTIAL reason (BE-4). Input is the TaskEvent envelope (its ``payload``
        is the inner event payload with task dict / output / approval_id)."""
        parts = topic.split("/")
        task_id = parts[1] if len(parts) > 1 else ""
        event = parts[-1]
        ev = env.get("event") or event
        rec = self._tasks.setdefault(task_id, {"task_id": task_id, "events": [],
                                                "stage": "", "state": "", "ts": "",
                                                "agent": "", "approval": None,
                                                "replans": 0, "fail_reason": "",
                                                "finding": "", "report": "",
                                                "stage_outputs": []})
        rec["events"].append(ev)
        del rec["events"][:-256]
        if env.get("stage_name"):
            rec["stage"] = env["stage_name"]
        if env.get("agent"):
            rec["agent"] = env["agent"]
        p = env.get("payload") if isinstance(env.get("payload"), dict) else {}
        task_info = p.get("task") if isinstance(p.get("task"), dict) else {}
        # FSM state: task dict (opened/handoff) hoặc field "state" do event mang theo
        # (approval_requested mang AWAITING_APPROVAL, closed mang terminal state)
        st = task_info.get("state") or p.get("state")
        rec["state"] = st if st else rec.get("state", "")
        if ev == "closed" and st == "PARTIAL":
            rec["fail_reason"] = p.get("fail") or p.get("reason") or ""
        if env.get("ts"):
            rec["ts"] = env["ts"]
        # approval_granted: store là EXECUTING nhưng event không mang task dict — set rõ
        if ev == "approval_granted":
            rec["state"] = "EXECUTING"
            rec["approval"] = None
        # approval card (AD-8): capture approval_id+options so the SPA can POST /api/decision
        if ev == "approval_requested":
            rec["approval"] = {
                "approval_id": p.get("approval_id", ""),
                "options": p.get("options", []),
                "stage": rec["stage"] or p.get("stage", ""),
                "device": p.get("device", ""),
                "ts": env.get("ts", ""),
            }
        elif ev in ("approval_granted", "approval_denied"):
            rec["approval"] = None
        # replan loop (AD-3): badge + last failed_stage
        if ev == "replan":
            rec["replans"] = rec.get("replans", 0) + 1
            rec["replan_stage"] = p.get("failed_stage", "")
            rec["replan_target"] = p.get("target", "")
            rec["replan_reason"] = p.get("reason", "")
            rec["replan_attempts"] = p.get("attempts", rec.get("replans", 0))
        # stage_done output: finding (observe) / report (report)
        out = p.get("output") if isinstance(p.get("output"), dict) else {}
        if ev == "stage_done" and rec["stage"] == "observe" and out.get("finding"):
            rec["finding"] = out["finding"]
        if ev == "stage_done" and rec["stage"] == "report" and out.get("summary"):
            rec["report"] = out["summary"]
        if env.get("stage_name"):
            rec["stage_outputs"].append({"stage": env["stage_name"],
                                           "agent": env.get("agent", ""),
                                           "ts": env.get("ts", "")})
            del rec["stage_outputs"][:-64]
        self._prune(self._tasks, cap=128)

    def _ingest_tool(self, topic: str, env: dict) -> None:
        """tool/<kind>/<event> -> audit trail cho Tool port panel (AD-6). Input is the
        raw ToolEvent envelope (tool / payload carry the ids + reused flag)."""
        parts = topic.split("/")
        kind = parts[1] if len(parts) > 2 else (parts[-1] if parts else "")
        evt = parts[-1]
        p = env.get("payload") if isinstance(env.get("payload"), dict) else {}
        rec = self._tools.setdefault(env.get("tool") or kind,
                                     {"kind": kind, "events": [], "last": "", "id": ""})
        rec["events"].append({"event": evt, "id": p.get("id", ""),
                               "reused": bool(p.get("reused")),
                               "reason": p.get("reason", ""), "ts": env.get("ts", "")})
        del rec["events"][:-64]
        rec["last"] = evt
        rec["id"] = p.get("id") or rec["id"]
        self._counters["tool"] = self._counters.get("tool", 0) + 1
        if evt == "failed":
            self._markers.append({"ts_ms": iso_ms(env.get("ts")), "kind": "tool_failed",
                                  "label": kind, "incident_id": p.get("task_id")})
        self._prune(self._tools, cap=100)

    def _ingest_bridge(self, topic: str, env: dict) -> None:
        """bridge/heartbeat (sole publisher: TrackCBridge, AD-5 mở rộng cho source
        health) -> chip trạng thái kết nối cho control room."""
        self._bridge["connected"] = bool(env.get("connected"))
        self._bridge["since"] = env.get("ts", "")
        self._bridge["last_seen"] = time.time()

    def _ingest_tele(self, topic: str, inner: dict) -> None:
        self._counters["tele"] += 1
        sig = inner.get("signal_id") or topic.rsplit("/", 1)[-1]
        s = self._series.setdefault(sig, {"unit": inner.get("unit", ""), "points": []})
        if inner.get("unit"):
            s["unit"] = inner["unit"]
        pts = s["points"]
        pts.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                    "value": inner.get("value"), "phase": inner.get("phase", ""),
                    "quality": inner.get("quality", "ok")})
        del pts[:-self.series_points]
        # clock bookmarks for the two epoch-clock chips (AD-10 / AD-9)
        self._last_tele_at = time.time()
        ts_ms = pts[-1]["ts_ms"] if pts else None
        if ts_ms:
            self._last_tele_ts_ms = max(self._last_tele_ts_ms or 0, ts_ms)

    def _ingest_op(self, topic: str, payload: dict, inner: dict) -> None:
        self._counters["ops"] += 1
        leaf = topic.rsplit("/", 1)[-1]
        stage = "act" if topic.startswith("cmd/") else leaf
        if not topic.startswith("tele/"):
            self._stage_counts[stage] = self._stage_counts.get(stage, 0) + 1
        # ops/cmd go to the recent ring too (never tele -> avoids flood)
        if topic.startswith("cmd/") or topic.startswith("ops/"):
            self._events.append({"topic": topic, "payload": payload})
            if len(self._events) > self.max_events:
                self._events = self._events[-self.max_events:]

        iid = _lookup_id(inner) or _lookup_id(payload)
        # learn the episode_key -> incident_id alias so perceive/diagnose (which only
        # know episode_key) merge into the one incident the decide/result minted.
        ek = inner.get("episode_key") or payload.get("episode_key")
        inc = inner.get("incident_id") or payload.get("incident_id")
        if ek and inc and ek != inc:
            self._alias[ek] = inc
            iid = inc

        if topic.startswith("cmd/"):
            self._counters["cmd"] += 1
            self._markers.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                                  "kind": "action", "label": leaf,
                                  "incident_id": iid})
        elif leaf == "shield":
            self._shield.append({"incident_id": iid,
                                 "action": inner.get("action"),
                                 "target": inner.get("target"),
                                 "allowed": inner.get("allowed"),
                                 "reason": inner.get("reason"),
                                 "envelope_abs": inner.get("envelope_abs"),
                                 "ts": inner.get("ts")})
            if inner.get("allowed") is False:
                self._counters["shield_blocked"] += 1
                self._markers.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                                      "kind": "blocked", "label": str(inner.get("action") or "?"),
                                      "incident_id": iid})
        elif leaf == "decide" and iid is not None:
            self._decisions[iid] = dict(inner)
            self._prune(self._decisions)
        elif leaf == "verify" and iid is not None:
            self._verifies[iid] = dict(inner)
            self._prune(self._verifies)
        elif leaf == "diagnose" and iid is not None:
            self._diagnoses[iid] = dict(inner)
            self._counters["runbook_hits"] += (1 if inner.get("runbook_hit") else 0)
            if inner.get("runbook_hit"):
                self._markers.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                                      "kind": "runbook", "label": "hit", "incident_id": iid})
            self._prune(self._diagnoses)
        elif leaf == "incident" and iid is not None:
            trail = self._fsm.setdefault(iid, [])
            trail.append({"incident_id": iid,
                          "episode_key": inner.get("episode_key"),
                          "event": inner.get("event"),
                          "from_state": inner.get("from_state"),
                          "to_state": inner.get("to_state"),
                          "retries": inner.get("retries", 0),
                          "ts": inner.get("ts")})
            del trail[:-64]
        elif leaf == "learn" and iid is not None:
            self._learn[iid] = dict(inner)
            self._prune(self._learn, cap=50)
        elif leaf == "perceive":
            self._markers.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                                  "kind": "anomaly", "label": inner.get("detector") or "?",
                                  "incident_id": iid})
        elif leaf == "result":
            self._markers.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                                  "kind": "result", "label": inner.get("state") or "?",
                                  "incident_id": iid})
        elif leaf == "demo":
            self._variant = str(inner.get("variant") or "")
            self._counters["rounds"] = int(inner.get("round") or 0)
        elif leaf == "incident_feed":
            self._incfeed.append(dict(inner))
            del self._incfeed[:-100]
            self._markers.append({"ts_ms": iso_ms(inner.get("ts")),
                                  "kind": "auto_incident",
                                  "label": str(inner.get("device") or "?"),
                                  "incident_id": inner.get("incident_id")})

    @staticmethod
    def _prune(od, cap: int = 100) -> None:
        while len(od) > cap:
            od.popitem(last=False)

    # -- snapshot ------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            events = list(self._events)
            stage_counts = dict(self._stage_counts)
            series = {k: {"unit": v["unit"], "points": list(v["points"])}
                      for k, v in self._series.items()}
            decisions = dict(self._decisions)
            verifies = dict(self._verifies)
            diagnoses = dict(self._diagnoses)
            fsm = {k: list(v) for k, v in self._fsm.items()}
            shield = list(self._shield)
            learn = dict(self._learn)
            markers = list(self._markers)
            alias = dict(self._alias)
            counters = dict(self._counters)
            variant = self._variant
            tasks = dict(self._tasks)
            tools = dict(self._tools)
            bridge = dict(self._bridge)
            incfeed = list(self._incfeed)
            policy = dict(self._policy)
            thresholds = dict(self._thresholds)
            last_tele_at = self._last_tele_at
            last_tele_ts_ms = self._last_tele_ts_ms

        # legacy per-incident fidelity (unchanged behaviour + alias merge)
        incidents: dict[str, dict] = {}
        stages: dict[str, int] = {}
        for rec in events:
            topic = rec["topic"]
            payload_rec = rec["payload"]
            inner = self._normalize(payload_rec)
            leaf = topic.split("/")[-1]
            stage = "act" if topic.startswith("cmd/") else leaf
            stages[stage] = stages.get(stage, 0) + 1
            iid = _lookup_id(inner) or _lookup_id(payload_rec)
            if iid is None:
                continue
            # resolve episode_key -> incident_id so perceive/diagnose fold in
            if iid in alias:
                iid = alias[iid]
            d = incidents.setdefault(
                iid, {"id": iid, "stages": {}, "order": [], "last_ts": None,
                       "state": None, "state_ts": None}
            )
            now = iso_ms(inner.get("ts") or inner.get("ts_ms"))
            d["stages"][stage] = rec
            if stage not in d["order"]:
                d["order"].append(stage)
            if now is not None and (d["last_ts"] is None or d["last_ts"] < now):
                d["last_ts"] = now
            state = inner.get("state") or (inner.get("outcome") or {}).get("classification")
            if state is not None and (d["state_ts"] is None or now is None or d["state_ts"] <= (now or 0)):
                d["state"] = str(state)
                d["state_ts"] = now
        for d in incidents.values():
            order = d["order"]
            lat = {}
            prev = None
            prev_stage = None
            for st in STAGES:
                if st in d["stages"]:
                    inner = self._normalize(d["stages"][st]["payload"])
                    ts = iso_ms(inner.get("ts") or inner.get("ts_ms"))
                    if prev is not None and ts is not None:
                        lat[f"{prev_stage}->{st}"] = max(0, ts - prev)
                    prev, prev_stage = ts, st
            d["latency_ms"] = lat
        # device grid (AD-10): group by signal prefix, age against ref_ts = the LARGEST
        # event-time across every signal (reg ich semantics), never wall clock.
        ref = max((p["ts_ms"] for s in series.values() for p in s["points"]
                   if p["ts_ms"]), default=None)
        devices: dict[str, dict] = {}
        for sig, s in series.items():
            dev_id = sig.rsplit("_", 1)[0]
            last = s["points"][-1] if s["points"] else {}
            ts_ms = last.get("ts_ms")
            age_s = round((ref - ts_ms) / 1000.0, 1) if (ref is not None and ts_ms) else None
            q = last.get("quality", "ok")
            if q in ("offline", "error", "missing_ts"):
                worst = q
            elif age_s is not None and age_s >= thresholds["critical_seconds"]:
                worst = "stale-critical"
            elif age_s is not None and age_s >= thresholds["stale_seconds"]:
                worst = "stale"
            else:
                worst = "ok"
            d = devices.setdefault(dev_id, {"device": dev_id, "signals": [],
                                            "worst": "ok", "age_s": None,
                                            "value": None, "unit": ""})
            d["signals"].append({"signal": sig, "value": last.get("value"),
                                 "unit": s.get("unit", ""), "quality": q,
                                 "age_s": age_s})
            order = {"ok": 0, "stale": 1, "stale-critical": 2,
                     "error": 3, "offline": 4, "missing_ts": 3}
            if order.get(worst, 0) > order.get(d["worst"], 0):
                d.update(worst=worst, age_s=age_s, value=last.get("value"),
                         unit=s.get("unit", ""))
        # 2 on the epoch clocks (AD-10): broker epoch lag vs wall clock — proves event-time
        clocks = {
            "tele_ts_ms": last_tele_ts_ms,
            "epoch_lag_s": round((time.time() - last_tele_ts_ms / 1000.0), 1)
                           if last_tele_ts_ms else None,
            "tele_idle_s": round(time.time() - last_tele_at, 1),
        }
        return {
            "elapsed_s": round(time.time() - self.start_ts, 2),
            "stage_counts": stages,
            "incidents": list(incidents.values()),
            "recent": events[-40:],
            "pipeline_counts": {s: stages.get(s, 0) for s in STAGES},
            # additive control-room keys
            "variant": variant,
            "signals": series,
            "markers": markers,
            "shield": shield,
            "decisions": decisions,
            "verifies": verifies,
            "diagnoses": diagnoses,
            "fsm": fsm,
            "learn": learn,
            "counters": counters,
            "tasks": tasks,
            "tools": tools,
            "bridge": bridge,
            "auto_incidents": incfeed,
            "policy": policy,
            "thresholds": thresholds,
            "devices": list(devices.values()),
            "clocks": clocks,
        }


def _esc(v: Any) -> str:
    s = "" if v is None else f"{v}"
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def _stage_color(st: str) -> str:
    return {
        "perceive": "#4ea1ff", "diagnose": "#b388ff", "decide": "#ffb04d",
        "act": "#ff7a59", "verify": "#37d67a", "incident": "#ff5c8a",
        "cmd": "#ff7a59", "learn": "#ffd166",
    }.get(st, "#8c9baf")


def _flag(v: Any) -> str:
    return "✓" if v is True else ("✗" if v is False else _esc(v))


def _render(admin: BaseHTTPRequestHandler, data: dict, as_json: bool = False) -> None:
    import json

    if as_json:
        j = json.dumps(data, ensure_ascii=False, indent=1)
        body = j.encode("utf-8")
        admin.send_response(200)
        admin.send_header("Content-Type", "application/json; charset=utf-8")
        admin.send_header("Content-Length", str(len(body)))
        admin.end_headers()
        admin.wfile.write(body)
        return
    st = data["stage_counts"]
    pipe = data["pipeline_counts"]
    steps = ["".join(
        f"<div class='step'><div class='dot' style='background:{_stage_color(s)}'></div>"
        f"<div class='sname'>{_esc(s)}</div><div class='scnt'>{pipe.get(s, 0)}</div></div>")
        for s in STAGES]
    flow = "<div class='dot connected'></div>".join(steps)

    cards = []
    for inc in data["incidents"]:
        bad = _flag(inc.get("state"))
        chips = "".join(
            f"<span class='chip' style='border-color:{_stage_color(s)}'>{_esc(s)}</span>"
            for s in inc.get("order", []))
        lat_rows = "".join(
            f"<tr><td>{_esc(k)}</td><td class='num'>{(v or 0) / 1000.0:.3f}s</td>"
            f"<td><div class='barwrap'><div class='bar' style='width:{min((v or 0) / 30.0, 100):.1f}%;background:{_stage_color(k.split('->')[-1])}'></div></div></td></tr>"
            for k, v in sorted(inc.get("latency_ms", {}).items()))
        cards.append(
            f"<div class='card'><div class='card-h'><span class='iid'>{_esc(inc.get('id'))}</span>"
            f"<span class='state'>{bad}</span><span class='muted'>last {_esc(inc.get('state_ts'))}</span></div>"
            f"<div class='chips'>{chips}</div>"
            f"<div class='grp'><div class='grp-t'>Stage latency (event-time)</div>"
            f"<table>{lat_rows}</table></div></div>")
    cards_html = "\n".join(cards) or "<div class='muted'>no incidents yet</div>"

    rows = "".join(
        f"<tr><td>{_esc(r['topic'])}</td><td class='num'>{_esc(r['payload'].get('ts') or r['payload'].get('ts_ms') or '-')}</td></tr>"
        for r in data["recent"])

    body = (f"<html><head><meta charset='utf-8'><title>SEAL Smart Ops</title>"
            f"<meta http-equiv='refresh' content='2'>"
            f"<style>"
            f"body{{font-family:'Consolas','Courier New',monospace;background:#0f1420;color:#e6eaf2;margin:0;padding:18px}}"
            f"h1{{font-size:18px;margin:0 0 4px}} .sub{{color:#7f8ba3;font-size:12px;margin-bottom:14px}}"
            f".flow{{display:flex;align-items:flex-start;gap:2px;flex-wrap:wrap;background:#161d2e;border:1px solid #232d42;border-radius:10px;padding:12px 14px;margin-bottom:16px}}"
            f".step{{text-align:center;min-width:86px}} .dot{{width:14px;height:14px;border-radius:50%;margin:0 auto 6px}}"
            f".dot.connected{{width:26px;height:3px;background:#2b3a55;margin:6px 0}}"
            f".sname{{font-size:11px;color:#cbd5e9}} .scnt{{font-size:16px;font-weight:bold}}"
            f".card{{background:#161d2e;border:1px solid #232d42;border-radius:10px;padding:12px 14px;margin-bottom:12px}}"
            f".card-h{{display:flex;gap:12px;align-items:center;margin-bottom:8px}}"
            f".iid{{font-weight:bold;color:#9ad0ff}} .state{{font-weight:bold;color:#37d67a}}"
            f".chips{{margin-bottom:10px}} .chip{{display:inline-block;border:1px solid;border-radius:20px;padding:1px 10px;font-size:11px;margin-right:6px}}"
            f".grp-t{{font-size:12px;color:#7f8ba3;margin-bottom:4px}} table{{border-collapse:collapse;font-size:12px}} td{{padding:1px 12px 1px 0}}"
            f".num{{text-align:right;font-variant-numeric:tabular-nums;color:#9ad0ff}}"
            f".barwrap{{width:140px;height:10px;background:#0b101b;border-radius:5px;overflow:hidden}} .bar{{height:100%}}"
            f".muted{{color:#5b6b87;font-size:11px}} table.ev{{width:100%;font-size:11px}} thead td{{color:#7f8ba3}}"
            f"</style></head><body>"
            f"<h1>SEAL Smart Ops Harness</h1><div class='sub'>live pipeline · elapsed {data['elapsed_s']}s · event-time latency · refresh 2s</div>"
            f"<div class='flow'>{flow}</div>"
            f"<h2 style='font-size:14px'>Incidents ({len(data['incidents'])})</h2>{cards_html}"
            f"<h2 style='font-size:14px'>Recent trail</h2>"
            f"<table class='ev'><thead><tr><td>topic</td><td>ts(ms)</td></tr></thead>{rows}</table>"
            f"</body></html>").encode("utf-8")
    admin.send_response(200)
    admin.send_header("Content-Type", "text/html; charset=utf-8")
    admin.send_header("Content-Length", str(len(body)))
    admin.end_headers()
    admin.wfile.write(body)


def _read_spa() -> bytes:
    """Read ui/app.html once if present (SPA), else None -> fall back to _render."""
    p = Path(__file__).with_name("app.html")
    if not p.exists():
        return b""
    return p.read_bytes()


_SPA = _read_spa()


def serve(dash: Dashboard, host: str, port: int) -> ThreadingHTTPServer:
    """Serve the dashboard snapshot over plain stdlib HTTP; call with a thread.
    ``/?json=1`` or ``/api/snapshot`` returns the raw JSON snapshot. With ui/app.html
    present the root serves the single-file SPA; otherwise the legacy HTML view.

    Track C write endpoints (AD-5): GET answers, POST mutates the coordination layer.
    The dashboard is the SOLE publisher of request/in + approval/<task_id>."""
    def _do_get(self):
        path = self.path.split("?", 1)[0]
        if "json=1" in self.path or path.startswith("/api/"):
            _render(self, dash.snapshot(), as_json=True)
        elif _SPA and path in ("/", ""):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(_SPA)))
            self.end_headers()
            self.wfile.write(_SPA)
        else:
            _render(self, dash.snapshot(), as_json=False)

    def _do_post(self):
        import json as _json

        path = self.path.split("?", 1)[0]
        ln = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(ln)
        try:
            data = _json.loads(raw.decode("utf-8") or "{}") if isinstance(raw, bytes) and raw else {}
        except Exception:
            data = {}
        if path == "/api/request":
            r = dash.publish_request(str(data.get("text") or ""),
                                     data.get("playbook_id"),
                                     data.get("priority"))
        elif path == "/api/decision":
            r = dash.publish_decision(str(data.get("task_id") or ""),
                                      str(data.get("approval_id") or ""),
                                      str(data.get("decision") or ""))
        elif path == "/api/snapshot":
            r = {"published": True}
        else:
            r = {"published": False, "reason": "unknown endpoint"}
            body = _json.dumps(r, ensure_ascii=False).encode("utf-8")
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = _json.dumps(r, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    server = ThreadingHTTPServer(
        (host, port),
        type("H", (BaseHTTPRequestHandler,), {
            "do_GET": _do_get,
            "do_POST": _do_post,
            "log_message": lambda *a, **k: None,
        }),
    )
    return server


def free_port(start: int = 8765) -> int:
    for port in range(start, start + 50):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    return start