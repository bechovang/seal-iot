"""Subscribe-only demo dashboard (Epic 5.2 / AD-14).

A zero-build, static/serve-only consumer: it subscribes to ``ops/*`` and ``cmd/*``
and renders a per-incident / per-stage stopwatch plus the recent event trail. It is
purely a consumer — with the dashboard process killed the loop keeps running. Latency
is derived from the event timestamp column (single clock), not from wall clock, so a
natural HAI attack expiry is never mistaken for improvement (5.6).

Uses only the standard library.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

STAGES = ["perceive", "diagnose", "decide", "act", "verify", "incident"]

# match ephemeral 'incident_id' inside event payloads when present
def _lookup_id(payload: dict) -> str | None:
    for k in ("incident_id", "episode_key", "episode_key"):
        if k in payload:
            return str(payload[k])
    return None


def iso_ms(ts: str) -> int | None:
    """Parse '1970-01-01T00:00:05Z' -> ms since epoch (event-time clock)."""
    if not ts:
        return None
    t = ts.replace("Z", "+00:00")
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(t)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


class Dashboard:
    """Collects events and answers a per-incident snapshot (thread-safe)."""

    def __init__(self, max_events: int = 200) -> None:
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []
        self.max_events = max_events
        self.start_ts = time.time()  # UI-only wall clock for 'elapsed' display

    def handler(self, topic: str, payload: dict) -> None:
        with self._lock:
            rec = {"topic": topic, "payload": payload}
            self._events.append(rec)
            if len(self._events) > self.max_events:
                self._events = self._events[-self.max_events:]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            events = list(self._events)
        incidents: dict[str, dict] = {}
        stages: dict[str, int] = {}
        for rec in events:
            topic = rec["topic"]
            payload = rec["payload"]
            stage = topic.split("/")[-1]
            # normalise a leaf like cmd/fc_controller to 'act'
            if topic.startswith("cmd/"):
                stage = "act"
            stages[stage] = stages.get(stage, 0) + 1
            iid = _lookup_id(payload)
            if iid is None:
                continue
            d = incidents.setdefault(
                iid, {"id": iid, "stages": {}, "order": [], "last_ts": None}
            )
            now = iso_ms(payload.get("ts") or payload.get("ts_ms"))
            d["stages"][stage] = rec
            if stage not in d["order"]:
                d["order"].append(stage)
            if now is not None and (
                d["last_ts"] is None or d["last_ts"] < now
            ):
                d["last_ts"] = now
        # per-stage latency (ms) by event-timestamp gaps within each incident
        for d in incidents.values():
            order = d["order"]
            lat = {}
            prev = None
            prev_stage = None
            for st in STAGES:
                if st in d["stages"]:
                    ts = iso_ms(d["stages"][st]["payload"].get("ts")
                                or d["stages"][st]["payload"].get("ts_ms"))
                    if prev is not None and ts is not None:
                        lat[f"{prev_stage}->{st}"] = max(0, ts - prev)
                    prev, prev_stage = ts, st
            d["latency_ms"] = lat
        return {
            "elapsed_s": round(time.time() - self.start_ts, 2),
            "stage_counts": stages,
            "incidents": list(incidents.values()),
            "recent": events[-40:],
        }


def _render(admin: BaseHTTPRequestHandler, data: dict) -> None:
    j = json.dumps(data, ensure_ascii=False, indent=1)
    body = ("<html><head><meta charset='utf-8'><title>SEAL Loop</title>"
            "<meta http-equiv='refresh' content='2'>"
            "<style>body{font:14px monospace;padding:12px}pre{white-space:pre-wrap;}"
            "</style></head><body><h3>SEAL Smart Ops Harness — live</h3>"
            f"<pre>{j}</pre></body></html>").encode("utf-8")
    admin.send_response(200)
    admin.send_header("Content-Type", "text/html; charset=utf-8")
    admin.send_header("Content-Length", str(len(body)))
    admin.end_headers()
    admin.wfile.write(body)


def serve(dash: Dashboard, host: str, port: int) -> ThreadingHTTPServer:
    """Serve the dashboard snapshot over plain stdlib HTTP; call with a thread."""
    server = ThreadingHTTPServer(
        (host, port),
        type("H", (BaseHTTPRequestHandler,), {
            "do_GET": lambda self: _render(self, dash.snapshot()),
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