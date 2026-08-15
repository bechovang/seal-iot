"""Subscribe-only demo dashboard (Epic 5.2 / AD-14).

A zero-build, static/serve-only consumer: it subscribes to ``ops/*`` and ``cmd/*``
and renders a per-incident / per-stage stopwatch plus the recent event trail. It is
purely a consumer — with the dashboard process killed the loop keeps running. Latency
is derived from the event timestamp column (single clock), not from wall clock, so a
natural HAI attack expiry is never mistaken for improvement (5.6).

Uses only the standard library.
"""

from __future__ import annotations

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

    Additive fields (state, last_stage, pipeline_counts) are exposed in :meth:`snapshot`
    without changing the stable keys the tests rely on.
    """

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
                iid, {"id": iid, "stages": {}, "order": [], "last_ts": None,
                       "state": None, "state_ts": None}
            )
            now = iso_ms(payload.get("ts") or payload.get("ts_ms"))
            d["stages"][stage] = rec
            if stage not in d["order"]:
                d["order"].append(stage)
            if now is not None and (d["last_ts"] is None or d["last_ts"] < now):
                d["last_ts"] = now
            # capture the latest FSM state (e.g. RESOLVED / ACTING) if present
            state = payload.get("state") or (payload.get("outcome") or {}).get("classification")
            if state is not None and (d["state_ts"] is None or now is None or d["state_ts"] <= (now or 0)):
                d["state"] = str(state)
                d["state_ts"] = now
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
            "pipeline_counts": {s: stages.get(s, 0) for s in STAGES},
        }


def _esc(v: Any) -> str:
    s = "" if v is None else f"{v}"
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def _stage_color(st: str) -> str:
    return {
        "perceive": "#4ea1ff", "diagnose": "#b388ff", "decide": "#ffb04d",
        "act": "#ff7a59", "verify": "#37d67a", "incident": "#ff5c8a",
        "cmd": "#ff7a59",
    }.get(st, "#8c9baf")


def _flag(v: Any) -> str:
    return "✓" if v is True else ("✗" if v is False else _esc(v))


def _render(admin: BaseHTTPRequestHandler, data: dict, as_json: bool = False) -> None:
    import json

    if as_json:
        j = json.dumps(data, ensure_ascii=False, indent=1)
        body = j.encode("utf-8")
        ctype = "application/json; charset=utf-8"
        admin.send_response(200)
        admin.send_header("Content-Type", ctype)
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


def serve(dash: Dashboard, host: str, port: int) -> ThreadingHTTPServer:
    """Serve the dashboard snapshot over plain stdlib HTTP; call with a thread.
    ``/?json=1`` returns the raw JSON snapshot instead of the HTML view."""
    def _do_get(self):
        _render(self, dash.snapshot(), as_json="json" in self.path)
    server = ThreadingHTTPServer(
        (host, port),
        type("H", (BaseHTTPRequestHandler,), {
            "do_GET": _do_get,
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