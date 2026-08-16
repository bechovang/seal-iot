"""Epic 4: the dashboard is the SOLE publisher of request/in + approval/<task_id>,
and the request-driven (judged) path runs end-to-end through the InMemoryBus."""
from __future__ import annotations

import json
import threading

from tests.test_trackc_orchestration import build_fabric
from bus.envelopes import request_topic, approval_topic
from ui.dashboard import Dashboard, serve, free_port


def make_dash():
    reg, bus, store, cmms, port, hist, sup = build_fabric()
    sup.subscribe(bus)
    dash = Dashboard()
    dash.set_publisher(bus)   # sole publisher
    bus.subscribe("", dash.handler)   # feed the view-model the same bus (FE/autopilot)
    return reg, bus, store, cmms, port, hist, sup, dash


def test_dashboard_is_sole_publisher_of_request_family():
    _, bus, store, _, _, _, _, dash = make_dash()
    dash.publish_request("prepare inspection of motor 1", "prepare_inspection", "URGENT")
    reqs = [p for t, p in bus.messages if t == request_topic()]
    assert len(reqs) == 1
    assert reqs[0]["playbook_id"] == "prepare_inspection"
    assert reqs[0]["priority"] == "URGENT"


def test_dashboard_publishes_decision_on_approval_family():
    _, bus, store, _, _, _, _, dash = make_dash()
    dash.publish_decision("t-abc", "apr_123", "APPROVED")
    msgs = [(t, p) for t, p in bus.messages if t == approval_topic("t-abc")]
    assert len(msgs) == 1
    assert msgs[0][1]["approval_id"] == "apr_123"
    assert msgs[0][1]["decision"] == "APPROVED"


def test_dashboard_rejects_invalid_decision():
    _, bus, store, _, _, _, _, dash = make_dash()
    r = dash.publish_decision("t-abc", "apr_1", "MAYBE")
    assert r["published"] is False
    assert [p for t, p in bus.messages if t.startswith("approval/")] == []


def _poll_state(store, tid, target, timeout=5.0):
    """Approvals are handled synchronously, but the request drive runs in a background
    thread -> poll FSM state instead of asserting immediately (fast/robust)."""
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        s = store.get(tid).state
        if s == target:
            return store.get(tid)
        time.sleep(0.05)
    raise AssertionError(f"task {tid} not in {target} (final {store.get(tid).state})")


def test_request_driven_path_end_to_end_runs_unchanged():
    """The judged path: dashboard request -> supervisor mints -> approvals -> REPORTED,
    with artifacts (work order + report) created via the Tool port."""
    import time as _t

    _, bus, store, cmms, port, hist, sup, dash = make_dash()
    dash.publish_request("prepare inspection", "prepare_inspection", "URGENT")
    tasks = store.list_by()
    assert len(tasks) == 1   # exactly one task minted (idempotent)
    tid = tasks[0].task_id

    # gate until first approval (adjudicate) — request is auto-driven in background
    t = _poll_state(store, tid, "AWAITING_APPROVAL")
    apr = t.approval_request_id

    # operator approves the adjudicate stage over the approval/<task_id> family
    dash.publish_decision(tid, apr, "APPROVED")   # supervisor auto-continues
    t2 = _poll_state(store, tid, "AWAITING_APPROVAL")
    assert t2.approval_request_id != apr, "must move to a second approval (act)"

    dash.publish_decision(tid, t2.approval_request_id, "APPROVED")
    assert store.get(tid).state == "REPORTED"
    assert len(cmms.lookup("work_orders")) >= 1
    assert len(cmms.lookup("reports")) >= 1


def test_http_endpoints_round_trip():
    """POST /api/request + /api/decision over real HTTP hit the bus; GET snapshot
    exposes the aggregated task card."""
    import http.client

    dash = Dashboard()
    # wire publisher + a supervisor so the loop could run; here we only assert the
    # request lands on the wire (sole publisher) and the snapshot shapes up.
    _, bus, store, _, _, _, sup, _ = make_dash()
    dash.set_publisher(bus)

    port = free_port()
    server = serve(dash, "127.0.0.1", port)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:

        def post(path, obj):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            conn.request("POST", path, json.dumps(obj),
                         {"Content-Type": "application/json"})
            resp = conn.getresponse()
            body = json.loads(resp.read().decode("utf-8"))
            conn.close()
            return resp.status, body

        st, body = post("/api/request", {"text": "prepare inspection", "playbook_id": "prepare_inspection"})
        assert st == 200 and body["published"] is True
        assert store.list_by(), "supervisor must have consumed the request"

        st, body = post("/api/decision", {"task_id": "t-0000", "approval_id": "apr_0", "decision": "MAYBE"})
        assert st == 200 and body["published"] is False

        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        conn.request("GET", "/api/snapshot")
        resp = conn.getresponse()
        snap = json.loads(resp.read().decode("utf-8"))
        conn.close()
        assert "tasks" in snap
    finally:
        server.shutdown()
        th.join(timeout=3)


def test_snapshot_exposes_tools_bridge_devices_policy():
    """FE-1..FE-6 nguồn: snapshot trả tools / bridge / auto_incidents / devices /
    policy / thresholds / clocks từ các frame đã publish qua bus."""
    from tests.test_trackc_orchestration import build_fabric

    reg, bus, store, cmms, port, hist, sup = build_fabric(seed_ticks=0)
    dash = Dashboard()
    dash.set_publisher(bus)
    bus.subscribe("", dash.handler)

    # tele với quality offline -> device grid báo offline
    dash.handler("tele/motor_01_vib",
                 {"signal_id": "motor_01_vib", "ts": "2026-08-16T10:00:00Z",
                  "value": 12.5, "quality": "offline"})
    # tool frame (AD-6)
    dash.handler("tool/work_order/result",
                 {"tool": "work_order", "event": "result", "payload": {"id": "WO-1", "reused": False}})
    # bridge heartbeat
    dash.handler("bridge/heartbeat", {"connected": True, "ts": "2026-08-16T10:00:00Z"})
    # policy
    dash.set_policy("auto", 4.0)

    s = dash.snapshot()
    assert s["tools"]["work_order"]["id"] == "WO-1"
    assert s["tools"]["work_order"]["last"] == "result"
    assert s["bridge"]["connected"] is True
    dev = [d for d in s["devices"] if d["device"] == "motor_01"]
    assert dev and dev[0]["worst"] == "offline"
    assert s["policy"]["mode"] == "auto" and s["policy"]["delay_s"] == 4.0
    assert s["thresholds"]["approve_ttl_seconds"] == 300.0
    assert isinstance(s["clocks"]["epoch_lag_s"], (int, float))
    assert s["clocks"]["tele_ts_ms"] is not None


def test_bridge_heartbeat_publishes_on_disconnect():
    """TrackCBridge._on_disconnect -> bus có bridge/heartbeat connected=False."""
    from bus import InMemoryBus
    from config import load_mapping
    from adapters import TrackCBridge, settings_from_env

    bus = InMemoryBus()
    reg = load_mapping().trackc
    # dựng bridge không cần client thật: chỉ cần start(bus) rồi gọi callback ngoại lệ
    br = TrackCBridge(reg, {"env": "test", "tls": False})
    br.start(bus)
    br._emit_heartbeat(True)
    assert any(t == "bridge/heartbeat" for t, _ in bus.messages)
    bus.messages.clear()
    br._on_disconnect(None, None, None, 1, None)
    frame = [p for t, p in bus.messages if t == "bridge/heartbeat"]
    assert frame and frame[0]["connected"] is False


def test_spa_has_no_operator_controls():
    """View-only guard: SPA không chứa button / POST cià/request / decision."""
    from pathlib import Path

    html = Path("ui/app.html").read_text(encoding="utf-8")
    assert "<button" not in html.lower()
    assert "api/decision" not in html
    assert "api/request" not in html
    assert "method:" not in html