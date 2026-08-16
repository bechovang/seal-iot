"""Epic 4: the dashboard is the SOLE publisher of request/in + approval/<task_id>,
and the request-driven (judged) path runs end-to-end through the InMemoryBus."""
from __future__ import annotations

import json
import threading

from test_trackc_orchestration import build_fabric
from bus.envelopes import request_topic, approval_topic
from ui.dashboard import Dashboard, serve, free_port


def make_dash():
    reg, bus, store, cmms, port, hist, sup = build_fabric()
    sup.subscribe(bus)
    dash = Dashboard()
    dash.set_publisher(bus)   # sole publisher
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


def test_request_driven_path_end_to_end_runs_unchanged():
    """The judged path: dashboard request -> supervisor mints -> approvals -> REPORTED,
    with artifacts (work order + report) created via the Tool port."""
    _, bus, store, cmms, port, hist, sup, dash = make_dash()
    dash.publish_request("prepare inspection", "prepare_inspection", "URGENT")
    tasks = store.list_by()
    assert len(tasks) == 1   # exactly one task minted (idempotent)
    tid = tasks[0].task_id
    assert tasks[0].state == "RECEIVED"

    # gate until first approval (adjudicate)
    sup.advance(tid)
    t = store.get(tid)
    assert t.state == "AWAITING_APPROVAL"
    apr = t.approval_request_id

    # operator approves the adjudicate stage over the approval/<task_id> family
    dash.publish_decision(tid, apr, "APPROVED")   # supervisor auto-continues
    t2 = store.get(tid)
    assert t2.state == "AWAITING_APPROVAL"
    assert t2.approval_request_id != apr

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