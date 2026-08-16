"""Epics 1-4: task store/FSM, supervisor playbook routing + agents, Tool port + CMMS,
and the approval gate. Runs entirely on the InMemoryBus with no network / broker."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from bus import InMemoryBus
from bus.envelopes import TASK_SCHEMA_VERSION
from config import load_mapping, HarnessConfig


def build_fabric(db=":memory:", cmms_db=":memory:", keys_db=":memory:", seed_ticks: int = 8):
    from history import HistoryBuffer
    from orchestration import TaskStore, Supervisor
    from tools import CMMSSim, ToolPort
    from adapters import TrackCSim, parse_payload

    h = HarnessConfig.load()
    reg = load_mapping().trackc
    bus = InMemoryBus()
    store = TaskStore(db)
    cmms = CMMSSim(cmms_db, production_seed=[
        {"order_id": "ORD-1001", "priority": "URGENT", "line": "line_01",
         "due": "2026-08-16T18:00:00Z", "status": "active"}])
    port = ToolPort(cmms, reg, lambda tid: store.get(tid).state if tid else None,
                    db_path=keys_db, bus=bus)
    hist = HistoryBuffer(":memory:")
    sim = TrackCSim(reg)
    for k in range(seed_ticks):
        for e in parse_payload(sim.tick(k), reg).envelopes:
            ts_ms = int(datetime.fromisoformat(e.ts.replace("Z", "+00:00")).timestamp() * 1000)
            hist.write(e.signal_id, ts_ms, e.value, e.quality)
    sup = Supervisor(store, reg, h.trackc, bus=bus, port=port, history=hist,
                     cmms=cmms, mode="live")
    return reg, bus, store, cmms, port, hist, sup


# ── Epic 1: task store + FSM ────────────────────────────────────────────────
def test_task_store_mints_and_resumes():
    from orchestration import TaskStore

    s = TaskStore(":memory:")
    t = s.mint("prepare_inspection", "inspect motor", "human", None, "ROUTINE")
    assert t.task_id.startswith("t-") and len(t.task_id) == 10  # t- + 8 hex
    assert t.state == "RECEIVED"
    s.transition(t.task_id, "start_planning")
    assert s.get(t.task_id).state == "PLANNING"
    assert s.resume_unfinished()[0].state == "PLANNING"
    s.close()


def test_spawn_live_pair_idempotent_raises_priority():
    from orchestration import TaskStore

    s = TaskStore(":memory:")
    first = s.mint("prepare_inspection", "auto", "auto", "incident-a", "ROUTINE")
    with pytest.raises(Exception):
        s.mint("prepare_inspection", "dup", "auto", "incident-a", "ROUTINE")
    assert s.raise_priority_if_lower(first.task_id, "SAFETY") is True
    assert s.get(first.task_id).priority == "SAFETY"
    assert s.raise_priority_if_lower(first.task_id, "ROUTINE") is False
    s.close()


def test_transition_invalid_raises():
    from orchestration import TaskStore

    s = TaskStore(":memory:")
    t = s.mint("generic", "x", "human")
    with pytest.raises(ValueError):
        s.transition(t.task_id, "report")  # RECEIVED cannot report
    s.close()


def test_ttl_expiry_cancels_awaiting_approval():
    from orchestration import TaskStore

    s = TaskStore(":memory:", ttl_seconds=30)
    t = s.mint("generic", "x", "human")
    s.transition(t.task_id, "start_planning")
    s.transition(t.task_id, "plan_done")
    s.transition(t.task_id, "request_approval")
    assert s.get(t.task_id).state == "AWAITING_APPROVAL"
    s._conn.execute("UPDATE tasks SET updated=? WHERE task_id=?", (0.0, t.task_id))
    s._conn.commit()
    s.resume_unfinished()
    assert s.get(t.task_id).state == "CANCELLED"
    s.close()


def test_terminal_property():
    from orchestration import TaskStore

    s = TaskStore(":memory:")
    t = s.mint("generic", "x", "human")
    assert not t.terminal  # fresh task is not terminal
    s.transition(t.task_id, "cancel")
    assert s.get(t.task_id).terminal is True
    s.close()


# ── Epic 2: request-driven routing (judged, auto-loop dead) ────────────────
def test_ingest_request_reaches_awaiting_approval():
    _, _, store, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection of motor 1", "prepare_inspection",
                             priority="URGENT")
    tid = res["task_id"]
    r = sup.advance(tid)
    assert r["state"] == "AWAITING_APPROVAL"
    t = store.get(tid)
    assert t.approval_request_id.startswith("apr_")
    assert t.state == "AWAITING_APPROVAL"


def test_approval_granted_continues_to_reported():
    _, bus, store, cmms, port, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    t = store.get(tid)
    apr = t.approval_request_id

    # approval requested event published with options
    evs = [p for topic, p in bus.messages if topic.endswith("/approval_requested")]
    assert evs, "approval_requested must be published"
    # first approval is the adjudicate stage -> human conflict decision options
    assert evs[0]["payload"]["stage"] == "adjudicate"
    assert evs[0]["payload"].get("options") == ["approve_plan", "revise", "cancel"]

    r = sup.handle_approval(tid, {"approval_id": apr, "decision": "APPROVED"})
    assert r["accepted"] is True
    # second approval stage (act) now waiting
    t2 = store.get(tid)
    assert t2.state == "AWAITING_APPROVAL"
    assert t2.approval_request_id != apr
    sup.handle_approval(tid, {"approval_id": t2.approval_request_id,
                              "decision": "APPROVED"})
    assert store.get(tid).state == "REPORTED"
    # artifacts created through the port
    assert len(cmms.lookup("work_orders")) >= 1
    assert len(cmms.lookup("reports")) >= 1


def test_approval_denied_ends_partial():
    _, _, store, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    apr = store.get(tid).approval_request_id
    r = sup.handle_approval(tid, {"approval_id": apr, "decision": "DENIED"})
    assert r["accepted"] is True
    assert store.get(tid).state == "PARTIAL"


def test_stale_bad_approval_id_rejected():
    _, _, store, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    r = sup.handle_approval(tid, {"approval_id": "apr_0000", "decision": "APPROVED"})
    assert r["accepted"] is False
    assert store.get(tid).state == "AWAITING_APPROVAL"


def test_incorrect_decision_rejected():
    _, _, store, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    apr = store.get(tid).approval_request_id
    r = sup.handle_approval(tid, {"approval_id": apr, "decision": "MAYBE"})
    assert r["accepted"] is False


def test_spawn_from_incident_idempotent_and_raises():
    _, _, store, _, _, _, sup = build_fabric()
    r1 = sup.spawn_from_incident("incident-9", "critical", "prepare_inspection")
    assert r1["minted"] is True and r1["priority"] == "SAFETY"
    # a later lower-severity re-spawn must NOT mint a duplicate, nor lower the priority
    r2 = sup.spawn_from_incident("incident-9", "low", "prepare_inspection")
    assert r2["minted"] is False and r2["priority_raised"] is False
    assert store.get(r1["task_id"]).priority == "SAFETY"


def test_unknown_severity_rejected():
    _, _, _, _, _, _, sup = build_fabric()
    with pytest.raises(ValueError):
        sup.spawn_from_incident("incident-1", "catastrophic", "prepare_inspection")


def test_only_supervisor_publishes_task_star():
    """All task/* bus frames are supervisor-shaped TaskEvent envelopes."""
    _, bus, _, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    task_msgs = [(t, p) for t, p in bus.messages if t.startswith("task/")]
    assert task_msgs
    for _t, body in task_msgs:
        assert body.get("schema_version") == TASK_SCHEMA_VERSION
        assert body.get("task_id") == tid
        assert body.get("event") in (
            "opened", "handoff", "stage_done", "approval_requested", "closed")


def test_replan_within_cap_then_partial():
    _, _, store, _, _, _, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    # simulate an act-stage failure: replan within cap, then exceed
    for k in range(4):
        r = sup.replan_or_partial(tid, "act", "motor_deviation detected")
        if not r.get("replanned"):
            assert r["state"] == "PARTIAL"
            break
    assert store.get(tid).state in ("PLANNING", "PARTIAL")


# ── Epic 3: Tool port + CMMS ────────────────────────────────────────────────
def _task_state_fixture():
    reg, _, store, cmms, port, _, _ = build_fabric()
    t = store.mint("generic", "wo", "human", None, "ROUTINE")
    # allow act: park in COORDINATING
    store.transition(t.task_id, "start_planning")
    store.transition(t.task_id, "plan_done")
    return reg, store, port, t


DEVICE = "motor_01"


def _evidence():
    return {"device": DEVICE, "signal": "motor_01_vibration", "value": 6.5,
            "event_time": "2026-08-16T10:00:00Z", "age": 3.0, "staleness": "critical"}


def test_port_create_work_order_readback():
    _, store, port, t = _task_state_fixture()
    r = port.create("work_order", {
        "task_id": t.task_id, "device_id": DEVICE, "summary": "rebalance motor",
        "priority": "URGENT", "idempotency_key": f"{t.task_id}:wo", "evidence": _evidence()},
        task_id=t.task_id)
    assert r.ok is True
    # backend_id returned and readback matches
    art = port._read_back("work_order", r.backend_id)
    assert art["device_id"] == DEVICE
    assert art["status"].upper() == "OPEN"


def test_port_rejects_unknown_device():
    _, store, port, t = _task_state_fixture()
    r = port.create("work_order", {
        "task_id": t.task_id, "device_id": "ghost_99", "summary": "x",
        "priority": "URGENT", "idempotency_key": "k1", "evidence": _evidence()},
        task_id=t.task_id)
    assert r.ok is False and "not in registry" in r.error


def test_port_requires_evidence_block():
    _, store, port, t = _task_state_fixture()
    r = port.create("work_order", {
        "task_id": t.task_id, "device_id": DEVICE, "summary": "x",
        "priority": "URGENT", "idempotency_key": "k2", "evidence": {}},
        task_id=t.task_id)
    assert r.ok is False and "evidence" in r.error


def test_port_rejects_missing_fields():
    _, store, port, t = _task_state_fixture()
    r = port.create("work_order", {"device_id": DEVICE}, task_id=t.task_id)
    assert r.ok is False and "missing required" in r.error


def test_port_idempotency_reuse():
    _, store, port, t = _task_state_fixture()
    kw = {"device_id": DEVICE, "summary": "rebalance", "priority": "ROUTINE",
          "idempotency_key": "samekey", "evidence": _evidence()}
    r1 = port.create("work_order", {"task_id": t.task_id, **kw}, task_id=t.task_id)
    r2 = port.create("work_order", {"task_id": t.task_id, **kw}, task_id=t.task_id)
    assert r1.ok and r2.ok
    assert r2.reused is True and r1.backend_id == r2.backend_id


def test_port_restricts_act_to_approved_states():
    _, store, port, t = _task_state_fixture()
    # move the task into a terminal state -> the sole writer must refuse an act
    store.update_state(t.task_id, "VERIFYING")
    r = port.create("work_order", {
        "task_id": t.task_id, "device_id": DEVICE, "summary": "x", "priority": "ROUTINE",
        "idempotency_key": "k3", "evidence": _evidence()}, task_id=t.task_id)
    assert r.ok is False and "not allowed to act" in r.error


def test_approval_request_via_port_validate_and_approve():
    _, store, port, t = _task_state_fixture()
    r = port.create("approval_request", {
        "device_id": DEVICE, "action": "act", "options": ["proceed", "cancel"],
        "evidence": _evidence()}, task_id=t.task_id, priority="URGENT")
    assert r.ok is True
    up = port.update_approval_status(r.backend_id, "APPROVED", "APPROVED")
    assert up["status"] == "APPROVED"


def test_port_timeout_list_by_key_reuse():
    """AD-6: after a suspected timeout, list-by-key finds the already-created artifact
    so a retry reuses it instead of duplicating."""
    _, store, port, t = _task_state_fixture()
    kw = {"device_id": DEVICE, "summary": "rebalance", "priority": "ROUTINE",
          "idempotency_key": "tick-tock", "evidence": _evidence()}
    r1 = port.create("work_order", {"task_id": t.task_id, **kw}, task_id=t.task_id)
    # simulate timeout: caller does NOT know backend_id, only the key
    found = port.list_by_key("tick-tock")
    assert found and found[0]["backend_id"] == r1.backend_id
    assert found[0]["artifact"]["device_id"] in ("motor_01", DEVICE)


# ── Epic 3 catch: Tool events are the only tool/* publisher ────────────────
def test_port_is_sole_tool_publisher():
    from bus.envelopes import TOOL_SCHEMA_VERSION
    _, bus, store, _, port, _, _ = build_fabric()
    t = store.mint("generic", "x", "human")
    store.transition(t.task_id, "start_planning")
    store.transition(t.task_id, "plan_done")
    # nothing published until a create happens THROUGH the port
    assert [t for t, _ in bus.messages if t.startswith("tool/")] == []
    r = port.create("work_order", {
        "task_id": t.task_id, "device_id": DEVICE, "summary": "rebalance",
        "priority": "ROUTINE", "idempotency_key": "sole", "evidence": _evidence()},
        task_id=t.task_id)
    assert r.ok is True
    tool_msgs = [(t, p) for t, p in bus.messages if t.startswith("tool/")]
    assert tool_msgs, "port must publish tool/* on create"
    for _t, body in tool_msgs:
        assert body.get("schema_version") == TOOL_SCHEMA_VERSION
        assert body.get("tool") == "work_order"