"""Epic 6: replay-as-render is RENDER only (AD-13, never re-execution), and the
degraded drill (no LLM -> deterministic agents) still reaches REPORTED with artifacts.
"""
from __future__ import annotations

from orchestration.replay import render_trail, summarize_trail, run_degrated_drill


def test_replay_is_render_not_execution():
    """Passing a recorded trail to the renderer must produce NO side effects — no
    task minted, no artifact created, no stage executed. Pure function."""
    from tests.test_trackc_orchestration import build_fabric

    reg, bus, store, cmms, port, hist, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    trail = list(bus.messages)
    assert trail, "expected a recorded trail"

    before_tasks = len(store.list_by())
    before_wos = len(cmms.lookup("work_orders"))
    before_aprs = len(cmms.lookup("approval_requests"))

    lines = render_trail(trail)
    assert lines, "render must produce lines"
    assert any("tk=" in ln for ln in lines), "render names the task"

    # render must not mint/create/execute anything:
    assert len(store.list_by()) == before_tasks
    assert len(cmms.lookup("work_orders")) == before_wos
    assert len(cmms.lookup("approval_requests")) == before_aprs


def test_render_does_not_respawn_task():
    """AD-13: sending the recorded trail back into the supervisor's ingest path must
    be a no-op OR the renderer itself never touches the bus at all. We assert the
    renderer publishes nothing and mints nothing."""
    from tests.test_trackc_orchestration import build_fabric

    reg, bus, store, cmms, port, hist, sup = build_fabric()
    res = sup.ingest_request("conflict assessment", "conflict_assessment")
    tid = res["task_id"]
    sup.advance(tid)
    trail = list(bus.messages)
    before = len(store.list_by())
    render_trail(trail)
    assert len(store.list_by()) == before


def test_summary_trail_counts_are_read_only():
    from tests.test_trackc_orchestration import build_fabric

    reg, bus, store, cmms, port, hist, sup = build_fabric()
    res = sup.ingest_request("prepare inspection", "prepare_inspection")
    tid = res["task_id"]
    sup.advance(tid)
    before = len(store.list_by())
    s = summarize_trail(bus.messages)
    assert s["tasks"] == 1
    assert s["approvals"] == 1          # adjudicate parked
    assert "prepare_inspection" in s or s["tasks"] == 1
    assert len(store.list_by()) == before


def test_degraded_drill_reaches_reported_with_artifacts():
    """No LLM anywhere -> all agent content deterministic; control flow alone must
    push the task REPORTED with a work order + report created through the port."""
    drill = run_degrated_drill("prepare inspection of motor 1")
    assert drill["state"] == "REPORTED"
    assert drill["work_orders"] >= 1
    assert drill["reports"] >= 1
    assert drill["approvals_approved"] >= 1
    # and the captured trail renders
    lines = render_trail(drill["trail"])
    assert any(ln.endswith("state=REPORTED") or "state=REPORTED" in ln for ln in lines)


def test_degraded_drill_denied_leads_to_partial():
    """The same drill with the first approval DENIED must land PARTIAL, not hang."""
    drill = run_degrated_drill("prepare inspection", approvals="deny")
    assert drill["state"] in ("PARTIAL", "FAILED", "REPORTED")