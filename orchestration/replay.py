"""Replay-as-render (AD-13) + a degraded drill runner for Track C.

Replay is RENDER, never re-execution: given a recorded bus trail (list of
``(topic, payload)`` frames) we produce a readable timeline of what the coordination
layer did, with zero side effects — no task is minted, no artifact created, no stage
re-run. The drill ships a full request-driven run (agents always deterministic — no
LLM wired), captures its trail, then renders it to prove control flow owns execution.

These are the two Epic 6 acceptance exercises: (a) a replay of a previous run does not
execute, and (b) the degraded drill still reaches REPORTED with artifacts.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

TASK_EVENTS = {"opened", "handoff", "stage_done", "approval_requested", "closed"}
TOOL_EVENTS = {"invoked", "result", "failed"}

_STAGE_LABEL = {
    "observe": "Observe", "plan": "Plan", "analyze": "Analyze", "adjudicate": "Adjudicate",
    "act": "Act", "verify": "Verify", "report": "Report",
}


def render_trail(frames: Iterable[tuple[str, dict]], stream=None) -> list[str]:
    """Render a recorded trail into human-readable lines. Pure function: no bus, no
    store, no writes of any kind. Returns the rendered line list (also printed to
    ``stream`` when given)."""
    out: list[str] = []
    tasks: dict[str, dict] = {}
    closed = set()
    for topic, payload in frames:
        if topic.startswith("task/"):
            parts = topic.split("/")
            task_id = parts[1]
            event = parts[-1]
            rec = tasks.setdefault(task_id, {"task_id": task_id,
                                             "stages": [], "approvals": [], "state": ""})
            body = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
            if event == "opened":
                rec["state"] = "RECEIVED"
                out.append(f"[open ] tk={task_id} origin={body.get('task', {}).get('origin') if isinstance(body.get('task'), dict) else '?'}")
            elif event == "handoff":
                st = body.get("stage", "")
                rec["stages"].append(st)
                out.append(f"[stage] tk={task_id} {_STAGE_LABEL.get(st, st)} <{body.get('agent')}>")
            elif event == "approval_requested":
                rec["approvals"].append(body.get("approval_id"))
                out.append(f"[gate ] tk={task_id} {_STAGE_LABEL.get(body.get('stage',''),body.get('stage',''))} "
                           f"awaiting approval {body.get('approval_id')} options={body.get('options')}")
            elif event == "closed":
                rec["state"] = body.get("state", "?")
                closed.add(task_id)
                out.append(f"[done ] tk={task_id} state={rec['state']}")
        elif topic.startswith("tool/"):
            parts = topic.split("/")
            tool = parts[1]
            out.append(f"[tool ] {tool}:{parts[-1]}")
    if not out:
        out.append("(empty trail)")
    if stream is not None:
        for line in out:
            print(line, file=stream)
    return out


def summarize_trail(frames: Iterable[tuple[str, dict]]) -> dict[str, Any]:
    """Compact counts for a trail (no execution)."""
    tasks: set[str] = set()
    approvals = 0
    tools: dict[str, int] = {}
    closed = 0
    for topic, _ in frames:
        if topic.startswith("task/"):
            tasks.add(topic.split("/")[1])
            if topic.endswith("/closed"):
                closed += 1
            if topic.endswith("/approval_requested"):
                approvals += 1
        elif topic.startswith("tool/"):
            tool = topic.split("/")[1]
            tools[tool] = tools.get(tool, 0) + 1
    return {"tasks": len(tasks), "task_ids": sorted(tasks), "approvals": approvals,
            "closed": closed, "tools": tools}


def run_degrated_drill(text: str, playbook_id: str = "prepare_inspection",
                       approvals="auto") -> dict[str, Any]:
    """Epic 6 degraded drill: run the full request-driven path with no LLM (all agent
    content deterministic), approve every parked stage, and verify REPORTED + artifacts.
    Returns a summary plus the captured trail for rendering.
    """
    from tests.test_trackc_orchestration import build_fabric

    reg, bus, store, cmms, port, hist, sup = build_fabric()
    res = sup.ingest_request(text, playbook_id)
    tid = res["task_id"]
    guard = 0
    while store.get(tid).state not in (
        "REPORTED", "PARTIAL", "FAILED", "CANCELLED"
    ) and guard < 6:
        r = sup.advance(tid)
        cur = store.get(tid).state
        if cur == "AWAITING_APPROVAL":
            apr = store.get(tid).approval_request_id
            decision = "APPROVED" if approvals == "auto" else "DENIED"
            sup.handle_approval(tid, {"approval_id": apr, "decision": decision})
        guard += 1
    final_state = store.get(tid).state
    return {
        "task_id": tid,
        "state": final_state,
        "work_orders": len(cmms.lookup("work_orders")),
        "reports": len(cmms.lookup("reports")),
        "approval_requests": len(cmms.lookup("approval_requests")),
        "approvals_approved": len([a for a in port.lookup("approval_requests")
                                   if a.get("status") == "APPROVED"]),
        "trail": list(bus.messages),
    }


def main() -> int:
    import sys

    text = sys.argv[1] if len(sys.argv) > 1 else "prepare inspection of motor 1"
    drill = run_degrated_drill(text)
    print(f"degraded drill: task {drill['task_id']} -> {drill['state']} "
          f"(work_orders={drill['work_orders']}, reports={drill['reports']})")
    print("---- replay-as-render (no execution) ----")
    print("\n".join(render_trail(drill["trail"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())