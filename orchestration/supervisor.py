"""Supervisor (AD-1/AD-3/AD-5/AD-8/AD-12): consume ``request/in`` + ``approval/*``,
mint tasks, walk each task deterministically through its playbook, and be the SOLE
publisher of ``task/*``. Re-plan is a declared back-edge; beyond the cap -> PARTIAL.
The auto loop spawns via ``spawn_from_incident`` (idempotent, priority-raises only).
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable

from bus.envelopes import (task_topic, approval_topic, request_topic, TaskEvent, TASK_STAGES)
from .task_store import TaskStore, TERMINAL
from .playbooks import Playbook, get_playbook, SEVERITY_PRIORITY, agent_for_stage
from .agents.base import AgentContext, Observation
from .agents import ROLE_CLASSES

STAGE_FOR_AGENT = {
    "observer": "observe", "maintenance": "plan", "production": "adjudicate",
    "safety": "analyze", "action": "act", "supervisor": "report",
}


def _iso(epoch: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class Supervisor:
    def __init__(
        self,
        store: TaskStore,
        registry,
        runtime,
        bus=None,
        port=None,
        history=None,
        cmms=None,
        llm_factory: Callable[[str], Any] | None = None,
        runbook_store=None,
        mode: str = "live",
        replay=False,
    ) -> None:
        self.store = store
        self.registry = registry
        self.runtime = runtime
        self.bus = bus
        self.port = port
        self.history = history
        self.cmms = cmms
        self.runbook_store = runbook_store
        self.mode = mode
        self.replay = replay
        self.llm_factory = llm_factory or (lambda r: None)  # per-role LLMClient
        self._llms: dict[str, Any] = {}
        self._agents: dict[str, Any] = {}
        self._subscribed = False

    # -- bus subscriptions ----------------------------------------------
    def subscribe(self, bus) -> None:
        """Attach to ``request/in`` + ``approval/<task_id>``. The supervisor is the
        ONLY consumer of these families (AD-5). Prefixes on InMemoryBus match by
        startswith, so the topic families line up with the AD-5 namespaces."""
        self.bus = bus
        bus.subscribe("request/", self._on_request)
        bus.subscribe("approval/", self._on_approval)
        self._subscribed = True

    def _on_approval(self, topic: str, payload: dict) -> None:
        task_id = topic.rsplit("/", 1)[-1]
        self.handle_approval(task_id, payload)

    def _on_request(self, topic: str, payload: dict) -> None:
        text = payload.get("text") or payload.get("request_text") or ""
        pb = payload.get("playbook_id")
        priority = payload.get("priority")
        in_reply = payload.get("in_reply_to")
        if in_reply:
            self._handle_clarification_reply(in_reply, text)
            return
        if not text:
            return
        if pb and get_playbook(pb) is None:
            # illegal playbook id from a request -> guard to generic (AD-3)
            pb = "generic"
        pb = pb or self._match_playbook(text)
        self.ingest_request(text, pb, priority=priority)

    # -- task creation (AD-1) -------------------------------------------
    def _match_playbook(self, text: str) -> str:
        t = text.lower()
        if any(k in t for k in ("inspection", "prepare inspection")):
            return "prepare_inspection"
        if any(k in t for k in ("conflict", "urgent order", "production vs safety")):
            return "conflict_assessment"
        if any(k in t for k in ("timeout", "stale", "line down")):
            return "line_inspection_timeout"
        return "generic"

    def ingest_request(self, text: str, playbook_id: str = "generic",
                       source_incident_id: str | None = None,
                       priority: str = "ROUTINE", origin: str = "human") -> dict:
        pb = get_playbook(playbook_id)
        if pb is None:
            raise ValueError(f"unknown playbook {playbook_id!r}")
        if playbook_id == "generic":
            # request-driven generic: compose from a safe closed fallback sequence
            pb = get_playbook("generic")
        created = time.time()
        task = self.store.mint(playbook_id, text, origin=origin,
                               source_incident_id=source_incident_id,
                               priority=pb.priority or priority, created=created)
        self._task_event(task.task_id, "opened",
                         {"task": task.to_dict(), "playbook": playbook_id})
        return {"task_id": task.task_id, "state": task.state}

    def spawn_from_incident(self, incident_id: str, severity: str,
                            playbook_id: str = "prepare_inspection") -> dict:
        """Auto-loop door (AD-1): idempotent spawn; a re-spawn may only raise priority.
        Severity -> priority is a mandatory closed table, never silently defaulted."""
        if severity not in SEVERITY_PRIORITY:
            raise ValueError(f"incident severity {severity!r} has no mapped priority (AD-1)")
        priority = SEVERITY_PRIORITY[severity]
        live = self.store.live_pair(incident_id, playbook_id)
        if live is not None:
            raised = self.store.raise_priority_if_lower(live.task_id, priority)
            return {"task_id": live.task_id, "state": live.state, "priority_raised": raised,
                    "minted": False}
        task = self.store.mint(playbook_id, f"auto from incident {incident_id}",
                               origin="auto", source_incident_id=incident_id,
                               priority=priority)
        self._task_event(task.task_id, "opened",
                         {"auto": True, "source_incident_id": incident_id})
        return {"task_id": task.task_id, "state": task.state, "minted": True,
                "priority": task.priority}

    # -- stage execution (AD-3) -----------------------------------------
    def advance(self, task_id: str) -> dict:
        """Run the task forward from its cursor until: a wait (approval) is reached,
        a terminal state is reached, or a replan sends it back a stage."""
        t = self.store.get(task_id)
        if t is None:
            return {"state": "unknown", "task_id": task_id}
        if t.state in TERMINAL:
            return {"state": t.state, "task_id": task_id}
        if t.state == "RECEIVED":
            self.store.transition(task_id, "start_planning")
            t = self.store.get(task_id)
        elif t.state == "AWAITING_CLARIFICATION":
            self.store.transition(task_id, "replan")
            t = self.store.get(task_id)
        elif t.state == "AWAITING_APPROVAL":
            return {"state": "waiting", "task_id": task_id}

        pb = get_playbook(t.playbook_id)
        if pb is None:
            self.store.transition(task_id, "failed", fail_step="no_playbook")
            return {"state": "FAILED", "task_id": task_id}
        state = self._evidence_store(t)
        observations = [Observation.from_dict(o) for o in state.get("observations", [])]
        approved = set(state.get("approved", []))
        stage_idx = int(t.stage_cursor or "0")
        while stage_idx < len(pb.stages):
            t = self.store.get(task_id)
            stage = pb.stages[stage_idx]
            # planning phase (observe/plan/analyze) runs in PLANNING; once we reach the
            # coordination phase (adjudicate +), move COORDINATING (AD-2/AD-3). Replan
            # moves the cursor back under PLANNING and this re-arms naturally.
            if t.state == "PLANNING" and stage.name in ("adjudicate", "act", "verify", "report"):
                self.store.transition(task_id, "plan_done")
                t = self.store.get(task_id)
            if getattr(stage, "approval_marked", False) and stage.name not in approved:
                return self._request_approval(task_id, t, pb, stage, observations)
            out = self._execute_stage(t, pb, stage, observations)
            self.store.set_cursor(task_id, str(stage_idx + 1))
            if out.get("failed"):
                return self.replan_or_partial(task_id, stage.name,
                                              out.get("failed_step") or out.get("reason") or "")
            if stage.name == "observe":
                observations = [Observation.from_dict(o) for o in out.get("observations", [])]
                state["observations"] = [o.to_dict() for o in observations]
                self._save_evidence(task_id, state)
            if stage.name == "act":
                self._apply_action(task_id, t, out)
            stage_idx += 1
        if self.store.get(task_id).state == "REPORTED":
            return {"state": "REPORTED", "task_id": task_id}
        self.store.transition(task_id, "report")
        self._task_event(task_id, "closed", {"state": "REPORTED"})
        return {"state": "REPORTED", "task_id": task_id}

    def _execute_stage(self, task, pb: Playbook, stage, observations) -> dict:
        agent_name = stage.agent or agent_for_stage(stage.name)
        self._task_event(task.task_id, "handoff",
                         {"stage": stage.name, "agent": agent_name,
                          "devices": stage.devices, "task": task.to_dict()})
        if stage.name == "report":
            out = self._produce_report(task.task_id, task, observations)
            out["agent"] = "supervisor"
            out["stage"] = "report"
            self._task_event(task.task_id, "stage_done",
                             {"stage": stage.name, "agent": "supervisor", "output": out})
            return out
        agent = self._agent(agent_name)
        ctx = AgentContext(
            task=task, stage=stage, playbook=pb, registry=self.registry,
            runtime=self.runtime, history=self.history, observations=observations,
            previous=self._previous(task), cmms=self.cmms,
            runbook_store=self.runbook_store, mode=self.mode,
        )
        out = agent.run(ctx)
        out.setdefault("stage", stage.name)
        out.setdefault("agent", agent_name)
        self._task_event(task.task_id, "stage_done",
                         {"stage": stage.name, "agent": agent_name, "output": out})
        return out

    def _apply_action(self, task_id: str, task, out: dict) -> None:
        """After an approved act intent, the Tool port creates the informational
        artifacts (work order + record + notification). Never ``cmd/*`` (AD-12)."""
        if self.port is None:
            return
        if out.get("type") == "work_order":
            self.port.create("work_order", dict(out), task_id=task_id, stage_name="act",
                             agent="action", priority=getattr(task, "priority", "ROUTINE"))
        if getattr(task, "source_incident_id", None):
            self.port.create("incident_record", {
                "source_incident_id": task.source_incident_id, "note": out.get("summary", ""),
            }, task_id=task_id, stage_name="act", agent="action")
        self.port.create("notification", {
            "recipient": "maintenance_manager",
            "message": out.get("summary", "maintenance notification"),
        }, task_id=task_id, stage_name="act", agent="action")

    def _produce_report(self, task_id: str, task, observations) -> dict:
        """Supervisor-authored manager-condensed report (AD-12 / Reports): decision,
        options, evidence, owner, status + a link to the full trace."""
        evidence = [o.to_evidence() for o in observations]
        state = self._evidence_store(task)
        summary = (f"Task {task_id} ({task.playbook_id}, {task.priority}) completed. "
                   f"Condition: { (evidence[0].get('staleness') or 'ok') if evidence else 'ok' }")
        trace = f"task/{task_id}/handoff..closed"
        if self.port is not None:
            rep = self.port.create("report", {
                "summary": summary, "owner": "supervisor", "trace": trace,
                "evidence": evidence,
            }, task_id=task_id, stage_name="report", agent="supervisor", priority=getattr(task, "priority", "ROUTINE"))
            return {"report": rep.backend_id if rep.ok else "", "ok": rep.ok,
                    "summary": summary, "arrangements": state.get("approved", []),
                    "evidence": evidence}
        return {"report": "", "ok": True, "summary": summary, "evidence": evidence}

    def _previous(self, task) -> dict:
        return self._evidence_store(task)

    def _evidence_store(self, task) -> dict:
        try:
            data = json.loads(task.evidence_json or "{}")
            return data if isinstance(data, dict) else {"observations": data if isinstance(data, list) else []}
        except Exception:
            return {"observations": []}

    def _save_evidence(self, task_id: str, state: dict) -> None:
        self.store.set_evidence(task_id, json.dumps(state))

    def _agent(self, role: str):
        if role in self._agents:
            return self._agents[role]
        cls = ROLE_CLASSES.get(role)
        if cls is None:
            # unknown agent role -> deterministic passthrough so control flow survives
            class _Null:
                def run(self, ctx):
                    return {"degraded": True, "reason": f"no agent role {role}"}
            obj = _Null()
        else:
            llm = self.llm_factory(role)
            budget = self.runtime.llm_budgets.get(role, 3)
            if hasattr(llm, "budget"):
                llm.budget = budget
            obj = cls(llm_client=llm, registry=self.registry, runtime=self.runtime)
        self._agents[role] = obj
        return obj

    # -- approval (AD-8) -------------------------------------------------
    def _request_approval(self, task_id: str, task, pb: Playbook, stage, observations) -> dict:
        device = stage.devices[0] if stage.devices else ""
        evidence = [o.to_evidence() for o in observations]
        options = ["proceed", "cancel"]
        if stage.name == "adjudicate":
            options = ["approve_plan", "revise", "cancel"]
        self.store.transition(task_id, "request_approval")
        apr = self.port.create(
            "approval_request",
            {"device_id": device, "action": stage.name, "options": options,
             "evidence": evidence, "idempotency_key": f"{task_id}:{stage.name}:approval"},
            task_id=task_id, stage_name=stage.name, agent=stage.agent or "supervisor",
            priority=task.priority,
        )
        if not apr.ok or not apr.backend_id:
            self.store.transition(task_id, "failed", fail_step=f"approval_create:{apr.error}")
            self._task_event(task_id, "closed", {"state": "FAILED",
                                                 "fail": f"approval_create:{apr.error}"})
            return {"state": "FAILED", "task_id": task_id}
        self.store.set_approval(task_id, apr.backend_id)
        self._task_event(task_id, "approval_requested",
                         {"approval_id": apr.backend_id, "options": options,
                          "device": device, "stage": stage.name, "evidence": evidence})
        return {"state": "AWAITING_APPROVAL", "task_id": task_id}

    def handle_approval(self, task_id: str, payload: dict) -> dict:
        """Consume ``approval/<task_id>``: validate the pending apr_ id + the task is in
        AWAITING_APPROVAL, update the port record (read-back), transition, publish."""
        approval_id = payload.get("approval_id") or payload.get("approval_request_id")
        decision = str(payload.get("decision", "")).upper()
        t = self.store.get(task_id)
        if t is None:
            return {"accepted": False, "reason": "unknown task"}
        if t.state != "AWAITING_APPROVAL":
            return {"accepted": False, "reason": f"task not waiting (state {t.state})"}
        if approval_id != t.approval_request_id:
            return {"accepted": False, "reason": "approval_id mismatch"}
        if decision not in ("APPROVED", "DENIED"):
            return {"accepted": False, "reason": "decision must be APPROVED|DENIED"}
        rec = self.port.update_approval_status(approval_id, decision, decision)
        if rec is None or rec.get("status") != decision:
            return {"accepted": False, "reason": "port read-back verify failed"}
        if decision == "APPROVED":
            t = self.store.get(task_id)
            pb = get_playbook(t.playbook_id)
            st = self._evidence_store(t)
            stage_name = self._stage_name_at(t, pb)
            approved = set(st.get("approved", []))
            if stage_name:
                approved.add(stage_name)
            st["approved"] = sorted(approved)
            self._save_evidence(task_id, st)
            self.store.transition(task_id, "approve")
            self._task_event(task_id, "approval_granted", {"approval_id": approval_id})
            self.advance(task_id)
            return {"accepted": True, "decision": decision}
        else:
            self.store.transition(task_id, "deny", fail_step="approval_denied")
            self._task_event(task_id, "approval_denied", {"approval_id": approval_id})
            self._task_event(task_id, "closed", {"state": "PARTIAL",
                                                 "fail": "approval_denied"})
            return {"accepted": True, "decision": decision}

    def request_clarification(self, task_id: str, question: str) -> dict:
        if self.store.get(task_id).state not in ("RECEIVED", "PLANNING"):
            return {"accepted": False}
        self.store.transition(task_id, "request_clarification")
        self._task_event(task_id, "clarification_requested", {"question": question})
        return {"accepted": True}

    def _handle_clarification_reply(self, task_id: str, text: str) -> dict:
        self.store.transition(task_id, "clarify_reply")
        self._task_event(task_id, "clarified", {"reply": text})
        return {"accepted": True}

    # -- replan / partial (AD-3) -----------------------------------------
    def replan_or_partial(self, task_id: str, failed_stage: str, reason: str) -> dict:
        t = self.store.get(task_id)
        pb = get_playbook(t.playbook_id) if t else None
        cap = pb.back_edge_cap(failed_stage) if pb else None
        if cap is not None and t.replan_count < cap:
            self.store.bump_replan(task_id)
            target = next((b.target for b in pb.back_edges if b.stage == failed_stage), None)
            self.store.set_cursor(task_id, str(self._stage_index(pb, target)))
            self.store.transition(task_id, "replan")
            self._task_event(task_id, "replan",
                             {"failed_stage": failed_stage, "target": target, "reason": reason,
                              "attempts": t.replan_count + 1, "cap": cap})
            return {"state": "PLANNING", "task_id": task_id, "replanned": True}
        self.store.transition(task_id, "partial", fail_step=failed_stage)
        self._task_event(task_id, "closed", {"state": "PARTIAL",
                                             "fail": f"back-edge exceeded for {failed_stage}"})
        return {"state": "PARTIAL", "task_id": task_id, "replanned": False}

    def _stage_index(self, pb: Playbook, stage_name: str) -> int:
        for i, s in enumerate(pb.stages):
            if s.name == stage_name:
                return i
        return 0

    # -- task/topic publication (AD-5: sole publisher) -------------------
    def _task_event(self, task_id: str, event: str, payload: dict | None = None) -> None:
        if self.bus is None:
            return
        t = self.store.get(task_id) if task_id else None
        env = TaskEvent(event=event, task_id=task_id,
                        stage_name=payload.get("stage", "") if payload else "",
                        agent=payload.get("agent", "") if payload else "supervisor",
                        priority=t.priority if t else "",
                        payload=payload or {}, ts=_iso(time.time()))
        self.bus.publish(task_topic(task_id, event), env.to_dict(), qos=1)

    def _stage_name_at(self, task, pb: Playbook) -> str | None:
        """Name of the stage a task is currently parked on (its approval stage)."""
        idx = int(task.stage_cursor or "0")
        if pb and 0 <= idx < len(pb.stages):
            return pb.stages[idx].name
        return None