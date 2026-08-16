"""Action agent: emits structured ACTION INTENTS only — never executes, never writes.
The Tool port is the sole writer; the intent specifies intent type (work_order |
notification | approval_request | report), device, and evidence block (AD-1/AD-6)."""

from __future__ import annotations

from .base import BaseAgent, AgentContext


class ActionAgent(BaseAgent):
    role = "action"

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        evidence = ctx.observations and [o.to_evidence() for o in ctx.observations] or []
        # generic playbook stages declare no device -> fall back to the first observed
        # device (observer now scans the whole registry for generic requests), else None
        # -> the port rejects that work order -> act fails -> replan (AD-6).
        device = (ctx.device_ids()
                  or [o.device_id for o in ctx.observations[:1]]
                  or [None])[0]
        # Approval-marked act stage -> the port also creates an approval_request.
        intent = {
            "type": "work_order",
            "task_id": ctx.task.task_id,
            "device_id": device,
            "summary": f"{'Corrective' if not (evidence and evidence[0].get('staleness')=='fresh') else 'Preventive'} "
                       f"work for {device or 'device'}",
            "priority": ctx.task.priority if hasattr(ctx.task, "priority") else "ROUTINE",
            # work_order evidence must be a single block copied from the observer payload
            "evidence": evidence[0] if evidence else {},
            "idempotency_key": f"{ctx.task.task_id}+work_order+{device}",
        }
        if getattr(ctx.stage, "approval_marked", False):
            intent["approval"] = True
        return intent