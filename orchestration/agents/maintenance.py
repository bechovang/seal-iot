"""Maintenance agent: reads runbooks + CMMS maintenance history and produces a plan.
Degraded fallback = deterministic template (never fabricates work)."""

from __future__ import annotations

from .base import BaseAgent, AgentContext


class MaintenanceAgent(BaseAgent):
    role = "maintenance"

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        history_rows = []
        if ctx.cmms is not None and hasattr(ctx.cmms, "lookup"):
            try:
                history_rows = ctx.cmms.lookup("maintenance_history")
            except Exception:
                history_rows = []
        runbook_hit = False
        if ctx.runbook_store is not None:
            try:
                rbh = ctx.runbook_store.find(ctx.task.request_text)
                runbook_hit = bool(rbh)
            except Exception:
                runbook_hit = False

        evidence = ctx.observations and [o.to_evidence() for o in ctx.observations] or []
        plan = _template_plan(ctx, evidence, runbook_hit)
        plan["runbook_hit"] = runbook_hit
        plan["history_rows"] = len(history_rows)
        plan["evidence"] = evidence
        return plan


def _template_plan(ctx: AgentContext, evidence: list, runbook_hit: bool) -> dict:
    device = (ctx.device_ids() or ["(device)"] )[0]
    action = "schedule inspection" if not runbook_hit else "apply recorded runbook"
    return {
        "title": f"Plan for {device}",
        "priority": ctx.task.priority if hasattr(ctx.task, "priority") else "ROUTINE",
        "steps": [
            {"order": 1, "device": device, "task": "confirm device telemetry baseline"},
            {"order": 2, "device": device, "task": action},
            {"order": 3, "device": device, "task": "schedule corrective work order"},
        ],
        "assumptions": [e["staleness"] for e in evidence if e.get("staleness") not in ("fresh",)],
    }