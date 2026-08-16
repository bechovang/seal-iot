"""Production agent: reads the CMMS production-context table (orders/shift plan) so
decisions are grounded in live order pressure (Scenario 2)."""

from __future__ import annotations

from .base import BaseAgent, AgentContext


class ProductionAgent(BaseAgent):
    role = "production"

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        orders = []
        if ctx.cmms is not None and hasattr(ctx.cmms, "lookup"):
            try:
                orders = ctx.cmms.lookup("production_context")
            except Exception:
                orders = []
        urgent = [o for o in orders if o.get("priority") == "URGENT"] if orders else []
        return {
            "orders_context": orders,
            "urgent_orders": len(urgent),
            "summary": (f"{len(urgent)} urgent order(s) active; line downtime carries "
                        "production risk" if urgent else "no urgent order pressure"),
            "recommendation": "avoid unnecessary line downtime on urgent order" if urgent
                              else "standard production posture",
        }


class SafetyAgent(BaseAgent):
    role = "safety"

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        evidence = ctx.observations and [o.to_evidence() for o in ctx.observations] or []
        stale = [e for e in evidence if e.get("staleness") in ("stale", "critical_stale", "offline")]
        gas_risky = bool(ctx.device_ids())  # conservative when acting on any device
        return {
            "hazards": ["stale device telemetry" if stale else "routine equipment state"],
            "gas_hazard": gas_risky and any("gas" in d for d in ctx.device_ids()),
            "verdict": "proceed" if not stale else "proceed-with-caution",
            "evidence": evidence,
        }