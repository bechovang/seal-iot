"""Track C action surface (AD-6/AD-11): the Tool port + simulated CMMS. Agents never
write; only this layer creates artifacts, behind the validate -> create -> read-back
gate with a port-owned idempotency registry."""

from .cmms_sim import CMMSSim
from .port import ToolPort, ToolResult

__all__ = ["CMMSSim", "ToolPort", "ToolResult"]


def _production_seed(registry) -> list[dict]:
    """Seed production-context orders from the device registry (Scenario 2: an urgent
    production order that must be weighed against maintenance downtime)."""
    lines = [d.device_id for d in registry.devices.values() if d.name and d.is_aggregate] or ["line_01"]
    return [
        {"order_id": "ORD-1001", "priority": "URGENT", "line": lines[0],
         "due": "2026-08-16T18:00:00Z", "status": "active"},
        {"order_id": "ORD-1002", "priority": "ROUTINE", "line": lines[0],
         "due": "2026-08-17T06:00:00Z", "status": "active"},
    ]