"""Agent role registry (AD-4): one object per role, each with its own LLMClient
(every conversation is an independent LLM context). Roles are built by the
supervisor with per-role budgets so one role cannot starve another."""

from __future__ import annotations

from ..agents.base import AgentContext, BaseAgent, Observation
from ..agents.observer import ObserverAgent
from ..agents.maintenance import MaintenanceAgent
from ..agents.production import ProductionAgent, SafetyAgent
from ..agents.action import ActionAgent

ROLE_CLASSES = {
    "observer": ObserverAgent,
    "maintenance": MaintenanceAgent,
    "production": ProductionAgent,
    "safety": SafetyAgent,
    "action": ActionAgent,
}

__all__ = [
    "AgentContext", "BaseAgent", "Observation",
    "ObserverAgent", "MaintenanceAgent", "ProductionAgent", "SafetyAgent", "ActionAgent",
    "ROLE_CLASSES",
]