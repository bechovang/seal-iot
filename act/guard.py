"""Guarded Action (AD-1 / AD-7): every command is gated by a DETERMINISTIC safety
shield; only the executor publishes to command topics. A blocked command is never
published and is escalated for human attention.
"""

from __future__ import annotations

from dataclasses import dataclass

from config import Command


@dataclass
class Action:
    command: Command | None
    target: float


@dataclass
class ShieldVerdict:
    allowed: bool
    reason: str


class SafetyShield:
    """Deterministic interlock gate. The only static thresholds in the system live
    here (as interlocks, never as the anomaly detector)."""

    def evaluate(self, action: Action) -> ShieldVerdict:
        cmd = action.command
        if cmd is None or action.command is None:
            return ShieldVerdict(False, "blocked: no (do-nothing) actions are not commands")
        if cmd is None:
            return ShieldVerdict(False, "blocked: unknown command")
        if not (cmd.safe_min <= action.target <= cmd.safe_max):
            return ShieldVerdict(
                False,
                f"blocked: target {action.target:g} outside safe envelope "
                f"[{cmd.safe_min:g}, {cmd.safe_max:g}]",
            )
        if not (cmd.min <= action.target <= cmd.max):
            return ShieldVerdict(
                False,
                f"blocked: target {action.target:g} outside physical range [{cmd.min:g}, {cmd.max:g}]",
            )
        return ShieldVerdict(True, "allowed")


class ActExecutor:
    """The SOLE publisher of command topics (AD-7/AD-8). Subscribing for kill-switch
    is optional; publishing is exclusive to this class."""

    def __init__(self, bus) -> None:
        self.bus = bus

    def execute(self, action: Action, ts: str, incident_key: str = "") -> dict:
        verdict = SafetyShield().evaluate(action)
        # publish the verdict itself (alongside action_status) so the UI can render
        # the interlock decision with the envelope it was judged against
        if hasattr(self.bus, "publish_event"):
            cmd = action.command
            self.bus.publish_event(
                "shield", "act", "shield", ts,
                {
                    "incident_id": incident_key,
                    "action": cmd.name if cmd else None,
                    "target": action.target,
                    "allowed": verdict.allowed,
                    "reason": verdict.reason,
                    "envelope_abs": (
                        {"min": cmd.min, "max": cmd.max,
                         "safe_min": cmd.safe_min, "safe_max": cmd.safe_max}
                        if cmd else None
                    ),
                },
                episode_key=incident_key,
            )
        if not verdict.allowed:
            self.bus.publish_event(
                "action_status", "act", "shield_blocked", ts,
                {"incident_key": incident_key, "reason": verdict.reason,
                 "action": action.command.name if action.command else None,
                 "target": action.target},
                episode_key=incident_key,
            )
            return {"executed": False, "reason": verdict.reason}

        self.bus.publish_command(action.command.name, action.target, ts)
        self.bus.publish_event(
            "action_status", "act", "command_executed", ts,
            {"incident_key": incident_key,
             "action": action.command.name, "target": action.target},
            episode_key=incident_key,
        )
        return {"executed": True}