"""Observer agent (AD-10): the ONLY place freshness is computed for the task layer.
Reads ``history/`` + the device registry and returns, per device, latest value,
event-time age, and a staleness class from config thresholds. Stale the-or-missing
data is stated in the finding and shapes the downstream plan — never silently
dropped and never filled with invented values."""

from __future__ import annotations

from typing import Any

from .base import BaseAgent, AgentContext, Observation


class ObserverAgent(BaseAgent):
    role = "observer"

    def deterministic(self, ctx: AgentContext) -> dict[str, Any]:
        device_ids = list(ctx.device_ids())
        # Scenario 1: expand to related devices when the stage asks for them.
        if getattr(ctx.stage, "inputs", {}).get("related"):
            for did in list(device_ids):
                for rel in ctx.registry.related(did):
                    if rel not in device_ids:
                        device_ids.append(rel)

        observations: list[Observation] = []
        # Reference event-time = the newest sample across observed signals (single
        # logical clock, AD-10). Never wall clock.
        ref_ts = 0
        if ctx.history is not None:
            sigs = []
            for did in device_ids:
                dev = ctx.registry.device(did)
                if dev:
                    sigs.extend(dev.signal_ids)
            rows = ctx.history.recent(sigs, limit=200) if sigs else []
            for _sid, ts, _val, _q in rows:
                ref_ts = max(ref_ts, int(ts))

        for did in device_ids:
            dev = ctx.registry.device(did)
            if dev is None:
                continue
            for signal_id in dev.signal_ids:
                observations.append(self._observe_signal(ctx, signal_id, ref_ts))

        evidence = [o.to_evidence() for o in observations]
        finding = _finding(observations)
        return {
            "role": self.role,
            "evidence": evidence,
            "observations": [o.to_dict() for o in observations],
            "finding": finding,
            "devices_observed": device_ids,
        }

    def _observe_signal(self, ctx: AgentContext, signal_id: str,
                        ref_ts: int) -> Observation:
        rows = ctx.history.recent([signal_id], limit=3) if ctx.history is not None else []
        unit = ctx.registry.signal_unit(signal_id)
        dev_id = signal_id.rsplit("_", 1)[0]
        if not rows:
            return Observation(device_id=dev_id, signal_id=signal_id, value=None,
                               event_ts="", age_seconds=None, staleness="offline",
                               quality="offline", unit=unit)
        ts, value, quality = rows[0][1], rows[0][2], rows[0][3]
        if quality == "missing_ts" or ts <= 0:
            return Observation(device_id=dev_id, signal_id=signal_id, value=None,
                               event_ts="", age_seconds=None, staleness="missing_ts",
                               quality="missing_ts", unit=unit)
        from datetime import datetime, timezone

        event_ts = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z")
        age = None if ref_ts <= 0 else max(0, (ref_ts - ts) / 1000.0)
        staleness = _staleness(age, ctx.runtime)
        return Observation(
            device_id=dev_id, signal_id=signal_id,
            value=value, event_ts=event_ts, age_seconds=age, staleness=staleness,
            quality=quality, unit=unit,
        )


def _staleness(age: float | None, runtime) -> str:
    if age is None:
        return "missing_age"
    if age >= runtime.critical_seconds:
        return "critical_stale"
    if age >= runtime.stale_seconds:
        return "stale"
    return "fresh"


def _finding(obs: list[Observation]) -> str:
    stale = [o for o in obs if o.staleness in ("stale", "critical_stale", "offline", "missing_ts")]
    if not obs:
        return "no device telemetry available for inspection"
    if not stale:
        return f"observed {len(obs)} signal(s); all fresh"
    names = ", ".join(o.signal_id for o in stale[:5])
    return f"{len(stale)} stale/offline signal(s): {names} — plan must assume degraded telemetry"