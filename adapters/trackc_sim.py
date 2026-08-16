"""Track C simulator — a deterministic source that emits the immutable contract
payload (via ``build_payload``) so the whole data path can run without a broker and,
once the broker host lands, feed the live ``submit``/`judge` topics. Event-time is
``BASE_TS + seq`` seconds (AD-10 — never wall clock); values are seeded
``base + amp*sin(2π·seq/period + phase) + noise``.

``force_status`` lets a demo force ``offline``/``error`` on specific devices so
Scenario 3 (a device gone stale) is rehearsable end-to-end.
"""

from __future__ import annotations

import math
import random
from typing import Any

from config import TrackCRegistry
from .trackc import build_payload

# Fixed event-time origin (the sample epoch the organizer shared); seq advances 1s each
# tick so event ordering is stable and replayable across hosts/timezones.
BASE_TS = 1784369401


# signal_id -> deterministic waveform profile
#   base, amp, period(seq), phase(cycles), noise_amp, unit
PROFILES: dict[str, dict[str, float]] = {
    "motor_01_current":    {"base": 12.0, "amp": 2.0, "period": 20.0, "phase": 0.0, "noise": 0.2},
    "motor_01_vibration":  {"base": 2.5,  "amp": 0.4, "period": 6.0,  "phase": 1.0, "noise": 0.05},
    "motor_01_temperature":{"base": 55.0, "amp": 3.0, "period": 60.0, "phase": 0.5, "noise": 0.4},
    "line_01_voltage":     {"base": 400.0,"amp": 4.0, "period": 30.0, "phase": 0.2, "noise": 0.8},
    "line_01_current":     {"base": 15.0, "amp": 1.5, "period": 15.0, "phase": 2.0, "noise": 0.3},
    "conveyor_01_speed":   {"base": 0.8,  "amp": 0.05,"period": 25.0, "phase": 0.8, "noise": 0.01},
    "conveyor_01_load":    {"base": 120.0,"amp": 10.0, "period": 18.0, "phase": 3.0, "noise": 2.0},
    "press_01_pressure":   {"base": 6.0,  "amp": 0.3, "period": 12.0, "phase": 0.4, "noise": 0.05},
    "probe_01_temperature":{"base": 185.0,"amp": 5.0, "period": 40.0, "phase": 0.1, "noise": 1.0},
    "gas_01_gas":          {"base": 35.0, "amp": 3.0, "period": 10.0, "phase": 0.7, "noise": 0.5},
}


class TrackCSim:
    """Deterministic contract-payload generator over the whole registry."""

    def __init__(self, registry: TrackCRegistry, seed: int = 21,
                 base_ts: int = BASE_TS) -> None:
        self.registry = registry
        self.seed = seed
        self.base_ts = base_ts
        self._rng = random.Random(seed)

    def _signal_value(self, signal_id: str, seq: int) -> float:
        # device/unmodified simple drift uses the global RNG per (signal,seq)? Keep it
        # cheap & deterministic: value is purely a function of seq (no cross-tick RNG
        # state), so replay yields identical waveforms. noise also keyed by seq.
        p = PROFILES.get(signal_id, {"base": 10.0, "amp": 1.0, "period": 20.0,
                                     "phase": 0.0, "noise": 0.1})
        base = p["base"]
        amp = p["amp"]
        period = p["period"]
        phase = p["phase"] * 2 * math.pi
        noise = p["noise"]
        wave = amp * math.sin(2 * math.pi * seq / period + phase)
        # deterministic per-(signal_id, seq) noise, no mutable RNG state
        r = random.Random(self.seed * 1000003 + seq * 7 + sum(ord(c) * 3 for c in signal_id) * 13)
        n = r.uniform(-noise, noise)
        return round(base + wave + n, 4)

    def _values_for_tick(self, seq: int) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for dev in self.registry.devices.values():
            metrics: dict[str, float] = {}
            for sig in dev.signals:
                signal_id = f"{dev.device_id}_{sig.metric}"
                metrics[sig.metric] = self._signal_value(signal_id, seq)
            out[dev.device_id] = metrics
        return out

    def tick(self, seq: int, force_status: dict[str, str] | None = None,
             environment: str | None = None, team_code: str | None = None) -> dict[str, Any]:
        """Return one contract payload for tick ``seq`` (event-time = base+seq). All
        six devices / ten signals are covered; ``force_status`` sets device status."""
        settings = globals().get("_SETTINGS", {})
        return build_payload(
            self._values_for_tick(seq),
            environment or settings.get("environment", "FACTORY"),
            team_code or settings.get("team", "UNDERRATED"),
            self.base_ts + seq,
            status=force_status,
        )


# Optional ambient settings injected by the pipeline so sim honours mqtt.env team/env.
_SETTINGS: dict[str, Any] = {}


def bind_settings(settings: dict[str, Any]) -> None:
    global _SETTINGS
    _SETTINGS = settings or {}