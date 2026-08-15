"""One plant command\u2192response model (AD-12): simulation AND expected-effect AND
online calibration from the same module, so prediction-vs-outcome never lies.

Given a command and a target value, predicts the post-command value of the controlled
signal in absolute engineering units via a bounded, physics-lite linear response. On
Day 1 it calibrates online from observed (command, response) pairs, starting from HAI
priors, so DECIDE never optimizes against a model the live plant has contradicted.
"""

from __future__ import annotations

from config import Command


class PlantModel:
    def __init__(self) -> None:
        # per-command learned gain, seeded from command.effect_gain (HAI prior)
        self._gain: dict[str, float] = {}

    def _gain_for(self, cmd: Command) -> float:
        return self._gain.get(cmd.name, cmd.effect_gain)

    def predict(self, cmd: Command, target: float, baseline: float | None = None) -> dict:
        """Predict the post-command value of the controlled signal.

        physics-lite: post = baseline + gain * (target - baseline), bounded to the
        physical envelope [min, max]. Returns the predicted value and the effect
        delta vs baseline.
        """
        base = baseline if baseline is not None else cmd.default
        gain = self._gain_for(cmd)
        pred = base + gain * (target - base)
        pred = max(cmd.min, min(cmd.max, pred))
        return {"predicted": pred, "delta": pred - base, "gain": gain}

    def calibrate(self, cmd: Command, target: float, observed_response: float) -> float:
        """Online bounded-parameter update from an observed (command, response) pair."""
        base = cmd.default
        denom = target - base
        if abs(denom) < 1e-9:
            return self._gain_for(cmd)
        observed_gain = (observed_response - base) / denom
        # bounded update: clamp gain to a sane [0,1.5] so one outlier can't explode it
        updated = 0.7 * self._gain_for(cmd) + 0.3 * max(0.0, min(1.5, observed_gain))
        self._gain[cmd.name] = updated
        return updated