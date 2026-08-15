"""VERIFY (AD-9/AD-10): observe the signals named in the action's expected_effect over
an event-time sample window (never wall clock) against the pre-action baseline and
classify improved | no_change | worsened per explicit thresholds. Only 'improved' may
resolve; no_change/worsened loop to diagnosing (bounded, handled by the FSM).

VERIFY emits the outcome event; it writes no metrics (LEARN does).
"""

from __future__ import annotations

from dataclasses import dataclass

from config import VerifyConfig
from history import HistoryBuffer


@dataclass
class Outcome:
    classification: str  # improved | no_change | worsened
    baseline: float | None
    post: float | None
    relative_change: float
    expected_effect: float | None = None

    def to_dict(self) -> dict:
        return {
            "classification": self.classification,
            "baseline": round(self.baseline, 4) if self.baseline is not None else None,
            "post": round(self.post, 4) if self.post is not None else None,
            "relative_change": round(self.relative_change, 4),
            "expected_effect": round(self.expected_effect, 4) if self.expected_effect is not None else None,
        }


class OutcomeClassifier:
    def __init__(self, history: HistoryBuffer, cfg: VerifyConfig) -> None:
        self.history = history
        self.cfg = cfg

    def _mean_window(self, signal_id: str, after_epoch_ms: int | None = None) -> float | None:
        series = list(self.history.window_series([signal_id], limit=100000).get(signal_id) or [])
        if after_epoch_ms is not None:
            after = [v for ts_ms, v in series if ts_ms > after_epoch_ms]
            series_rows = after
        else:
            series_rows = [v for _, v in series[-self.cfg.window_samples:]]
        if not series_rows:
            return None
        return sum(series_rows) / len(series_rows)

    def classify(self, signal_id: str, baseline: float | None = None,
                 after_epoch_ms: int | None = None,
                 expected_effect: float | None = None) -> Outcome:
        """Classify a signal's post-action outcome vs its pre-action baseline.

        baseline: pre-action mean of the named signal (falls back to the trailing
            pre-action window from history). after_epoch_ms: event-time marker; only
            samples AFTER it count as the post window, so accelerated replay can
            never confuse the window.
        """
        if baseline is None:
            baseline = self._mean_window(signal_id)
        post = self._mean_window(signal_id, after_epoch_ms=after_epoch_ms)
        if post is None:
            post = self._mean_window(signal_id)

        if baseline is None or post is None or abs(baseline) < 1e-12:
            return Outcome("no_change", baseline, post, 0.0, expected_effect)

        rel = (post - baseline) / abs(baseline)
        if rel >= self.cfg.improved_threshold:
            cls = "improved"
        elif rel <= -self.cfg.worsened_threshold:
            cls = "worsened"
        else:
            # within the neutral band: an action only counts as 'improved' if it moved
            # the signal in the expected direction by at least the expected effect
            if expected_effect is not None and (post - baseline) >= expected_effect:
                cls = "improved"
            else:
                cls = "no_change"
        return Outcome(cls, baseline, post, rel, expected_effect)