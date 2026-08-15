"""Streaming-statistical anomaly detection (AD-4).

Electric bias is bad. This module detects anomalies adaptively — never with static
thresholds on the raw telemetry value. Each signal is smoothed (EMA), the filter
innovation (prediction error) is normalized adaptively via a running mean/std
(Welford), and drift is additionally checked with river's ADWIN. A signal whose
innovation exceeds an *adaptive* z bound fires an evidence-backed episode.

A `ResidualChannel` interface is the placeholder for the deferred physics-residual
second channel (full implementation lands with the plant model, Epic 3).
"""

from __future__ import annotations

import math
from typing import Protocol

import numpy as np

from .episode import Episode, episode_key


class ResidualChannel(Protocol):
    """Placeholder for the deferred physics-residual second channel (AD-4)."""

    def residual(self, signal_id: str, value: float) -> float:
        """Return a residual for this sample; 0.0 means 'no physics model yet'."""
        ...


class NullResidualChannel:
    def residual(self, signal_id: str, value: float) -> float:
        return 0.0


class _Welford:
    """Online mean/variance (Welford) for adaptive normalization."""

    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        delta = x - self.mean
        self.m2 += d * delta

    def std(self) -> float:
        if self.n < 2:
            return 0.0
        var = self.m2 / (self.n - 1)
        return math.sqrt(max(var, 0.0))


class AdaptiveDetector:
    """EMA + adaptive-z + ADWIN streaming detector (primary PERCEIVE variant)."""

    detector_name = "ema_adwin"

    def __init__(
        self,
        ema_span: float = 20.0,
        adwin_delta: float = 0.002,
        z_threshold: float = 4.0,
        min_seed: int = 30,
        residual_channel: ResidualChannel | None = None,
        bucket_ms: int = 60_000,
    ) -> None:
        self.ema_span = ema_span
        self.adwin_delta = adwin_delta
        self.z_threshold = z_threshold
        self.min_seed = min_seed
        self.bucket_ms = bucket_ms
        self.residual = residual_channel or NullResidualChannel()
        self._ema: dict[str, float] = {}
        self._welford: dict[str, _Welford] = {}
        self._adwin: dict[str, "object"] = {}

    def _get_adwin(self, signal_id: str):
        if signal_id not in self._adwin:
            from river.drift import ADWIN

            self._adwin[signal_id] = ADWIN(delta=self.adwin_delta)
        return self._adwin[signal_id]

    def update(self, signal_id: str, value: float, ts_epoch_ms: int) -> Episode | None:
        """Ingest one sample; return an episode if an anomaly is evidenced."""
        alpha = 2.0 / (self.ema_span + 1.0)
        prev = self._ema.get(signal_id, value)
        self._ema[signal_id] = prev + alpha * (value - prev)

        innovation = abs(value - self._ema[signal_id]) + abs(self.residual.residual(signal_id, value))

        w = self._welford.setdefault(signal_id, _Welford())
        w.update(innovation)
        std = w.std()
        if std <= 1e-9:
            return None
        z = innovation / std

        adwin = self._get_adwin(signal_id)
        drift = bool(adwin.update(innovation))

        group = signal_id.rsplit("_", 1)[0] if "_" in signal_id else signal_id
        if z >= self.z_threshold or drift:
            return Episode(
                episode_key=episode_key(group, ts_epoch_ms, self.bucket_ms),
                signal_id=signal_id,
                score=float(z),
                filter_innovation=float(innovation),
                window_stats={"mean_innovation": w.mean, "std_innovation": std, "n": w.n},
                ts_epoch_ms=ts_epoch_ms,
                detector=self.detector_name,
                extra={"drift": drift},
            )
        return None


class IsolationForestVariant:
    """V1 ablation-floor baseline: batch IsolationForest over the history window."""

    detector_name = "isolation_forest"

    def __init__(self, contamination: float = 0.05) -> None:
        from sklearn.ensemble import IsolationForest

        self.contamination = contamination
        self._model: IsolationForest | None = None

    def score_window(self, signal_id: str, values: list[float], ts_epoch_ms: int) -> Episode | None:
        if len(values) < 10:
            return None
        x = np.asarray(values, dtype=float).reshape(-1, 1)
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(contamination=self.contamination, random_state=0)
        model.fit(x)
        preds = model.predict(x)
        if int(preds[-1]) == -1:
            group = signal_id.rsplit("_", 1)[0] if "_" in signal_id else signal_id
            return Episode(
                episode_key=episode_key(group, ts_epoch_ms),
                signal_id=signal_id,
                score=1.0,
                filter_innovation=0.0,
                window_stats={"n": len(values)},
                ts_epoch_ms=ts_epoch_ms,
                detector=self.detector_name,
            )
        return None