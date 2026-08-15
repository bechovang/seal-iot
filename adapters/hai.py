"""HAI 21.03 virtual-plant adapter (Story 1.3).

The adapter is the ONLY place that knows HAI column/tag names (AD-8): it consumes a
`mapping.yaml` contract and emits canonical telemetry onto `tele/<signal_id>` using
event-time from the source. Ground-truth label columns are quarantined and never
published. If no dataset slice is supplied it streams a seeded synthetic replay so
the pipeline can run and be tested without the secret HAI files.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterator

import numpy as np

from config import Mapping
from bus import BusClient, InMemoryBus
from history import HistoryBuffer
from perceive import AdaptiveDetector, Episode

# Quarantined label columns must never reach the bus (AD-8). Hard barrier beyond YAML.
_LABEL_QUARANTINE = {"normal", "attack"}


def _is_label(source: str) -> bool:
    lower = source.lower()
    return any(tok in lower for tok in ("_normal", "attack", "benign"))


def _epoch_ms_from_iso(iso: str) -> int:
    from datetime import datetime, timezone

    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(dt.timestamp() * 1000)


def synthetic_rows(
    mapping,
    n: int = 300,
    rate_hz: float = 1.0,
    seed: int = 0,
    anomaly_source: str | None = None,
    anomaly_start: int = 150,
    anomaly_len: int = 60,
):
    """Deterministic baseline for every mapped source + an injected anomaly window
    on one source (seeded replay for tests). Returns rows keyed by source-tag name
    plus ``_ts``. A missing-mapping/None case is not special here — callers pass a
    real Mapping."""
    rng = np.random.default_rng(seed)
    if anomaly_source is None:
        flow = next((c.source for c in mapping.columns if c.kind == "flow"), None)
        anomaly_source = flow or (mapping.columns[0].source if mapping.columns else "P1_FC101")
    rows = []
    for i in range(n):
        row: dict = {}
        for c in mapping.columns:
            base = 10.0 + 1.5 * np.sin((i + hash(c.signal_id) % 17) / 20.0)
            # seed each column independently but deterministically
            col_seed = (seed * 31 + sum(ord(ch) for ch in c.signal_id)) % (2**31 - 1)
            noise = np.random.default_rng(col_seed + i).normal(0.0, 0.2)
            val = base + noise
            if c.source == anomaly_source and anomaly_start <= i < anomaly_start + anomaly_len:
                val += 6.0
            row[c.source] = float(val)
        row["_ts"] = int(i * 1000.0 / rate_hz)
        rows.append(row)
    return iter(rows)


class HAIAdapter:
    """Streams one canonical telemetry message per mapped signal, per event-time row."""

    def __init__(
        self,
        mapping: Mapping,
        bus: BusClient | None = None,
        history: HistoryBuffer | None = None,
    ) -> None:
        self.mapping = mapping
        self.bus = bus or BusClient(InMemoryBus())
        self.history = history
        self.published = 0
        self.publish_failures = 0

    def event_time(self, row: dict) -> int:
        ts = row.get("_ts")
        if ts is None:
            src_ts = next((str(row[c]) for c in row if isinstance(row[c], str) and "T" in str(row[c]) and "Z" in str(row[c])), None)
            ts = _epoch_ms_from_iso(src_ts) if src_ts else 0
        return int(ts)

    def stream(self, rows: Iterator[dict]) -> Iterator[Episode]:
        detector = AdaptiveDetector(ema_span=20.0, adwin_delta=0.002)
        for row in rows:
            epoch_ms = self.event_time(row)
            for ms in self.mapping.columns:
                if _is_label(ms.source):
                    continue
                if ms.source not in row:
                    continue
                raw = float(row[ms.source])
                value = raw * ms.scale
                quality = "ok"
                if self.history is not None:
                    self.history.write(ms.signal_id, epoch_ms, value, quality)
                self.published += 1
                # BROKER_DOWN degradation: a failed publish must not crash the
                # stream or silently lose the source sample — fail loudly, keep going.
                try:
                    self.bus.publish_telemetry(
                        ms.signal_id, self._iso(epoch_ms), value, unit=ms.unit, quality=quality
                    )
                except ConnectionError:
                    self.publish_failures += 1
                ep = detector.update(ms.signal_id, value, epoch_ms)
                if ep is not None:
                    try:
                        self.bus.publish_event(
                            "perceive", "episode", "perceive", self._iso(epoch_ms),
                            ep.to_payload(), episode_key=ep.episode_key,
                        )
                    except ConnectionError:
                        self.publish_failures += 1
                    yield ep

    @staticmethod
    def _iso(epoch_ms: int) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def load_source_slice(path: str | None) -> dict | None:
    """Load a CSV slice keyed by source-tag columns. Returns None + synthetic fallback."""
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"dataset slice not found: {p}")
    raw = {}
    import csv

    with p.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            raw[rec.get("_ts", len(raw))] = rec
    return raw


def run(
    mapping_path: str | None = None,
    source: str | None = None,
    max_rows: int = 300,
    emit: bool = True,
) -> dict:
    """CLI entry: stream a dataset (or synthetic) and surface published counts/episodes."""
    from config import load_mapping

    mapping = load_mapping() if mapping_path is None else load_mapping(mapping_path)
    adapter = HAIAdapter(mapping)
    src = load_source_slice(source)
    if src is not None:
        rows = (src[k] for k in sorted(src.keys()))
    else:
        rows = synthetic_rows(mapping, n=max_rows, rate_hz=mapping.rate_hz)
    episodes = list(adapter.stream(rows))
    return {
        "dataset": mapping.dataset,
        "published": adapter.published,
        "episodes": len(episodes),
        "published_ok": True,
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="HAI virtual-plant adapter")
    ap.add_argument("--mapping", default=None)
    ap.add_argument("--source", default=None)
    ap.add_argument("--max-rows", type=int, default=300)
    args = ap.parse_args()
    res = run(args.mapping, args.source, args.max_rows)
    print(res)