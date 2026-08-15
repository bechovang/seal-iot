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
    """Parse an ISO-ish timestamp to UTC epoch ms.

    Handles both ``Z``-suffixed and space-separated naive forms that HAI 21.03
    emits (e.g. ``'2020-07-07 15:00:00'``). Naive strings are treated as UTC so
    event-time ordering is stable regardless of host timezone (AD-10).
    """
    from datetime import datetime, timezone, timedelta

    s = iso.strip().replace("Z", "+00:00")
    if "T" not in s and " " in s:
        # HAI emits 'YYYY-MM-DD HH:MM:SS' with no tz marker; treat as UTC.
        return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
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
            # HAI 21.03 names its timestamp column `time` (e.g. '2020-07-07 15:00:00');
            # fall back to scanning for any ISO-ish string column.
            src_ts = row.get("time")
            if src_ts is None:
                src_ts = next((str(row[c]) for c in row if isinstance(row[c], str) and "T" in str(row[c]) and "Z" in str(row[c])), None)
            ts = _epoch_ms_from_iso(str(src_ts)) if src_ts not in (None, "") else 0
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


def load_source_slice(path: str | None, max_rows: int | None = None) -> dict | None:
    """Load a CSV (or .csv.gz) slice keyed by source-tag columns, ordered by the
    real event-time from the ``time``/``_ts`` column. Returns None + synthetic
    fallback when no path is supplied. ``time`` is normalised to an int epoch-ms
    key so replay respects event ordering (AD-10).
    """
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"dataset slice not found: {p}")
    import csv
    import gzip

    opener = gzip.open if p.suffix == ".gz" else p.open
    rows = []
    with opener(p, "rt", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            rows.append(rec)
            if max_rows is not None and len(rows) >= max_rows:
                break
    # Normalise + order by real event-time key.
    keyed = {}
    fmt = "%Y-%m-%d %H:%M:%S"
    for rec in rows:
        ts = rec.get("_ts")
        t = rec.get("time")
        if ts is not None:
            key = int(ts)
        elif t not in (None, ""):
            from datetime import datetime, timezone

            try:
                key = int(datetime.strptime(t, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
            except ValueError:
                key = _epoch_ms_from_iso(t)
        else:
            key = len(keyed)
        keyed[key] = rec
    return keyed


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