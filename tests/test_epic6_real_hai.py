"""Real HAI 21.03 integration test (validates mapping.yaml against the actual
vendor dataset in ``tai lieu/bo-du-lieu/03-HAI/``).

These tests exercise the REAL adapter code-path against REAL telemetry columns and
the REAL ``time`` column (space-separated ISO, event-time source). They are skipped
automatically when the dataset slice is not present on disk (e.g. clean CI clone or
before the competitor unpacks the secret files), so they never gate the fast test
suite on missing data.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from adapters.hai import HAIAdapter, load_source_slice, synthetic_rows
from history import HistoryBuffer

_TEST_FILE = Path(__file__).resolve().parent.parent / \
    "tai lieu" / "bo-du-lieu" / "03-HAI" / "hai-21.03_test1.csv.gz"


def _slice() -> dict:
    if not _TEST_FILE.exists():
        pytest.skip("HAI test1 dataset not present on disk")
    return load_source_slice(str(_TEST_FILE), max_rows=2600)


def _iter_rows(rows: dict):
    return (rows[k] for k in sorted(rows))


def _check(rows) -> HAIAdapter:
    from config import load_mapping

    mapping = load_mapping()
    assert not mapping.validate(), "mapping.yaml must validate"
    hist = HistoryBuffer(":memory:")
    adapter = HAIAdapter(mapping, bus=__import__("bus").InMemoryBus(), history=hist)
    episodes = list(adapter.stream(_iter_rows(rows)))
    return mapping, adapter, hist, episodes


# ---- REAL_COLUMNS_ALIGNED ----
def test_mapping_sources_exist_in_real_file():
    from config import load_mapping

    mapping = load_mapping()
    cols = set()
    with __import__("gzip").open(_TEST_FILE, "rt", encoding="utf-8") as fh:
        import csv
        cols = set(next(csv.reader(fh)))
    missing = [c.source for c in mapping.columns if c.source not in cols]
    assert not missing, f"mapping.yaml sources missing from HAI file: {missing}"


# ---- REAL_TIME_EVENT_ORDERING ----
def test_real_time_column_parsed_as_event_time():
    rows = load_source_slice(str(_TEST_FILE), max_rows=5)
    mapping = __import__("config").load_mapping()
    adapter = HAIAdapter(mapping)
    ts = [adapter.event_time(r) for _, r in sorted(rows.items())]
    assert len(ts) == 5
    assert ts == sorted(ts), "event-time from `time` column must be monotonic"
    assert ts[0] > 0, "timestamp must parse (HAI emits 'YYYY-MM-DD HH:MM:SS')"


# ---- REAL_INGEST_PUBLISHES_CANONICAL_TELEMETRY ----
def test_real_ingest_publishes_canonical_telemetry():
    rows = _slice()
    mapping, adapter, hist, episodes = _check(rows)
    assert adapter.published >= 2600, "expect one canonical telemetry msg per mapped signal per row"
    # telemetry landed in the single-writer history buffer, and detection ran
    assert hist.count() >= 2600, "history buffer must hold ingested samples"


# ---- REAL_ATTACK_WINDOW_YIELDS_EPISODE ----
def test_real_attack_window_detected():
    rows = _slice()
    mapping, adapter, hist, episodes = _check(rows)
    # first attack scene in test1 starts at row ~2111; our slice (2600 rows) covers it
    assert episodes, "expected at least one Episode from the real attack window in the slice"


# ---- SYNTHETIC_FALLBACK_STILL_VALID ----
def test_synthetic_fallback_still_valid_with_realigned_mapping():
    from config import load_mapping

    mapping = load_mapping()
    rows = synthetic_rows(mapping, n=120, seed=1)
    adapter = HAIAdapter(mapping, bus=__import__("bus").InMemoryBus())
    eps = list(adapter.stream(rows))
    assert all(e.score >= 0 for e in eps)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))