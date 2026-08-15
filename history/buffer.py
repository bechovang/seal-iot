"""Single-writer telemetry history ring buffer (AD-11).

`HistoryBuffer` is a SQLite-backed ring buffer. Only the ingest consumer writes it;
all other stages read. Uses WAL + busy_timeout and keeps exactly `capacity` most-recent
canonical telemetry rows keyed by (signal_id, ts_epoch_ms) in event time.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

_LOCK = threading.RLock()


class HistoryBuffer:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        capacity: int = 10_000,
        signal_count: int = 8,
    ) -> None:
        self.db_path = str(db_path)
        self.capacity = max(1, capacity)
        self.signal_count = max(1, signal_count)
        self._conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL;") if self.db_path != ":memory:" else None
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS telemetry ("
            " signal_id TEXT NOT NULL,"
            " ts_epoch_ms INTEGER NOT NULL,"
            " value REAL NOT NULL,"
            " quality TEXT NOT NULL,"
            " PRIMARY KEY (signal_id, ts_epoch_ms)"
            ")"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry (ts_epoch_ms)"
        )

    # ---- write (ingest consumer only) ----
    def write(self, signal_id: str, ts_epoch_ms: int, value: float, quality: str = "ok") -> None:
        """Append a sample. Caller is responsible for row ordering per signal."""
        with _LOCK:
            self._conn.execute(
                "INSERT OR REPLACE INTO telemetry(signal_id, ts_epoch_ms, value, quality) "
                "VALUES (?,?,?,?)",
                (signal_id, ts_epoch_ms, value, quality),
            )
            self._trim()

    def _trim(self) -> None:
        # boundary = capacity across signals; keep the most recent N rows globally
        self._conn.execute(
            "DELETE FROM telemetry WHERE ts_epoch_ms NOT IN "
            "(SELECT ts_epoch_ms FROM telemetry "
            " ORDER BY ts_epoch_ms DESC LIMIT ?)",
            (self.capacity,),
        )

    # ---- read (any stage) ----
    def recent(self, signal_ids: list[str] | None = None, limit: int = 200) -> list[tuple]:
        cols = ",".join(["signal_id", "ts_epoch_ms", "value", "quality"])
        q = f"SELECT {cols} FROM telemetry"
        args: list = []
        if signal_ids:
            marks = ",".join("?" * len(signal_ids))
            q += f" WHERE signal_id IN ({marks})"
            args.extend(signal_ids)
        q += " ORDER BY ts_epoch_ms DESC LIMIT ?"
        args.append(limit)
        cur = self._conn.execute(q, args)
        return list(cur.fetchall())

    def count(self) -> int:
        cur = self._conn.execute("SELECT COUNT(*) FROM telemetry")
        return int(cur.fetchone()[0])

    def close(self) -> None:
        with _LOCK:
            self._conn.close()