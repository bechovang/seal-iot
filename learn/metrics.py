"""Per-incident metric log (AD-11): learn/ is the SOLE writer of metrics. VERIFY
emits outcome events but writes nothing here. One row per incident; definitions are
owned here so nothing double-counts.

Fields: detection delay, RCA latency, resolution time, outcome, ablation arm.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


@dataclass
class MetricRow:
    incident_id: str
    episode_key: str = ""
    detection_delay_sec: float = 0.0
    rca_latency_sec: float = 0.0
    resolution_time_sec: float = 0.0
    outcome: str = ""            # improved | no_change | worsened | escalated
    arm: str = ""                # ablation arm label
    signal_id: str = ""
    unsafe_actions: int = 0
    downtime_avoided: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


class MetricStore:
    """SQLite single-writer metric log; read by score/ only."""

    def __init__(self, db_path) -> None:
        self.db_path = str(db_path)
        if str(db_path) != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS metrics(
                incident_id TEXT PRIMARY KEY,
                episode_key TEXT NOT NULL DEFAULT '',
                detection_delay_sec REAL NOT NULL DEFAULT 0,
                rca_latency_sec REAL NOT NULL DEFAULT 0,
                resolution_time_sec REAL NOT NULL DEFAULT 0,
                outcome TEXT NOT NULL DEFAULT '',
                arm TEXT NOT NULL DEFAULT '',
                signal_id TEXT NOT NULL DEFAULT '',
                unsafe_actions INTEGER NOT NULL DEFAULT 0,
                downtime_avoided INTEGER NOT NULL DEFAULT 0
            )"""
        )
        self._conn.commit()

    def record(self, row: MetricRow) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO metrics(
                incident_id, episode_key, detection_delay_sec, rca_latency_sec,
                resolution_time_sec, outcome, arm, signal_id, unsafe_actions,
                downtime_avoided)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (row.incident_id, row.episode_key, row.detection_delay_sec,
             row.rca_latency_sec, row.resolution_time_sec, row.outcome, row.arm,
             row.signal_id, row.unsafe_actions, int(row.downtime_avoided)),
        )
        self._conn.commit()

    def update_outcome(self, incident_id: str, outcome: str,
                       resolution_time_sec: float | None = None) -> None:
        row = self.get(incident_id)
        if row is None:
            return
        rt = row.resolution_time_sec if resolution_time_sec is None else resolution_time_sec
        self._conn.execute(
            "UPDATE metrics SET outcome=?, resolution_time_sec=? WHERE incident_id=?",
            (outcome, rt, incident_id),
        )
        self._conn.commit()

    def get(self, incident_id: str) -> Optional[MetricRow]:
        cur = self._conn.execute(
            "SELECT incident_id, episode_key, detection_delay_sec, rca_latency_sec, "
            "resolution_time_sec, outcome, arm, signal_id, unsafe_actions, downtime_avoided "
            "FROM metrics WHERE incident_id=?", (incident_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return MetricRow(
            incident_id=row[0], episode_key=row[1], detection_delay_sec=row[2],
            rca_latency_sec=row[3], resolution_time_sec=row[4], outcome=row[5],
            arm=row[6], signal_id=row[7], unsafe_actions=row[8],
            downtime_avoided=bool(row[9]),
        )

    def all_rows(self) -> list[MetricRow]:
        return [self.get(r[0]) for r in self._conn.execute("SELECT incident_id FROM metrics")]

    def close(self) -> None:
        self._conn.close()