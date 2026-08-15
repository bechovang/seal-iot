"""Append-only JSONL event recorder + replay (Epic 5.1 / AD-14 replay story).

Every bus event is appended as a JSON line. The same log serves as the failure-
recovery replay fixture: ``replay()`` feeds lines back through a handler so a demo
incident can be replayed end-to-end and asserted.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Iterable

Handler = Callable[[str, dict], None]  # (topic, payload)


class JsonlRecorder:
    """Subscribe-source-agnostic writer: append any dict as one JSON line."""

    def __init__(self, path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # append-only in text mode with UTF-8
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, topic: str, payload: dict) -> None:
        line = {"topic": topic, "payload": payload}
        self._fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        self._fh.flush()

    def hook(self, topic: str, payload: dict) -> None:
        """Drop-in bus handler."""
        self.write(topic, payload)

    def close(self) -> None:
        self._fh.close()


def record_messages(recorder, topics: Iterable[tuple[str, dict]]) -> None:
    for topic, payload in topics:
        recorder.write(topic, payload)


def replay(path, handler: Handler) -> int:
    """Replay all lines from a JSONL log through ``handler``. Returns line count."""
    count = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            handler(obj["topic"], obj["payload"])
            count += 1
    return count


def topics_in(log_path, prefix: str) -> list[dict]:
    hits = []
    with open(log_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj["topic"].startswith(prefix):
                hits.append(obj["payload"])
    return hits