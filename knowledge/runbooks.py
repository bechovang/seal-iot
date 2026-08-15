"""Runbook wiki + closed symptom-token taxonomy (AD-3).

Store of runbook entries keyed by a \u201cfixed symptom-token vocabulary. Matching is
Jaccard over token sets. Free-form symptom strings are never stored or matched.
Runbook schema (id, symptom_tokens, root_cause, action, occurrences, reliability)
is owned here; this module is the sole writer/reader of runbooks.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import RunbookConfig


class Runbook:
    def __init__(
        self,
        rid: str,
        symptom_tokens: list[str],
        root_cause: str,
        action: str,
        occurrences: int = 1,
        reliability: float = 1.0,
    ) -> None:
        self.id = rid
        self.symptom_tokens = symptom_tokens
        self.root_cause = root_cause
        self.action = action
        self.occurrences = occurrences
        self.reliability = reliability

    @classmethod
    def from_dict(cls, d: dict) -> "Runbook":
        return cls(
            rid=d["id"],
            symptom_tokens=list(d.get("symptom_tokens", [])),
            root_cause=d.get("root_cause", ""),
            action=d.get("action", ""),
            occurrences=int(d.get("occurrences", 1)),
            reliability=float(d.get("reliability", 1.0)),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symptom_tokens": self.symptom_tokens,
            "root_cause": self.root_cause,
            "action": self.action,
            "occurrences": self.occurrences,
            "reliability": self.reliability,
        }


def jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class RunbookStore:
    """Queryable runbook store persisted as JSON files under ``store_path``."""

    def __init__(self, config: RunbookConfig) -> None:
        self.config = config
        self.store_path = Path(config.store_path)
        self._entries: dict[str, Runbook] = {}
        self._load()

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        for f in sorted(self.store_path.glob("*.json")):
            with f.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            for item in data if isinstance(data, list) else [data]:
                rb = Runbook.from_dict(item)
                self._entries[rb.id] = rb

    def save(self, rb: Runbook) -> None:
        """Write a runbook to disk (sole writer of runbooks)."""
        self.store_path.mkdir(parents=True, exist_ok=True)
        path = self.store_path / f"{rb.id}.json"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(rb.to_dict(), fh, indent=2)
        self._entries[rb.id] = rb

    def add(self, tokens: list[str], root_cause: str, action: str) -> Runbook:
        rid = f"rb-{len(self._entries) + 1:04d}"
        rb = Runbook(rid=rid, symptom_tokens=tokens, root_cause=root_cause, action=action)
        self.save(rb)
        return rb

    def all(self) -> list[Runbook]:
        return list(self._entries.values())

    def match(self, symptom_tokens: list[str]) -> Runbook | None:
        """Jaccard match against the closed taxonomy.

        Returns the best runbook if it clears the threshold AND meets the minimum
        matched-token count; otherwise None (DIAGNOSE proceeds to full reasoning).
        """
        if not symptom_tokens:
            return None
        best: Runbook | None = None
        best_score = 0.0
        for rb in self._entries.values():
            overlap = len(set(symptom_tokens) & set(rb.symptom_tokens))
            if overlap < self.config.min_tokens_match:
                continue
            score = jaccard(symptom_tokens, rb.symptom_tokens)
            if score > best_score:
                best, best_score = rb, score
        if best is not None and best_score >= self.config.jaccard_threshold:
            return best
        return None