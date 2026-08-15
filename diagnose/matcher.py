"""Runbook matcher (AD-3): the first step of DIAGNOSE before any full reasoning.

Maps episode evidence onto the closed symptom-token taxonomy (bounded vocabulary in
harness.yaml) and attempts a Jaccard match against the runbook wiki. Free-form
symptom strings are never produced or matched here.
"""

from __future__ import annotations

from config import HarnessConfig, Mapping
from knowledge import RunbookStore
from perceive import Episode


def extract_symptoms(
    episode: Episode,
    mapping: Mapping,
    taxonomy: list[str],
    divergence: float = 0.0,
) -> list[str]:
    """Derive a subset of the taxonomy token set from episode + D/Z divergence evidence."""
    tax = set(taxonomy)
    tokens: list[str] = []
    if divergence and divergence > 0.2:
        tokens.append("setpoint_divergence")
        tokens.append("feedback_offset")
    sig = mapping.by_id.get(episode.signal_id)
    kind = sig.kind if sig else ""
    if kind == "flow":
        tokens.append("flow_dip")
    elif kind == "pressure":
        tokens.append("pressure_drop")
    elif kind == "temperature":
        tokens.append("temp_spike")
    if episode.score >= 6.0:
        tokens.append("noise_burst")
    return [t for t in tokens if t in tax]


class RunbookMatcher:
    def __init__(self, store: RunbookStore, harness: HarnessConfig) -> None:
        self.store = store
        self.harness = harness

    def extract(self, episode: Episode, mapping: Mapping, divergence: float = 0.0) -> list[str]:
        return extract_symptoms(
            episode, mapping, self.harness.symptom_taxonomy, divergence=divergence
        )

    def match(self, episode: Episode, mapping: Mapping, divergence: float = 0.0) -> tuple:
        """Return (runbook, symptom_tokens). runbook is None on a miss (full reasoning)."""
        tokens = self.extract(episode, mapping, divergence)
        return self.store.match(tokens), tokens