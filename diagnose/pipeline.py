"""DIAGNOSE orchestration: runbook-first, then causal+RCA+debate (AD-3, AD-5)."""

from __future__ import annotations

from config import HarnessConfig, Mapping
from history import HistoryBuffer
from knowledge import RunbookStore
from llm import LLMClient
from perceive import Episode
from bus import BusClient

from .causal import CausalGraphBuilder
from .debate import DebateGate, Diagnosis
from .matcher import RunbookMatcher
from .rca import RCAAgent


class Diagnoser:
    """Runbook match -> (hit ? done : causal graph + RCA + debate) -> Diagnosis."""

    def __init__(
        self,
        harness: HarnessConfig,
        mapping: Mapping,
        store: RunbookStore,
        history: HistoryBuffer,
        bus: BusClient | None = None,
        llm: LLMClient | None = None,
        causal: CausalGraphBuilder | None = None,
        rca: RCAAgent | None = None,
        debate: DebateGate | None = None,
    ) -> None:
        self.harness = harness
        self.mapping = mapping
        self.matcher = RunbookMatcher(store, harness)
        self.history = history
        self.bus = bus
        self.causal = causal or CausalGraphBuilder(harness.diagnosis)
        self.rca = rca or RCAAgent(harness.diagnosis.max_hypotheses)
        self.llm = llm or LLMClient(harness.llm)
        self.debate = debate or DebateGate(self.llm, harness.debate)

    def diagnose(self, episode: Episode) -> Diagnosis:
        from .rca import compute_divergence

        div = compute_divergence(self.mapping, self.history, episode.signal_id)
        runbook, tokens = self.matcher.match(episode, self.mapping, divergence=div.divergence)
        if runbook is not None:
            d = Diagnosis(
                episode_key=episode.episode_key,
                symptom_tokens=tokens,
                root_cause=runbook.root_cause,
                confidence=runbook.reliability,
                action_hint=runbook.action,
                debate_mode="runbook_hit",
                runbook_hit=True,
            )
            d.signal_id = episode.signal_id
            d.ts = _iso_from_epoch(episode.ts_epoch_ms)
        else:
            # identify involved signals = episode group + D/Z pair neighbors
            pair = self.mapping.pair_for(episode.signal_id)
            involved = [episode.signal_id]
            if pair is not None:
                involved += [pair.setpoint, pair.feedback]
            graph = self.causal.build(involved, self.history)
            hyps = self.rca.rank(episode, self.mapping, graph, self.history)
            if self.harness.variant.rule_only():
                # AD-13: deterministic rule diagnosis — no LLM, no debate
                top = hyps[0] if hyps else None
                d = Diagnosis(
                    episode_key=episode.episode_key,
                    symptom_tokens=tokens,
                    root_cause=top.root_cause if top else "unexplained deviation",
                    confidence=(top.confidence if top else 0.2),
                    action_hint="recalibrate",
                    debate_mode="rule_only",
                    hypotheses_json={"hypotheses": [h.to_dict() for h in hyps[:3]]},
                )
            else:
                d = self.debate.run(episode, hyps, tokens)
            d.signal_id = episode.signal_id
            d.ts = _iso_from_epoch(episode.ts_epoch_ms)
        if self.bus is not None:
            ts = _iso_from_epoch(episode.ts_epoch_ms)
            self.bus.publish_event(
                "diagnosis", "diagnosis", "diagnose",
                ts, d.to_dict(), episode_key=episode.episode_key,
            )
        return d


def _iso_from_epoch(epoch_ms: int) -> str:
    """Event-time ISO from the episode's source-assigned epoch (never wall clock)."""
    from datetime import datetime, timezone

    if epoch_ms <= 0:
        return "1970-01-01T00:00:00Z"
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")