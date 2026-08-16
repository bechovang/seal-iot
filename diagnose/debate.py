"""Multi-agent debate gate (AD-5: proposer/critic/arbiter + AD-13 budget fallback).

A single agent's bias must not decide the final diagnosis. The gate runs proposer,
critic, and arbiter roles through the `llm/` wrapper. If the per-call-path LLM budget
is exhausted (or no LLM is available), it falls back to a deterministic single-pass
arbitration over the ranked hypotheses — so the loop still advances rather than hangs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from config import DebateConfig, LLMConfig
from llm import LLMClient
from perceive import Episode

from .rca import Hypothesis


@dataclass
class Diagnosis:
    episode_key: str
    symptom_tokens: list[str]
    root_cause: str
    confidence: float
    action_hint: str = ""
    debate_mode: str = "debate"  # debate | single_pass_fallback
    hypotheses_json: dict = field(default_factory=dict)
    runbook_hit: bool = False
    signal_id: str = ""
    ts: str = ""
    divergence: dict = field(default_factory=dict)
    causal_edges: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "episode_key": self.episode_key,
            "signal_id": self.signal_id,
            "symptom_tokens": self.symptom_tokens,
            "root_cause": self.root_cause,
            "confidence": self.confidence,
            "action_hint": self.action_hint,
            "debate_mode": self.debate_mode,
            "hypotheses": self.hypotheses_json,
            "runbook_hit": self.runbook_hit,
            "ts": self.ts,
            "divergence": self.divergence,
            "causal_edges": self.causal_edges,
        }


class DebateGate:
    ROLE_PROMPTS = {
        "proposer": (
            "You are the PROPOSER. Given RCA hypotheses, choose the single most likely "
            "root cause and explain why, returning JSON {\"root_cause\": str, \"action_hint\": str}."
        ),
        "critic": (
            "You are the CRITIC. Given the proposal and alternatives, note weaknesses and "
            "return JSON {\"root_cause\": str, \"accept\": bool, \"objection\": str}."
        ),
        "arbiter": (
            "You are the ARBITER. Given the proposal and the critique, emit the final gated "
            "diagnosis as JSON {\"root_cause\": str, \"confidence\": 0..1, \"action_hint\": str}."
        ),
    }

    def __init__(self, llm: LLMClient, config: DebateConfig | None = None) -> None:
        self.llm = llm
        self.config = config or DebateConfig()

    def run(
        self,
        episode: Episode,
        hypotheses: list[Hypothesis],
        symptom_tokens: list[str],
    ) -> Diagnosis:
        hyps_json = {
            "hypotheses": [h.to_dict() for h in hypotheses],
            "episode": {"signal_id": episode.signal_id, "score": episode.score},
            "symptoms": symptom_tokens,
        }
        context = json.dumps(hyps_json)
        # single-pass deterministic fallback: top hypothesis by confidence
        fallback = hypotheses[0] if hypotheses else None
        final = Diagnosis(
            episode_key=episode.episode_key,
            symptom_tokens=symptom_tokens,
            root_cause=fallback.root_cause if fallback else "cause unknown",
            confidence=fallback.confidence if fallback else 0.0,
            hypotheses_json=hyps_json,
            debate_mode="single_pass_fallback",
        )
        if not fallback:
            return final

        # proposer
        prop = self.llm.complete_json(self.ROLE_PROMPTS["proposer"], context)
        if prop is None:
            return final
        proposal = prop.get("root_cause") or fallback.root_cause
        action_hint = prop.get("action_hint", "")

        # critic
        crit = self.llm.complete_json(
            self.ROLE_PROMPTS["critic"], json.dumps({"proposal": proposal, "hypotheses": hyps_json})
        )
        if crit is None:
            # budget exhausted mid-path -> deterministic single-pass arbitration
            return Diagnosis(
                episode_key=episode.episode_key,
                symptom_tokens=symptom_tokens,
                root_cause=proposal,
                confidence=fallback.confidence,
                action_hint=action_hint,
                hypotheses_json=hyps_json,
                debate_mode="single_pass_fallback",
            )

        # arbiter
        arb = self.llm.complete_json(
            self.ROLE_PROMPTS["arbiter"],
            json.dumps({"proposal": proposal, "critique": crit}),
        )
        if arb is None:
            return Diagnosis(
                episode_key=episode.episode_key,
                symptom_tokens=symptom_tokens,
                root_cause=proposal,
                confidence=fallback.confidence,
                action_hint=action_hint,
                hypotheses_json=hyps_json,
                debate_mode="single_pass_fallback",
            )
        final.root_cause = arb.get("root_cause", proposal)
        final.confidence = float(arb.get("confidence", fallback.confidence))
        final.action_hint = arb.get("action_hint", action_hint)
        final.debate_mode = "debate"
        return final