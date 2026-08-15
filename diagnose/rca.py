"""RCA agent (AD-5): ranked root-cause hypotheses with confidence.

Reasons over (a) the heuristic causal graph, (b) asset topology, and (c) D/Z
setpoint\u2013feedback pairing. D/Z divergence is first-class evidence that
distinguishes actuator manipulation from sensor fault. Output is structured JSON.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import networkx as nx

from config import Mapping
from history import HistoryBuffer
from perceive import Episode


@dataclass
class DivergenceEvidence:
    setpoint_mean: float
    feedback_mean: float
    divergence: float  # mean(|Z - D|) normalized
    setpoint_std: float
    hypothesis_type: str  # actuator_manipulation | sensor_fault | unknown
    rationale: str


@dataclass
class Hypothesis:
    rank: int
    root_cause: str
    asset: str | None
    confidence: float
    ci: list  # uncertainty quantification [lo, hi]
    htype: str
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        return {"rank": d["rank"], "root_cause": d["root_cause"], "asset": d["asset"],
                "confidence": d["confidence"], "confidence_interval": d["ci"],
                "type": d["htype"], "evidence": d["evidence"]}


def compute_divergence(mapping: Mapping, history: HistoryBuffer, signal_id: str) -> DivergenceEvidence:
    pair = mapping.pair_for(signal_id)
    if pair is None:
        return DivergenceEvidence(0, 0, 0, 0, "unknown", "no D/Z pairing for signal")
    sp = history.window_series([pair.setpoint], limit=200).get(pair.setpoint, [])
    fb = history.window_series([pair.feedback], limit=200).get(pair.feedback, [])
    if not sp or not fb:
        return DivergenceEvidence(0, 0, 0, 0, "unknown", "insufficient D/Z history")
    sp_v = [v for _, v in sp]
    fb_v = [v for _, v in fb]
    sp_mean, fb_mean = sum(sp_v) / len(sp_v), sum(fb_v) / len(fb_v)
    div = sum(abs(s - f) for s, f in zip(sp_v, fb_v)) / min(len(sp_v), len(fb_v))
    sp_std = (sum((v - sp_mean) ** 2 for v in sp_v) / len(sp_v)) ** 0.5
    # actuator manipulation: setpoint actively changing (nonzero std) OR command
    # saturated while feedback lags; sensor fault: setpoint steady, feedback deviates
    nz = max(abs(sp_mean), 1e-6)
    norm = div / nz
    if norm > 0.2:
        if sp_std / max(abs(sp_mean), 1e-6) > 0.02:
            htype, why = "actuator_manipulation", "D/Z divergence with moving setpoint/command"
        else:
            htype, why = "sensor_fault", "D/Z divergence with steady setpoint, deviating feedback"
    else:
        htype, why = "unknown", "no significant D/Z divergence"
    return DivergenceEvidence(round(sp_mean, 3), round(fb_mean, 3), round(norm, 3), round(sp_std, 3), htype, why)


class RCAAgent:
    def __init__(self, max_hypotheses: int = 3) -> None:
        self.max_hypotheses = max_hypotheses

    def rank(
        self,
        episode: Episode,
        mapping: Mapping,
        graph: nx.DiGraph,
        history: HistoryBuffer,
    ) -> list[Hypothesis]:
        div = compute_divergence(mapping, history, episode.signal_id)
        hyps: list[Hypothesis] = []
        topo = []
        # graph-based hypotheses: neighbors of the anomaly signal with strong weight
        if graph.has_node(episode.signal_id):
            for pred, _, w in graph.in_edges(episode.signal_id, data="weight"):
                method = graph.get_edge_data(pred, episode.signal_id).get("method", "lagkor")
                topo.append((pred, w, method))
        topo.sort(key=lambda t: -t[1])
        for sibling, w, method in topo[: self.max_hypotheses]:
            asset = mapping.asset_for(sibling)
            conf = min(0.95, 0.4 + 0.5 * w)
            ci = [round(max(0, conf - 0.12), 3), round(min(1.0, conf + 0.12), 3)]
            hyps.append(Hypothesis(
                rank=len(hyps) + 1,
                root_cause=f"{sibling} leading driver of {episode.signal_id} deviation",
                asset=asset,
                confidence=round(conf, 3),
                ci=ci,
                htype="graph_edge",
                evidence=[f"{sibling}->{episode.signal_id} {method} weight={w:.2f}"],
            ))
        # D/Z-aware hypothesis is first-class
        asset = mapping.asset_for(episode.signal_id)
        if div.hypothesis_type != "unknown":
            d_conf = min(1.0, 0.45 + 0.4 * min(1.0, div.divergence))
            ci = [round(max(0.05, d_conf - 0.15), 3), round(min(1.0, d_conf + 0.15), 3)]
            hyps.insert(0, Hypothesis(
                rank=1,
                root_cause=div.rationale,
                asset=asset,
                confidence=round(d_conf, 3),
                ci=ci,
                htype=div.hypothesis_type,
                evidence=[
                    f"D/Z divergence={div.divergence} (SP={div.setpoint_mean}, FB={div.feedback_mean})",
                    div.rationale,
                ],
            ))
        if not hyps:
            hyps.append(Hypothesis(
                rank=1,
                root_cause=f"unexplained deviation of {episode.signal_id}",
                asset=asset,
                confidence=0.35,
                ci=[0.2, 0.5],
                htype="unknown",
                evidence=["no causal edge or D/Z divergence exceeded thresholds"],
            ))
        # re-rank after the D/Z-first insertion
        for i, h in enumerate(hyps, 1):
            h.rank = i
        return hyps[: self.max_hypotheses]

    def to_json(self, hyps: list[Hypothesis]) -> dict[str, Any]:
        top = hyps[0].to_dict() if hyps else None
        return {"top": top, "hypotheses": [h.to_dict() for h in hyps]}