"""Epic 2 — Diagnosis & Runbook Memory tests.

Stories 2.1 (runbook Jaccard), 2.2 (causal graph heuristic weights),
2.3 (RCA D/Z-first hypotheses), 2.4 (debate gate + budget fallback).
"""

import shutil
from pathlib import Path

import pytest

from config import HarnessConfig, LLMConfig, RunbookConfig, load_mapping
from history import HistoryBuffer
from knowledge import Runbook, RunbookStore
from llm import LLMClient, MockLLM
from diagnose import (
    CausalGraphBuilder,
    DebateGate,
    Diagnoser,
    Diagnosis,
    RCAAgent,
)
from diagnose.matcher import extract_symptoms
from perceive import Episode


ROOT = Path(__file__).resolve().parent.parent
MAPPING = load_mapping(ROOT / "mapping.yaml")
HARNESS = HarnessConfig.load(ROOT / "harness.yaml")


def make_episode(signal_id="P1_FC_01", score=5.0, key="P1@120000"):
    return Episode(
        episode_key=key, signal_id=signal_id, score=score,
        filter_innovation=1.0, window_stats={"n": 60},
        ts_epoch_ms=120000,
    )


def make_store(tmp_path, entries):
    cfg = RunbookConfig(store_path=str(tmp_path / "runbooks"))
    store = RunbookStore(cfg)
    for e in entries:
        store.save(e)
    return store, cfg


# ---- Story 2.1: Runbook wiki + closed Jaccard matching ----
def test_jaccard_basic():
    from knowledge import jaccard
    assert round(jaccard(["a", "b", "c"], ["b", "c", "d"]), 3) == 0.5


def test_runbook_match_hit_and_miss(tmp_path):
    store, cfg = make_store(tmp_path, [
        Runbook("rb-0001", ["setpoint_divergence", "flow_dip"], "P1 FC-101 valve deadband", "recalibrate valve"),
    ])
    hit = store.match(["setpoint_divergence", "flow_dip"])
    assert hit is not None and hit.id == "rb-0001"
    miss = store.match(["temp_spike"])  # below min matched tokens/threshold
    assert miss is None


def test_symptom_extractor_only_closed_tokens():
    ep = make_episode(score=7.0)
    toks = extract_symptoms(ep, MAPPING, HARNESS.symptom_taxonomy, divergence=0.5)
    assert toks  # non-empty
    assert all(t in HARNESS.symptom_taxonomy for t in toks)


# ---- Story 2.2: Causal graph builder (heuristic weights) ----
def test_causal_graph_lag_correlation(tmp_path):
    hist = HistoryBuffer(str(tmp_path / "c.db"))
    # strongly coupled signals with a small time lead on a -> b
    from math import sin
    for i in range(200):
        a = sin(i / 5.0) + 3.0
        b = sin((i - 1) / 5.0) + 3.0  # b lags a
        hist.write("P1_FC_01", i * 1000, a)
        hist.write("P1_FC_01_Z", i * 1000, b)
    builder = CausalGraphBuilder(use_granger=False)
    g = builder.build(["P1_FC_01", "P1_FC_01_Z"], hist)
    edges = list(g.edges(data=True))
    assert edges, "expected at least one heuristic edge"
    for u, v, d in edges:
        assert d["heuristic"] is True
        assert 0.0 < d["weight"] <= 1.0
    assert builder.describe(g)  # presentable heuristic summary
    hist.close()


# ---- Story 2.3: RCA agent, D/Z first-class evidence ----
def test_rca_dz_divergence_prioritized(tmp_path):
    hist = HistoryBuffer(str(tmp_path / "r.db"))
    # setpoint steady ~10, feedback diverges to ~15 (sensor/fault pattern)
    for i in range(60):
        hist.write("P1_FC_01_D", i * 1000, 10.0)
        hist.write("P1_FC_01_Z", i * 1000, 10.0 + (5.0 if i > 30 else 0.0))
        hist.write("P1_FC_01", i * 1000, 10.0 + (5.0 if i > 30 else 0.0))
    agent = RCAAgent()
    builder = CausalGraphBuilder(use_granger=False)
    g = builder.build(["P1_FC_01", "P1_FC_01_D", "P1_FC_01_Z"], hist)
    hyps = agent.rank(make_episode(), MAPPING, g, hist)
    assert hyps and hyps[0].rank == 1
    j = agent.to_json(hyps)
    assert j["top"] is not None
    assert "confidence_interval" in j["top"]
    assert j["top"]["confidence"] > 0
    hist.close()


def test_rca_unknown_when_no_evidence(tmp_path):
    hist = HistoryBuffer(str(tmp_path / "u.db"))
    agent = RCAAgent()
    hyps = agent.rank(make_episode(signal_id="P1_P_01"), MAPPING, __import__("networkx").DiGraph(), hist)
    assert hyps and hyps[0].htype == "unknown"
    assert hyps[0].confidence <= 0.5
    hist.close()


# ---- Story 2.4: Debate gate + budget fallback ----
def test_debate_single_pass_fallback_when_budget_exhausted():
    llm = LLMClient(LLMConfig(budget_per_call_path=0))  # zero budget -> always single-pass
    gate = DebateGate(llm)
    hyps = [
        __import__("diagnose.rca", fromlist=["Hypothesis"]).Hypothesis(
            rank=1, root_cause="A", asset="P1_FC101", confidence=0.8, ci=[0.7, 0.9], htype="sensor_fault"),
    ]
    d = gate.run(make_episode(), hyps, ["setpoint_divergence"])
    assert isinstance(d, Diagnosis)
    assert d.debate_mode == "single_pass_fallback"
    assert d.root_cause == "A"


def test_debate_full_path_with_mock():
    llm = LLMClient(LLMConfig(budget_per_call_path=5), backend=MockLLM(5))
    gate = DebateGate(llm)
    from diagnose.rca import Hypothesis
    hyps = [Hypothesis(rank=1, root_cause="A", asset="P1", confidence=0.8, ci=[0.7, 0.9], htype="sensor_fault")]
    d = gate.run(make_episode(), hyps, ["setpoint_divergence"])
    assert d.debate_mode == "debate"


# ---- Diagnoser end-to-end: runbook hit vs full reasoning ----
def test_diagnoser_runbook_hit(tmp_path):
    store, cfg = make_store(tmp_path, [
        Runbook("rb-0001", ["setpoint_divergence", "flow_dip"], "valve deadband", "recalibrate"),
    ])
    hist = HistoryBuffer(str(tmp_path / "d.db"))
    for i in range(40):
        hist.write("P1_FC_01_Z", i * 1000, 10.0 + (5.0 if i > 20 else 0.0))
        hist.write("P1_FC_01_D", i * 1000, 10.0)
        hist.write("P1_FC_01", i * 1000, 10.0 + (5.0 if i > 20 else 0.0))
    diag = Diagnoser(HARNESS, MAPPING, store, hist)
    d = diag.diagnose(make_episode(score=5.0))
    assert d.runbook_hit is True
    assert d.root_cause == "valve deadband"


def test_diagnoser_full_reasoning_no_runbook(tmp_path):
    store, _ = make_store(tmp_path, [])  # empty wiki -> must reason
    hist = HistoryBuffer(str(tmp_path / "f.db"))
    for i in range(60):
        hist.write("P1_FC_01", i * 1000, 10.0 + (5.0 if i > 30 else 0.0))
    diag = Diagnoser(HARNESS, MAPPING, store, hist)
    d = diag.diagnose(make_episode(score=6.0))
    assert d.runbook_hit is False
    assert d.root_cause  # some root cause produced
    hist.close()


# ---- LLM budget + mock ----
def test_llm_budget_exhaustion():
    backend = MockLLM(2)
    llm = LLMClient(LLMConfig(budget_per_call_path=2), backend=backend)
    assert llm.complete_json("s", "{'a':1}") is not None
    assert llm.complete_json("s", "{'a':1}") is not None
    assert llm.complete_json("s", "{'a':1}") is None  # budget exhausted
    assert llm.exhausted is True


def test_epic1_still_passes():
    """Sanity that Epic 2 additions did not regress load_mapping/harness parsing."""
    assert MAPPING.dataset == "HAI_21_03"
    assert MAPPING.pairing and HARNESS.symptom_taxonomy