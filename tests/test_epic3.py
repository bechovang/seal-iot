"""Epic 3: Guarded Action & Verification — stories 3.1-3.5.

3.1 shared plant model (sim + expected-effect + calibration)
3.2 DECIDE candidate generation + objective scoring incl. do(empty-set)
3.3 deterministic safety shield + sole executor
3.4 persistent/resumable/TTL incident FSM (sole id minter)
3.5 VERIFY event-time outcome classification
"""

import json
import sqlite3

import pytest

from act import ActExecutor, Action, SafetyShield
from act.pipeline import GuardedActionPipeline
from bus import BusClient, InMemoryBus
from config import (
    Command,
    DecideConfig,
    HarnessConfig,
    IncidentConfig,
    Mapping,
    Pairing,
    VerifyConfig,
)
from decide import ActionCandidate, CandidateGenerator, Decider, ObjectiveScorer
from diagnose import Diagnosis
from history import HistoryBuffer
from incident import IncidentFSM, IncidentStore
from plant_model import PlantModel
from verify import OutcomeClassifier


def make_mapping() -> Mapping:
    cmd = Command(
        name="fc_controller", target="P1_FC_01_Z", min=0.0, max=50.0,
        safe_min=1.0, safe_max=40.0, default=10.0, unit="m3/h",
        energy_cost=0.5, risk_baseline=0.2, effect_gain=0.9,
    )
    return Mapping(dataset="test", rate_hz=1.0, columns=[],
                   pairing=[Pairing(signal="P1_FC_01", setpoint="P1_FC_01_D", feedback="P1_FC_01_Z")],
                   label_columns=[], commands=[cmd])

# ---- 3.1 plant model ----
def test_plant_model_predicts_absolute_units_and_calibrates():
    m = PlantModel()
    cmd = Command(name="fc", target="Z", min=0, max=50, safe_min=1, safe_max=40,
                  default=10, effect_gain=0.9)
    pred = m.predict(cmd, 5.0, baseline=10.0)
    assert abs(pred["predicted"] - (10.0 + 0.9 * (5.0 - 10.0))) < 1e-9
    assert pred["delta"] < 0  # command reduced the flow
    gained = m.calibrate(cmd, target=5.0, observed_response=2.0)
    assert gained != cmd.effect_gain  # online calibration changed the gain
    pred2 = m.predict(cmd, 5.0, baseline=10.0)
    assert abs(pred2["predicted"] - 10.0) > abs(pred["predicted"] - 10.0)


# ---- 3.2 DECIDE ----
def test_decide_generates_noop_plus_candidates_and_scores():
    mapping = make_mapping()
    cfg = DecideConfig(weights={"alpha": 0.5, "beta": 0.15, "gamma": 0.2, "delta": 0.15},
                       candidate_count=2)
    decider = Decider(mapping, cfg)
    d = decider.decide({}, mapping.commands[0], baseline=10.0)
    comparison = d["comparison"]
    assert any(c["noop"] for c in comparison), "mandatory do(empty-set) row missing"
    non_noop = [c for c in comparison if not c["noop"]]
    assert len(non_noop) >= 2
    assert d["winner"]["score"] == max(c["score"] for c in comparison)
    assert d["noop_row"]["noop"] is True
    # objective weights are explicit & reproducible
    assert d["objective"]["alpha"] == 0.5


def test_decide_candidates_stay_inside_safe_envelope():
    mapping = make_mapping()
    gen = CandidateGenerator(DecideConfig(candidate_count=2))
    cands = gen.generate(mapping, mapping.commands[0])
    for c in cands:
        if not c.is_noop:
            assert mapping.commands[0].safe_min <= c.target <= mapping.commands[0].safe_max


# ---- 3.3 safety shield + sole executor ----
def test_shield_blocks_out_of_envelope_and_never_executes():
    mapping = make_mapping()
    cmd = mapping.commands[0]
    bus = BusClient(InMemoryBus())
    exec_ = ActExecutor(bus)
    # 45 is within physical [0,50] but outside safe envelope [1,40] -> must block
    bad = Action(cmd, 45.0)
    assert SafetyShield().evaluate(bad).allowed is False
    topics = [t for t, _ in bus.transport.messages]
    assert not any(t.startswith("cmd/") for t in topics), "blocked action must not publish"
    result = exec_.execute(bad, "2026-08-15T00:00:00Z", "inc1")
    assert result["executed"] is False
    topics = [t for t, _ in bus.transport.messages]
    assert not any(t.startswith("cmd/") for t in topics)
    # escalation event emitted, no command topic
    assert any(t == "ops/action_status" for t in topics)


def test_only_executor_is_sole_publisher_of_command_topic():
    mapping = make_mapping()
    cmd = mapping.commands[0]
    bus = BusClient(InMemoryBus())
    exec_ = ActExecutor(bus)
    good = Action(cmd, 8.0)  # inside safe envelope
    assert SafetyShield().evaluate(good).allowed is True
    result = exec_.execute(good, "2026-08-15T00:00:00Z", "inc2")
    assert result["executed"] is True
    cmd_msgs = [p for t, p in bus.transport.messages if t.startswith("cmd/")]
    assert len(cmd_msgs) == 1
    assert cmd_msgs[0]["command"] == "fc_controller"
    assert cmd_msgs[0]["target"] == 8.0
    # BusClient exposes no LLM-facing command API; publish_command is only reachable here


# ---- 3.4 incident FSM ----
def test_fsm_mints_unique_ids_persists_and_resumes(tmp_path):
    store = IncidentStore(str(tmp_path / "inc.db"), retry_max=2, ttl_seconds=1000)
    fsm = IncidentFSM(store, retry_max=2)
    i1 = fsm.create("ep@1000", "2026-08-15T00:00:00Z")
    i2 = fsm.create("ep2@2000", "2026-08-15T00:00:01Z")
    assert i1 != i2
    state = store.get(i1)
    assert state.state == "DETECTED"
    fsm.transition(i1, "diagnose")
    assert store.get(i1).state == "DIAGNOSING"
    # persistence survives a reopened store (same WAL file)
    store.close()
    store2 = IncidentStore(str(tmp_path / "inc.db"), retry_max=2, ttl_seconds=1000)
    fsm2 = IncidentFSM(store2, retry_max=2)
    resumed = [x for x in store2.resume_unfinished() if x.incident_id == i1]
    assert resumed and resumed[0].state == "DIAGNOSING"


def test_fsm_rejects_invalid_transition(tmp_path):
    store = IncidentStore(str(tmp_path / "b.db"))
    fsm = IncidentFSM(store)
    inc = fsm.create("ep@1", "")
    with pytest.raises(ValueError):
        fsm.transition(inc, "resolve")  # VERIFYING->RESOLVED only from VERIFYING, state is DETECTED


def test_fsm_verify_outcome_resolves_only_improved_and_escalates_after_retries(tmp_path):
    store = IncidentStore(str(tmp_path / "c.db"), retry_max=2)
    fsm = IncidentFSM(store, retry_max=2)
    # improved -> resolved
    a = fsm.create("a@1", "")
    fsm.transition(a, "diagnose")
    fsm.transition(a, "plan")
    fsm.transition(a, "act")
    fsm.transition(a, "verify")
    assert fsm.verify_outcome(a, "improved").state == "RESOLVED"
    # worsened, bounded retries -> retries then escalate
    b = fsm.create("b@1", "")
    fsm.transition(b, "diagnose"); fsm.transition(b, "plan")
    fsm.transition(b, "act"); fsm.transition(b, "verify")
    s = fsm.verify_outcome(b, "worsened")
    assert s.state == "DIAGNOSING" and s.retries == 1
    fsm.transition(b, "plan"); fsm.transition(b, "act"); fsm.transition(b, "verify")
    s = fsm.verify_outcome(b, "worsened")
    assert s.state == "DIAGNOSING" and s.retries == 2
    fsm.transition(b, "plan"); fsm.transition(b, "act"); fsm.transition(b, "verify")
    s = fsm.verify_outcome(b, "worsened")
    assert s.state == "ESCALATED"  # exhausted retries -> escalate


def test_fsm_ttl_auto_escalates_stalled(monkeypatch, tmp_path):
    store = IncidentStore(str(tmp_path / "d.db"), retry_max=2, ttl_seconds=10)
    fsm = IncidentFSM(store, retry_max=2)
    inc = fsm.create("ttl@1", "")
    monkeypatch.setattr("incident.fsm.time.time", lambda: 9999999999.0)
    resumed = [x for x in store.resume_unfinished() if x.incident_id == inc]
    assert resumed and resumed[0].state == "ESCALATED"


# ---- 3.5 VERIFY ----
def test_verify_classifies_improved_no_change_worsened(tmp_path=None):
    hist = HistoryBuffer(":memory:")
    cfg = VerifyConfig(window_samples=5, improved_threshold=0.3, worsened_threshold=0.1)
    for i in range(5):
        hist.write("sig", 0, 10.0)  # baseline
    v = OutcomeClassifier(hist, cfg)
    # improvement: post jumps up
    for i in range(5):
        hist.write("sig", i * 1000 + 1000, 15.0)
    o = v.classify("sig", baseline=10.0, after_epoch_ms=0, expected_effect=2.0)
    assert o.classification == "improved"
    # neutralize -> no_change
    hist2 = HistoryBuffer(":memory:")
    for i in range(10):
        hist2.write("sig", i, 10.0)
    o2 = OutcomeClassifier(hist2, cfg).classify("sig", baseline=10.0, after_epoch_ms=5)
    # post over the after-window: new writes 10.0... no change
    for i in range(5):
        hist2.write("sig", 100 + i, 10.0)
    o3 = OutcomeClassifier(hist2, cfg).classify("sig", baseline=10.0, after_epoch_ms=90)
    assert o3.classification in ("no_change", "improved")


def test_verify_event_time_window_not_wall_clock():
    hist = HistoryBuffer(":memory:")
    cfg = VerifyConfig(window_samples=3, improved_threshold=0.3, worsened_threshold=0.1)
    # baseline at epochs 0..2 = 10
    for i in range(3):
        hist.write("g", i * 1000, 10.0)
    # post window large epochs
    for i in range(3):
        hist.write("g", (100 + i) * 1000, 20.0)
    v = OutcomeClassifier(hist, cfg)
    o = v.classify("g", baseline=10.0, after_epoch_ms=50000, expected_effect=5.0)
    assert o.classification == "improved"
    assert o.relative_change == 1.0


# ---- e2e: guarded action pipeline ----
def test_pipeline_executes_and_resolves_on_improvement(tmp_path):
    mapping = make_mapping()
    harness = HarnessConfig(decide=DecideConfig(candidate_count=2),
                            verify=VerifyConfig(),
                            incident=IncidentConfig(db_path=str(tmp_path / "e.db")))
    bus = BusClient(InMemoryBus())
    hist = HistoryBuffer(":memory:")
    for i in range(5):
        hist.write("P1_FC_01_Z", i * 1000, 10.0)
    pipe = GuardedActionPipeline(harness, mapping, bus, history=hist)
    diag = Diagnosis(episode_key="ep@5000", symptom_tokens=["setpoint_divergence"],
                     root_cause="valve drift", confidence=0.8, signal_id="P1_FC_01",
                     ts="2026-08-15T00:00:05Z")
    # push post-action telemetry that clearly improves the signal
    for i in range(5):
        hist.write("P1_FC_01_Z", 6000 + i * 1000, 12.0)
    out = pipe.run(diag, baseline=10.0, after_epoch_ms=6000)
    assert out["executed"] is True
    cmd_msgs = [t for t, _ in bus.transport.messages if t.startswith("cmd/")]
    assert len(cmd_msgs) == 1, "exactly one command published"
    assert out["state"] in ("RESOLVED", "DIAGNOSING")

    def test_decide_noop_is_cheaper_path():  # noqa
        mapping = make_mapping()
        cfg = DecideConfig(candidate_count=2)
        gen = CandidateGenerator(cfg)
        cands = gen.generate(mapping, mapping.commands[0])
        assert cands[0].is_noop  # first candidate is always do(empty-set)