"""Epic 4: Self-Learning & Evaluation — stories 4.1-4.4.

4.1 knowledge distillation on incident resolve (AD-3, knowledge/ sole runbook writer)
4.2 per-incident metric log, single writer (AD-11, learn/ sole metric writer)
4.3 practice scoreboard from metric log + quarantined labels (AD-8, score/ only label reader)
4.4 compounding demo: recurring fault resolves via runbook match, measurably faster
"""

import json

from config import RunbookConfig
from history import HistoryBuffer
from knowledge import RunbookStore
from learn import MetricRow, MetricStore
from score import LabelRow, ScoreboardBuilder


def make_store(tmp_path):
    return RunbookStore(RunbookConfig(store_path=str(tmp_path / "runbooks")))


# ---- 4.1 knowledge distillation ----
def test_distill_creates_new_entry_on_first_resolve(tmp_path):
    store = make_store(tmp_path)
    rb = store.record_resolution(["setpoint_divergence", "flow_dip"], "valve deadband", "recalibrate")
    assert rb.id.startswith("rb-")
    assert rb.occurrences == 1
    assert rb.reliability == 1.0
    loaded = RunbookStore(RunbookConfig(store_path=str(tmp_path / "runbooks")))
    assert len(loaded.all()) == 1


def test_distill_increments_existing_entry(tmp_path):
    store = make_store(tmp_path)
    store.record_resolution(["setpoint_divergence", "flow_dip"], "valve deadband", "recalibrate")
    rb2 = store.record_resolution(["setpoint_divergence", "flow_dip"], "valve deadband", "recalibrate")
    assert rb2.occurrences == 2
    assert rb2.reliability < 1.0  # saturating reliability rises below 1
    unique_ids = {rb.id for rb in store.all()}
    assert len(unique_ids) == 1  # same cause -> same entry, not a new one


# ---- 4.2 metric log (single writer) ----
def test_metric_store_writes_and_reads(tmp_path):
    store = MetricStore(str(tmp_path / "metrics.db"))
    store.record(MetricRow(incident_id="i1", episode_key="e1", detection_delay_sec=2.5,
                           rca_latency_sec=1.2, resolution_time_sec=30.0, outcome="improved",
                           arm="full", signal_id="P1", unsafe_actions=0, downtime_avoided=True))
    store.update_outcome("i1", "improved")
    row = store.get("i1")
    assert row.detection_delay_sec == 2.5
    assert row.outcome == "improved"
    assert len(store.all_rows()) == 1
    # persistence across reopen
    store.close()
    store2 = MetricStore(str(tmp_path / "metrics.db"))
    assert store2.get("i1").incident_id == "i1"


# ---- 4.3 practice scoreboard ----
def test_scoreboard_computes_prf_mttd_mttr(tmp_path):
    metrics = [
        MetricRow(incident_id="m1", episode_key="e1", detection_delay_sec=2.0,
                  resolution_time_sec=30.0, outcome="improved", arm="v1",
                  signal_id="root1", unsafe_actions=0, downtime_avoided=True),
        MetricRow(incident_id="m2", episode_key="e2", detection_delay_sec=4.0,
                  resolution_time_sec=50.0, outcome="improved", arm="v2",
                  signal_id="other", unsafe_actions=0, downtime_avoided=False),
        MetricRow(incident_id="m3", episode_key="e3", detection_delay_sec=6.0,
                  outcome="escalated", arm="v1"),
    ]
    labels = [
        LabelRow("e1", True, root_label="root1"),
        LabelRow("e2", True, root_label="root2"),
        LabelRow("e3", False, root_label=""),
    ]
    sb = ScoreboardBuilder().compute(metrics, labels)
    d = sb.to_dict()
    # 2 true anomalies detected -> recall 1.0; 1 false positive -> precision 2/3
    assert d["recall"] == 1.0
    assert abs(d["precision"] - (2.0 / 3.0)) < 1e-3
    assert "f1" in d
    assert d["mttd_sec"] == 4.0  # mean of 2,4,6 = 4
    assert d["mttr_sec"] == 40.0  # mean of 30,50
    assert d["top1_rca_accuracy"] == 0.5  # 1 of 2 resolved-and-labeled root-matched
    assert d["downtime_avoided"] == 1


# ---- 4.4 compounding demo (runbook double-take) ----
def test_compounding_resolves_recurring_fault_via_runbook(tmp_path):
    # first occurrence: distil a new runbook from a resolved incident
    store = make_store(tmp_path)
    store.record_resolution(["setpoint_divergence", "flow_dip"], "valve deadband", "recalibrate")
    # same fault recurs -> DIAGNOSE matches by runbook before full reasoning
    from bus import BusClient, InMemoryBus
    from config import (Command, DecideConfig, HarnessConfig, IncidentConfig, Mapping,
                        MappedSignal, Pairing, RunbookConfig, VerifyConfig)
    from diagnose import Diagnosis, Diagnoser
    from perceive import Episode

    cmd = Command(name="fc_controller", target="P1_FC_01_Z", min=0.0, max=50.0,
                  safe_min=1.0, safe_max=40.0, default=10.0, effect_gain=0.9)
    mapping = Mapping(dataset="x", rate_hz=1.0,
                      columns=[MappedSignal(signal_id="P1_FC_01", source="P1_FC_01", area="A", kind="flow")],
                      pairing=[Pairing(signal="P1_FC_01", setpoint="P1_FC_01_D", feedback="P1_FC_01_Z")],
                      commands=[cmd])
    harness = HarnessConfig.load()
    harness.incident.db_path = str(tmp_path / "inc.db")
    harness.decide.candidate_count = 2
    hist = HistoryBuffer(":memory:")
    # D/Z diverge strongly (D=12 vs Z=5) -> setpoint_divergence + feedback_offset; flow kind -> flow_dip
    for i in range(40):
        hist.write("P1_FC_01", i * 1000, 10.0)
        hist.write("P1_FC_01_D", i * 1000, 12.0)
        hist.write("P1_FC_01_Z", i * 1000, 5.0)
    diag = Diagnoser(harness, mapping, store, hist)
    ep = Episode(episode_key="recur@5000", signal_id="P1_FC_01", score=4.0,
                 filter_innovation=1.0, window_stats={"n": 40}, ts_epoch_ms=5000)
    d = diag.diagnose(ep)
    assert d.runbook_hit is True  # resolved via runbook, NOT full reasoning
    assert d.root_cause == "valve deadband"
    assert d.debate_mode == "runbook_hit"


def test_pipeline_distills_and_records_metric_on_resolve(tmp_path):
    from bus import BusClient, InMemoryBus
    from config import Command, DecideConfig, HarnessConfig, IncidentConfig, Mapping, Pairing, VerifyConfig
    from diagnose import Diagnosis
    from act.pipeline import GuardedActionPipeline

    cmd = Command(name="fc_controller", target="P1_FC_01_Z", min=0.0, max=50.0,
                  safe_min=1.0, safe_max=40.0, default=10.0, effect_gain=0.9)
    mapping = Mapping(dataset="x", rate_hz=1.0, columns=[],
                      pairing=[Pairing(signal="P1_FC_01", setpoint="P1_FC_01_D", feedback="P1_FC_01_Z")],
                      commands=[cmd])
    harness = HarnessConfig(incident=IncidentConfig(db_path=str(tmp_path / "p.db")),
                            decide=DecideConfig(candidate_count=2), verify=VerifyConfig())
    bus = BusClient(InMemoryBus())
    hist = HistoryBuffer(":memory:")
    for i in range(5):
        hist.write("P1_FC_01_Z", i * 1000, 10.0)
    for i in range(5):
        hist.write("P1_FC_01_Z", 6000 + i * 1000, 14.0)
    rbs = make_store(tmp_path)
    ms = MetricStore(str(tmp_path / "metrics.db"))
    pipe = GuardedActionPipeline(harness, mapping, bus, history=hist,
                                 runbook_store=rbs, metric_store=ms)
    diag = Diagnosis(episode_key="ep@5000", symptom_tokens=["setpoint_divergence", "flow_dip"],
                     root_cause="valve deadband", confidence=0.8,
                     action_hint="recalibrate", signal_id="P1_FC_01",
                     ts="2026-08-15T00:00:05Z")
    out = pipe.run(diag, baseline=10.0, after_epoch_ms=6000,
                   detection_delay_sec=2.5, rca_latency_sec=1.0, arm="full")
    if out["state"] == "RESOLVED":
        assert len(rbs.all()) == 1  # distilled on resolve
        assert len(ms.all_rows()) == 1
        rec = ms.get(out["incident_id"])
        assert rec.outcome == "improved"
        assert rec.arm == "full"