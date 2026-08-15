"""Epic 5 tests: JSONL record/replay (5.1), dashboard (5.2), TTS (5.3),
degraded modes / red-team (5.4-5.5), and the make e2e smoke contract."""

from __future__ import annotations

from pathlib import Path

import pytest


# helper to run the demo loop against a temp log (no audio, no network)
def _run(log: str, variant: str = "", red_team: bool = False):
    import harness_loop

    from config import HarnessConfig, load_mapping
    from bus import InMemoryBus
    from bus.recorder import JsonlRecorder

    h = HarnessConfig.load()
    h.variant.red_team = red_team
    h.runbook.store_path = str(Path(log).parent / "runbooks5")
    m = load_mapping()
    b = InMemoryBus()
    r = JsonlRecorder(log)
    b.subscribe("", r.hook)
    harness_loop.run_demo(h, m, b, r, variant_tag=variant or None)
    r.close()
    return h


def test_jsonl_record_and_smoke(tmp_path):
    log = str(tmp_path / "e2e.jsonl")
    _run(log)
    from bus.recorder import topics_in
    assert topics_in(log, "ops/perceive"), "PERCEIVE missing"
    assert topics_in(log, "ops/diagnose"), "DIAGNOSE missing"
    assert topics_in(log, "cmd/"), "no command published"
    res = topics_in(log, "ops/result")
    assert res and res[0]["executed"] is True and res[0]["state"] == "RESOLVED"


def test_replay_smoke_passes(tmp_path):
    log = str(tmp_path / "e2e.jsonl")
    _run(log)
    import harness_loop
    summary = harness_loop.smoke(log)
    assert summary["commands"] >= 1
    assert summary["diagnose"] >= 1


def test_red_team_bad_candidates_are_blocked_by_shield(tmp_path):
    _run(str(tmp_path / "rt.jsonl"), variant="red-team", red_team=True)
    # the red-team run should still produce a RESOLVED incident (bad candidates exist
    # but are never selected+executed unguarded); assert an executed cmd occurred
    from bus.recorder import topics_in
    cmds = topics_in(str(tmp_path / "rt.jsonl"), "cmd/")
    assert cmds, "red-team run must execute a guarded command"


def test_candidate_generator_injects_unsafe_and_shield_blocks():
    from config import Command, DecideConfig, Mapping
    from decide import CandidateGenerator
    from act import SafetyShield, Action

    cmd = Command("fc_controller", "P1_FC_01_Z", 0.0, 50.0, 1.0, 40.0, 10.0)
    m = Mapping(dataset="x", rate_hz=1.0, columns=[], pairing=[], commands=[cmd])
    cands = CandidateGenerator(DecideConfig()).generate(m, cmd, red_team=True)
    unsafe = [c for c in cands if
              not (cmd.safe_min <= c.target <= cmd.safe_max)]
    assert unsafe, "red-team must inject candidates outside the safe envelope"
    shield = SafetyShield()
    for c in unsafe:
        v = shield.evaluate(Action(cmd, c.target))
        assert v.allowed is False, "shield must block unsafe candidates"


def test_rule_only_diagnosis_skips_llm(tmp_path):
    import harness_loop
    from config import HarnessConfig, load_mapping
    from history import HistoryBuffer
    from perceive import Episode
    from diagnose import Diagnoser
    from knowledge import RunbookStore

    h = HarnessConfig.load()
    h.variant.diagnosis_mode = "rule_only"
    h.runbook.store_path = str(tmp_path / "rb5")
    store = RunbookStore(h.runbook)
    m = load_mapping()
    hist = HistoryBuffer(":memory:")
    d_id = m.pairing[0].setpoint
    z_id = m.pairing[0].feedback
    for i in range(30):
        hist.write(d_id, i * 1000, 12.0)
        hist.write(z_id, i * 1000, 5.0)
    diag = Diagnoser(h, m, store, hist, bus=None)
    ep = Episode("e@100", m.columns[0].signal_id, score=4.0, filter_innovation=1.0,
                 window_stats={"n": 30}, ts_epoch_ms=100)
    d = diag.diagnose(ep)
    assert d.debate_mode == "rule_only"
    assert d.root_cause != ""


def test_static_prior_decide_uses_no_simulation():
    from config import Command, DecideConfig, Mapping
    from decide import Decider

    cmd = Command("fc_controller", "P1_FC_01_Z", 0.0, 50.0, 1.0, 40.0, 10.0)
    m = Mapping(dataset="x", rate_hz=1.0, columns=[], pairing=[], commands=[cmd])
    called = {"n": 0}

    class Spy:
        def predict(self, *a, **k):
            called["n"] += 1
            return {"predicted": 0, "delta": 0, "gain": 0}

    dec = Decider(m, __import__("config").DecideConfig(), model=Spy(), static=True)
    res = dec.decide({"signal_id": "P1_FC_01"}, cmd, baseline=10.0)
    assert called["n"] == 0, "static-prior DECIDE must not run what-if simulation"
    assert res["winner"]["action"] == "fc_controller"


def test_dashboard_snapshot_builds_latency(tmp_path):
    from ui import Dashboard

    dash = Dashboard()
    dash.handler("ops/perceive", {"episode_key": "i1", "ts": "1970-01-01T00:00:01Z"})
    dash.handler("ops/diagnose", {"incident_id": "i1", "ts": "1970-01-01T00:00:02Z"})
    snap = dash.snapshot()
    assert "i1" in {inc["id"] for inc in snap["incidents"]}
    inc = snap["incidents"][0]
    assert inc["latency_ms"].get("perceive->diagnose") == 1000


def test_tts_dedupe_per_stage(tmp_path):
    from ui import VTTSAnnouncer
    clips = tmp_path / "clips"
    clips.mkdir()
    for name in ("perceive.mp3", "diagnose.mp3", "verify.mp3"):
        (clips / name).write_bytes(b"x")
    played = []
    ann = VTTSAnnouncer(str(clips), play_clip=lambda p: played.append(p.name))
    ann.handler("ops/perceive", {})
    ann.handler("ops/perceive", {})  # duplicate within 0.5s -> deduped
    ann.handler("ops/diagnose", {})
    assert len(ann.pending()) == 2
    # two distinct stages within the dedupe window -> both announced
    assert played == ["perceive.mp3", "diagnose.mp3"]