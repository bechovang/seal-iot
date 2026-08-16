"""Epic 7: control-room dashboard upgrade — backend event richness (WP1/WP2),
Dashboard aggregation + alias merge + SPA service (WP3/WP4), TTS silence (WP5).

9 new tests on top of the 54 existing (total 63).
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _run_capture(log: str, runbook_dir=None, red_team=False, variant="red-team"):
    """Run one demo round capturing every bus event; returns (bus, harness)."""
    import harness_loop
    from bus import InMemoryBus
    from config import HarnessConfig, load_mapping
    from bus.recorder import JsonlRecorder

    h = HarnessConfig.load()
    h.variant.red_team = red_team
    m = load_mapping()
    b = InMemoryBus()
    r = JsonlRecorder(log)
    b.subscribe("", r.hook)
    harness_loop.run_demo(h, m, b, r, variant_tag=variant,
                          round_no=1, runbook_dir=runbook_dir)
    r.close()
    return b, h


def _topics(bus, prefix):
    return [p for t, p in bus.messages if t.startswith(prefix)]


def test_run_demo_publishes_full_topic_set_on_bus(tmp_path):
    import harness_loop  # noqa
    b, _ = _run_capture(str(tmp_path / "e.jsonl"))
    topics = {t for t, _ in b.messages}
    assert any(t.startswith("ops/perceive") for t in topics)
    assert any(t.startswith("ops/diagnose") for t in topics)
    assert any(t.startswith("ops/decide") for t in topics)
    assert any(t.startswith("ops/shield") for t in topics)
    assert any(t.startswith("ops/verify") for t in topics)
    assert any(t.startswith("ops/incident") for t in topics)
    assert any(t.startswith("ops/learn") for t in topics)
    assert any(t.startswith("ops/result") for t in topics)
    assert any(t.startswith("ops/demo") for t in topics)
    assert any(t.startswith("cmd/") for t in topics)
    assert any(t.startswith("tele/") for t in topics)
    # ops/result stays FLAT (regression contract): executed/state at top level
    res = _topics(b, "ops/result")
    assert res and res[0]["executed"] is True
    assert res[0]["state"] == "RESOLVED"
    assert "payload" not in res[0]


def test_run_demo_streams_telemetry(tmp_path):
    b, _ = _run_capture(str(tmp_path / "t.jsonl"))
    tele = _topics(b, "tele/")
    assert len(tele) >= 90, f"expected >=90 tele points, got {len(tele)}"
    for p in tele[:50]:
        assert "ts_ms" in p and "value" in p and "phase" in p


def test_shield_block_demonstrated(tmp_path):
    b, _ = _run_capture(str(tmp_path / "s.jsonl"))
    shields = _topics(b, "ops/shield")
    blocked = [p for p in shields if p.get("allowed") is False
               or (p.get("payload") or {}).get("allowed") is False]
    assert blocked, "expected at least one blocked shield verdict"
    # the rehearsal block must not produce a second cmd/*
    cmds = [t for t, _ in b.messages if t.startswith("cmd/")]
    assert len(cmds) == 1, f"sole executor must publish exactly one cmd/*, got {len(cmds)}"


def test_learn_distills_runbook_and_metric(tmp_path):
    rbd = tmp_path / "runbooks"
    b, _ = _run_capture(str(tmp_path / "l.jsonl"), runbook_dir=rbd)
    learns = _topics(b, "ops/learn")
    assert learns, "expected ops/learn events"
    one = next((p for p in learns if (p.get("payload") or p).get("runbook")), None)
    if one is None:
        one = next((p for p in learns if isinstance(p, dict) and p.get("runbook")), None)
    # find the envelope-wrapped learn that carries a runbook
    files = list(rbd.glob("*.json"))
    assert files, "distillation must persist a runbook json in the demo runbook dir"
    # metric db neighbours the runbook dir
    assert (rbd.parent / "metrics.db").exists()


def test_runbook_path_isolation(tmp_path):
    import harness_loop
    from config import HarnessConfig, load_mapping
    from bus import InMemoryBus
    from bus.recorder import JsonlRecorder

    h = HarnessConfig.load()
    h.runbook.store_path = str(tmp_path / "iso-rb")  # forced isolated test path
    m = load_mapping()
    b = InMemoryBus()
    r = JsonlRecorder(str(tmp_path / "iso.jsonl"))
    b.subscribe("", r.hook)
    # runbook_dir=None -> must NOT stomp harness.runbook.store_path
    harness_loop.run_demo(h, m, b, r, variant_tag="iso")
    r.close()
    assert h.runbook.store_path == str(tmp_path / "iso-rb"), \
        "demo must not overwrite an isolated runbook path when runbook_dir is None"


def test_diagnosis_carries_divergence_and_causal_edges(tmp_path):
    from config import HarnessConfig, load_mapping
    from history import HistoryBuffer
    from perceive import Episode
    from diagnose import Diagnoser
    from knowledge import RunbookStore

    h = HarnessConfig.load()
    h.variant.diagnosis_mode = "rule_only"
    h.runbook.store_path = str(tmp_path / "rb7")
    store = RunbookStore(h.runbook)
    m = load_mapping()
    hist = HistoryBuffer(":memory:")
    d_id = m.pairing[0].setpoint
    z_id = m.pairing[0].feedback
    for i in range(30):
        hist.write(d_id, i * 1000, 12.0)
        hist.write(z_id, i * 1000, 5.0)  # strong D/Z divergence
    diag = Diagnoser(h, m, store, hist, bus=None)
    ep = Episode("e@100", m.columns[0].signal_id, score=4.0, filter_innovation=1.0,
                 window_stats={"n": 30}, ts_epoch_ms=100)
    d = diag.diagnose(ep)
    dd = d.to_dict()
    assert "divergence" in dd and dd["divergence"].get("hypothesis_type")
    assert isinstance(dd["divergence"].get("divergence"), (int, float)) and \
        dd["divergence"]["divergence"] > 0.2, "D/Z drama must push divergence over threshold"
    assert "causal_edges" in dd and isinstance(dd["causal_edges"], list)


def test_dashboard_aggregates_new_topics(tmp_path):
    from ui import Dashboard
    from bus.envelopes import StageEvent

    dash = Dashboard()
    env = lambda stage, payload, ek=None: StageEvent(event=stage, source="t", ts="1970-01-01T00:00:01Z",
                                                     payload=payload, episode_key=ek).to_dict()
    dash.handler("ops/perceive", env("perceive", {"episode_key": "e1", "ts_ms": 1000}, "e1"))
    dash.handler("ops/decide", env("decide", {"incident_id": "i1", "episode_key": "e1",
                                              "winner": {"action": "a", "target": 1},
                                              "comparison": [], "noop_row": {}, "objective": {}}))
    dash.handler("ops/shield", env("shield", {"incident_id": "i1", "action": "a", "target": 99,
                                              "allowed": False, "reason": "r"}))
    dash.handler("ops/verify", env("verify", {"incident_id": "i1", "episode_key": "e1",
                                              "classification": "improved", "baseline": 1.0, "post": 1.4,
                                              "relative_change": 0.4, "expected_effect": 0.1}))
    dash.handler("ops/incident", env("incident", {"incident_id": "i1", "episode_key": "e1", "event": "resolve",
                                                  "from_state": "VERIFYING", "to_state": "RESOLVED"}))
    dash.handler("ops/learn", env("learn", {"incident_id": "i1", "episode_key": "e1",
                                            "runbook": {"id": "rb-1"}, "metric": {"outcome": "improved"}}))
    for i in range(3):
        dash.handler("tele/P1", {"signal_id": "P1", "ts_ms": 1000 + i, "value": 10 + i, "phase": "baseline"})
    snap = dash.snapshot()
    assert "decisions" in snap and snap["decisions"].get("i1")
    assert "shield" in snap and snap["shield"]
    assert "verifies" in snap and snap["verifies"].get("i1")
    assert "fsm" in snap and snap["fsm"].get("i1")
    assert "learn" in snap and snap["learn"].get("i1")
    assert "signals" in snap and snap["signals"].get("P1")
    # legacy keys stay intact
    assert "incidents" in snap and "stage_counts" in snap and "recent" in snap
    assert snap["counters"]["tele"] == 3
    assert snap["counters"]["shield_blocked"] == 1


def test_dashboard_alias_merges_episode_to_incident(tmp_path):
    from ui import Dashboard

    dash = Dashboard()
    dash.handler("ops/perceive", {"episode_key": "e1", "ts_ms": 1000})
    dash.handler("ops/decide", {"incident_id": "i9", "episode_key": "e1", "winner": {},
                                "comparison": [], "noop_row": {}, "objective": {}})
    dash.handler("ops/result", {"incident_id": "i9", "episode_key": "e1", "state": "RESOLVED",
                                "executed": True})
    snap = dash.snapshot()
    ids = {inc["id"] for inc in snap["incidents"]}
    assert "i9" in ids, f"merged incident must be i9, got {ids}"
    inc = next(i for i in snap["incidents"] if i["id"] == "i9")
    assert "perceive" in inc["order"], "perceive (episode-key-only) must merge into incident i9"


def test_serve_ui_returns_spa_and_json(tmp_path):
    import threading
    from ui.dashboard import Dashboard, serve, free_port

    dash = Dashboard()
    dash.handler("tele/P1", {"signal_id": "P1", "ts_ms": 1, "value": 3, "phase": "baseline"})
    port = free_port()
    srv = serve(dash, "127.0.0.1", port)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/") as r:
            html = r.read().decode("utf-8")
            assert 'id="pipeline"' in html
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/?json=1") as r:
            import json
            data = json.loads(r.read().decode("utf-8"))
            assert "signals" in data and data["signals"].get("P1")
    finally:
        srv.shutdown()


def test_tts_silent_for_control_room_topics(tmp_path):
    from ui import VTTSAnnouncer

    clips = tmp_path / "clips"
    clips.mkdir()
    for name in ("incident.mp3", "verify.mp3", "perceive.mp3"):
        (clips / name).write_bytes(b"x")
    played = []
    ann = VTTSAnnouncer(str(clips), play_clip=lambda p: played.append(p.name))
    ann.handler("ops/perceive", {})                 # still announced
    ann.handler("ops/shield", {"allowed": False})   # silent
    ann.handler("ops/learn", {})                    # silent
    ann.handler("ops/result", {})                   # silent
    ann.handler("ops/demo", {})                     # silent
    ann.handler("ops/verify", {})                   # still announced
    assert played == ["perceive.mp3", "verify.mp3"]