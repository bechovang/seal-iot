"""Demo orchestration loop (Epic 5.1 / AD-14).

Record a synthetic incident end-to-end through the harness to an append-only JSONL
log, then replay that log and assert a smoke predicate. This is the story behind
``make e2e`` and the failure-recovery replay fixture.

Top-level, adversary-free narrative for a recorded run:
  PERCEIVE  -> an anomaly Episode is detected (with D/Z divergence evidence)
  DIAGNOSE  -> runbook miss falls through to RCA (+ rule_only / debate per variant)
  DECIDE    -> candidates (>= 2 real + do(empty)) scored; winner gated by shield
  ACT       -> executor publishes a single cmd/<target> message (sole publisher)
  VERIFY    -> event-time outcome classification; only 'improved' resolves the FSM,
               which then distils a runbook and records a per-incident metric row.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from bus import InMemoryBus
from bus.recorder import JsonlRecorder, replay, topics_in
from config import HarnessConfig, load_mapping
from perceive import Episode


def seed_history(hist, mapping) -> None:
    """Seed D/Z so VERIFY has before/after windows; D diverges from Z (setpoint gap)."""
    sig_ids = [s.signal_id for s in mapping.columns] or ["P1_FC_01"]
    sig = sig_ids[0]
    pair = mapping.pair_for(sig)
    d_id = pair.setpoint if pair else f"{sig}_D"
    z_id = pair.feedback if pair else f"{sig}_Z"
    for i in range(30):  # 30s BEFORE window
        hist.write(sig, i * 1000, 10.0 + (8.0 if i >= 18 else 0.0))
        hist.write(d_id, i * 1000, 12.0)
        hist.write(z_id, i * 1000, 12.0)
    return sig, d_id, z_id


def run_demo(harness, mapping, bus, recorder, after_baseline: float = 14.0,
             variant_tag: str | None = None):
    from history import HistoryBuffer
    from diagnose import Diagnoser
    from act import GuardedActionPipeline
    from knowledge import RunbookStore

    hist = HistoryBuffer(":memory:")
    sig, d_id, z_id = seed_history(hist, mapping)
    harness.runbook.store_path = str(Path("demo") / "runbooks")
    store = RunbookStore(harness.runbook)
    diagr = Diagnoser(harness, mapping, store, hist, bus=None)
    pipe = GuardedActionPipeline(harness, mapping, bus, history=hist,
                                 incident_store=None)

    # PERCEIVE: a new deviation pushes feedback off setpoint -> Episode emitted
    ep_ts = 30_000
    for i in range(5):
        hist.write(sig, (ep_ts + i * 1000), 10.0 + 8.0)
        hist.write(z_id, (ep_ts + i * 1000), 14.0)  # feedback pulls toward anomaly
    ep = Episode(
        episode_key="demo@30000", signal_id=sig, score=6.2, filter_innovation=2.0,
        window_stats={"mean": 14.0, "std": 2.4, "recent_spike": 1.0},
        ts_epoch_ms=ep_ts,
        detector="isolation_forest"
        if harness.variant.detector == "isolation_forest" else "ema_adwin",
    )
    recorder.write("tele/" + sig, {"signal_id": sig, "ts_ms": ep_ts, "value": 14.0})
    recorder.write("ops/perceive", {"episode_key": ep.episode_key, "signal_id": sig,
                                    "score": ep.score, "variant": harness.variant.detector,
                                    "ts_ms": ep.ts_epoch_ms})

    # DIAGNOSE
    diag = diagr.diagnose(ep)
    recorder.write("ops/diagnose", diag.to_dict() if hasattr(diag, "to_dict") else vars(diag))

    # Write a post-action RECOVERY window so VERIFY can classify an improvement.
    recovery_start = ep_ts + 8_000
    for i in range(12):  # 12s AFTER window: signal returns toward setpoint (~16 >= 15.6)
        hist.write(sig, recovery_start + i * 1000, 16.0 + (0.5 if i > 6 else 0.0))
        hist.write(z_id, recovery_start + i * 1000, 12.0)

    # DECIDE -> ACT -> VERIFY (guarded)
    result = pipe.run(diag, baseline=12.0, after_epoch_ms=recovery_start,
                      detection_delay_sec=2.0, rca_latency_sec=3.0,
                      resolution_time_sec=18.0, arm=variant_tag or str(harness.variant))
    recorder.write("ops/result", {"incident_id": result["incident_id"],
                                  "executed": result["executed"],
                                  "outcome": result.get("outcome", {}),
                                  "state": result.get("state", ""),
                                  "variant": variant_tag or str(harness.variant)})
    diagr.close() if getattr(diagr, "close", None) else None
    return result


def smoke(path) -> dict:
    """Assert a recorded JSONL log represents a complete loop. Returns summary counts."""
    dia = topics_in(path, "ops/diagnose")
    res = topics_in(path, "ops/result")
    cmds = topics_in(path, "cmd/")
    tel = topics_in(path, "tele/")
    per = topics_in(path, "ops/perceive")
    assert per, "no PERCEIVE event recorded"
    assert dia, "no DIAGNOSE event recorded"
    assert cmds, "no command executed in recorded log"
    assert res and res[0]["executed"] is True, "guarded action did not execute"
    return {"perceive": len(per), "diagnose": len(dia), "commands": len(cmds),
            "telemetry": len(tel), "incident_id": res[0]["incident_id"]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SEAL demo loop (record/replay a demo incident)")
    ap.add_argument("--log", default="demo/e2e.jsonl")
    ap.add_argument("--replay", action="store_true")
    ap.add_argument("--variant", default="", help="optional variant tag recorded into the log")
    ap.add_argument("--serve-ui", type=int, default=0,
                    help="serve the subscribe-only dashboard on this port (5.2)")
    ap.add_argument("--tts", action="store_true", help="enable the Vietnamese TTS announcer (5.3)")
    args = ap.parse_args(argv)

    harness = HarnessConfig.load()
    mapping = load_mapping()
    bus = InMemoryBus()

    recorder = JsonlRecorder(args.log)
    bus.subscribe("", recorder.hook)  # empty prefix -> capture every bus event

    if args.tts:
        from ui import VTTSAnnouncer

        ann = VTTSAnnouncer(harness.demo.tts_clips_dir)
        bus.subscribe("", ann.handler)

    if args.serve_ui:
        from ui import Dashboard, serve

        dash = Dashboard()
        bus.subscribe("", dash.handler)
        port = args.serve_ui
        srv = serve(dash, "127.0.0.1", port)
        import threading as _th

        _th.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"dashboard on http://127.0.0.1:{port}")

    if args.replay:
        # AD-14: feed the recorded log into a handler; NEVER append to the source log
        # while reading it (that self-grows unboundedly). Write to a distinct replay log.
        replay_out = str(args.log) + ".replay"
        _r = JsonlRecorder(replay_out)
        n = replay(args.log, lambda t, p, r=_r: r.write("replay/" + t, p))
        _r.close()
        summary = smoke(args.log)
        print(json_dumps(summary))
        print(f"replayed {n} lines from {args.log}; smoke PASS")
    else:
        run_demo(harness, mapping, bus, recorder, variant_tag=args.variant or None)
        bus.close() if hasattr(bus, "close") else None
        summary = smoke(args.log)
        print(json_dumps(summary))
        print("demo recorded ->", args.log)
    recorder.close()
    return 0


def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


if __name__ == "__main__":
    raise SystemExit(main())