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
import sys
from pathlib import Path


def _force_utf8_streams() -> None:
    """Run on Windows consoles with cp936/cp1252 codepage where printing non-ASCII
    (e.g. Vietnamese) raises UnicodeEncodeError. Reconfigure stdout/stderr to UTF-8
    so the loop works without needing PYTHONUTF8=1 in the calling shell."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


from bus import InMemoryBus
from bus.recorder import JsonlRecorder, replay, topics_in
from config import HarnessConfig, load_mapping
from perceive import Episode


def _iso_ms(epoch_ms: int) -> str:
    """Event-time ISO-8601 UTC from a source-assigned epoch-ms (never wall clock)."""
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _tele(bus, mapping, signal_id: str, ts_ms: int, value: float, phase: str = "baseline"):
    """Mirror one hist.write as a flat telemetry event on ``tele/<id>`` so the
    control-room chart draws a real D/Z line (additive; never part of stable maths)."""
    if bus is None:
        return
    sig = mapping.by_id.get(signal_id)
    bus.publish("tele/" + signal_id, {
        "signal_id": signal_id,
        "ts_ms": ts_ms,
        "ts": _iso_ms(ts_ms),
        "value": value,
        "unit": sig.unit if sig else "",
        "quality": "ok",
        "phase": phase,
    })


def seed_history(hist, mapping, bus=None) -> None:
    """Seed D/Z so VERIFY has before/after windows; D diverges from Z (setpoint gap).
    Each write is mirrored to the bus as telemetry when ``bus`` is provided."""
    sig_ids = [s.signal_id for s in mapping.columns] or ["P1_FC_01"]
    sig = sig_ids[0]
    pair = mapping.pair_for(sig)
    d_id = pair.setpoint if pair else f"{sig}_D"
    z_id = pair.feedback if pair else f"{sig}_Z"
    for i in range(30):  # 30s BEFORE window
        v = 10.0 + (8.0 if i >= 18 else 0.0)
        hist.write(sig, i * 1000, v)
        hist.write(d_id, i * 1000, 12.0)
        hist.write(z_id, i * 1000, 12.0)
        if bus is not None:
            _tele(bus, mapping, sig, i * 1000, v, phase="baseline")
            _tele(bus, mapping, d_id, i * 1000, 12.0, phase="baseline")
            _tele(bus, mapping, z_id, i * 1000, 12.0, phase="baseline")
    return sig, d_id, z_id


def run_demo(harness, mapping, bus, recorder, after_baseline: float = 14.0,
             variant_tag: str | None = None, round_no: int = 1, runbook_dir=None):
    from history import HistoryBuffer
    from diagnose import Diagnoser
    from act import GuardedActionPipeline, Action
    from knowledge import RunbookStore
    from learn import MetricStore

    # Red-team activation must happen BEFORE the pipeline is constructed because the
    # Decider reads the flag at construction time.
    if variant_tag == "red-team" or getattr(harness.variant, "red_team", False):
        harness.variant.red_team = True

    hist = HistoryBuffer(":memory:")
    sig, d_id, z_id = seed_history(hist, mapping, bus=bus)

    # Runbook/metric wiring is gated: only a self-learning demo dir persists a runbook
    # wiki + metric db (never stomps an isolated test path). Round 2+ then becomes a
    # runbook-hit, which is exactly the self-learning story.
    store = RunbookStore(harness.runbook)
    metric_store = None
    if runbook_dir is not None:
        harness.runbook.store_path = str(runbook_dir)
        store = RunbookStore(harness.runbook)
        metric_store = MetricStore(str(Path(runbook_dir).parent / "metrics.db"))
    diagr = Diagnoser(harness, mapping, store, hist, bus=None)
    pipe = GuardedActionPipeline(harness, mapping, bus, history=hist,
                                 incident_store=None, runbook_store=store,
                                 metric_store=metric_store)

    # PERCEIVE: a new deviation pushes feedback off setpoint -> Episode emitted
    ep_ts = 30_000
    for i in range(5):
        hist.write(sig, (ep_ts + i * 1000), 10.0 + 8.0)
        hist.write(z_id, (ep_ts + i * 1000), 30.0)  # strong feedback pull -> D/Z evidence
        if bus is not None:
            _tele(bus, mapping, sig, ep_ts + i * 1000, 18.0, phase="anomaly")
            _tele(bus, mapping, z_id, ep_ts + i * 1000, 30.0, phase="anomaly")
    ep = Episode(
        episode_key="demo@30000", signal_id=sig, score=6.2, filter_innovation=2.0,
        window_stats={"mean": 14.0, "std": 2.4, "recent_spike": 1.0},
        ts_epoch_ms=ep_ts,
        detector="isolation_forest"
        if harness.variant.detector == "isolation_forest" else "ema_adwin",
    )
    recorder.write("tele/" + sig, {"signal_id": sig, "ts_ms": ep_ts, "value": 14.0})
    if bus is not None:
        bus.publish("ops/perceive", {
            "episode_key": ep.episode_key, "signal_id": sig,
            "score": ep.score, "variant": harness.variant.detector,
            "ts_ms": ep.ts_epoch_ms, "filter_innovation": ep.filter_innovation,
            "window_stats": ep.window_stats, "detector": ep.detector,
            "ts": _iso_ms(ep_ts),
        })
    else:
        recorder.write("ops/perceive", {"episode_key": ep.episode_key, "signal_id": sig,
                                        "score": ep.score, "variant": harness.variant.detector,
                                        "ts_ms": ep.ts_epoch_ms})

    # DIAGNOSE
    diag = diagr.diagnose(ep)
    if bus is not None:
        bus.publish("ops/diagnose", dict(diag.to_dict(), ts_ms=ep_ts + 2000))
    recorder.write("ops/diagnose", diag.to_dict() if hasattr(diag, "to_dict") else vars(diag))

    # Write a post-action RECOVERY window so VERIFY can classify an improvement.
    recovery_start = ep_ts + 8_000
    for i in range(12):  # 12s AFTER window: signal returns toward setpoint (~16 >= 15.6)
        hist.write(sig, recovery_start + i * 1000, 16.0 + (0.5 if i > 6 else 0.0))
        hist.write(z_id, recovery_start + i * 1000, 12.0)
        if bus is not None:
            _tele(bus, mapping, sig, recovery_start + i * 1000, 16.0 + (0.5 if i > 6 else 0.0),
                  phase="recovery")
            _tele(bus, mapping, z_id, recovery_start + i * 1000, 12.0, phase="recovery")

    # DECIDE -> ACT -> VERIFY (guarded)
    result = pipe.run(diag, baseline=12.0, after_epoch_ms=recovery_start,
                      detection_delay_sec=2.0, rca_latency_sec=3.0,
                      resolution_time_sec=18.0, arm=variant_tag or str(harness.variant))
    if bus is not None:
        bus.publish("ops/result", {"incident_id": result["incident_id"],
                                   "executed": result["executed"],
                                   "outcome": result.get("outcome", {}),
                                   "state": result.get("state", ""),
                                   "variant": variant_tag or str(harness.variant)})
    else:
        recorder.write("ops/result", {"incident_id": result["incident_id"],
                                      "executed": result["executed"],
                                      "outcome": result.get("outcome", {}),
                                      "state": result.get("state", ""),
                                      "variant": variant_tag or str(harness.variant)})

    # Shield-block rehearsal every round (AD-1 must always be visible): attempt an
    # unsafe target -> verdict blocked, no second cmd/* escapes the sole executor.
    try:
        winner = result["decision"]["winner"]
        cmd_rc = mapping.command_for(z_id) or mapping.command_for(d_id)
        if cmd_rc is not None:
            pipe.executor.execute(Action(cmd_rc, cmd_rc.max * 1.5),
                                  _iso_ms(ep_ts + 21_000), result["incident_id"])
    except Exception:
        pass

    # Round meta for the header badge + variant chip.
    if bus is not None:
        bus.publish("ops/demo", {
            "variant": variant_tag or str(harness.variant),
            "round": round_no,
            "red_team": bool(getattr(harness.variant, "red_team", False)),
            "incident_id": result["incident_id"],
            "ts": _iso_ms(ep_ts + 25_000),
        })

    if metric_store is not None:
        metric_store.close() if hasattr(metric_store, "close") else None
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
    ap.add_argument("--watch", type=int, default=0, nargs="?", const=100,
                    help="run the demo in a continuous loop (default 100 rounds) so the\n                    dashboard stays live; Ctrl+C to stop")
    ap.add_argument("--trackc", default="",
                    help="run one Track C coordination task (text) instead of the sim loop; "
                         "e.g. --trackc 'prepare inspection of motor 1'")
    ap.add_argument("--trackc-playbook", default="auto",
                    help="explicit playbook id for --trackc (default: auto-match)")
    ap.add_argument("--trackc-approve", default="all", choices=["all", "first", "deny", "none"],
                    help="approval decisions to make during --trackc")
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

    # AD-14: watch mode doubles as the control-room console — the dashboard server is
    # started with --serve-ui <port> or automatically (default 8765) under --watch.
    if args.serve_ui or args.watch:
        from ui import Dashboard, serve

        dash = Dashboard()
        bus.subscribe("", dash.handler)
        port = args.serve_ui or (8765 if args.watch else 8766)
        srv = serve(dash, "127.0.0.1", port)
        import threading as _th

        _th.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"control-room on http://127.0.0.1:{port}" if args.watch
              else f"dashboard on http://127.0.0.1:{port}")

    if args.trackc:
        # Track C (AD-1..8/12): run ONE request-driven coordination task over the same
        # bus the dashboard consumes. The request path is the judged path; the sim
        # (P-D-S-A) loop above stays untouched. Returns a summary + rendered trail.
        from config import HarnessConfig as _H, load_mapping as _lm, TrackCRegistry
        from history import HistoryBuffer
        from orchestration import TaskStore, Supervisor
        from tools import CMMSSim, ToolPort
        from adapters import TrackCSim, parse_payload

        _reg = _lm().trackc
        _store = TaskStore("orchestration/tasks.db")
        _cmms = CMMSSim("tools/cmms.db")
        _port = ToolPort(_cmms, _reg,
                         lambda tid: _store.get(tid).state if tid else None,
                         db_path="tools/port_keys.db", bus=bus)
        _hist = HistoryBuffer("demo/trackc_history.db")
        _sim = TrackCSim(_reg)

        def _iso_ms(s):
            from datetime import datetime

            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)

        for k in range(20):
            for e in parse_payload(_sim.tick(k), _reg).envelopes:
                _hist.write(e.signal_id, _iso_ms(e.ts), e.value, e.quality)
        _sup = Supervisor(_store, _reg, harness.trackc, bus=bus, port=_port,
                          history=_hist, cmms=_cmms, mode="live")

        pb = args.trackc_playbook if args.trackc_playbook != "auto" else None
        mint = (_sup.ingest_request if pb else _sup.ingest_request)
        r = mint(args.trackc, pb or "prepare_inspection")
        tid = r["task_id"]
        approvals_made = 0
        guard = 0
        while _store.get(tid).state not in ("REPORTED", "PARTIAL", "FAILED", "CANCELLED") and guard < 8:
            st = _store.get(tid).state
            _sup.advance(tid)
            if _store.get(tid).state == "AWAITING_APPROVAL":
                apr = _store.get(tid).approval_request_id
                if args.trackc_approve == "deny" and approvals_made == 0:
                    _sup.handle_approval(tid, {"approval_id": apr, "decision": "DENIED"})
                else:
                    _sup.handle_approval(tid, {"approval_id": apr, "decision": "APPROVED"})
                    approvals_made += 1
            guard += 1
        from orchestration.replay import render_trail
        print(f"Track C task {tid} -> {_store.get(tid).state} "
              f"(work_orders={len(_cmms.lookup('work_orders'))}, "
              f"reports={len(_cmms.lookup('reports'))}, approvals={approvals_made})")
        print("---- replay-as-render (no execution) ----")
        print("\n".join(render_trail(bus.messages)))
        recorder.close()
        return 0

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
        rounds = args.watch if args.watch else 1
        import time as _t
        for i in range(rounds):
            run_demo(harness, mapping, bus, recorder, variant_tag=args.variant or None,
                     round_no=i + 1, runbook_dir=Path("demo") / "runbooks")
            if args.watch:
                print(f"[watch {i + 1}/{rounds}]", end=" ")
                _t.sleep(0.8)
        summary = smoke(args.log)
        print(json_dumps(summary))
        print("demo recorded ->", args.log)
        if args.watch:
            print("watch loop done; control-room kept serving until Ctrl-C")
    if args.watch:
        # keep the process alive so the control-room (daemon thread) keeps serving
        try:
            print("press Ctrl-C to stop")
            while True:
                _t.sleep(1)
        except KeyboardInterrupt:
            srv.shutdown()
    recorder.close()
    return 0


def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


if __name__ == "__main__":
    _force_utf8_streams()
    raise SystemExit(main())