"""Rehearsal helper: publish bad-status frames for ONE device to the TEAM TEST
topic so the live control room (``--trackc-live``) shows a full auto-loop cycle
(incident feed -> task -> approval gate -> REPORTED -> tool audit).

Rehearsal ONLY — never point this at the judge topic during scoring.

Usage: uv run python demo/inject_offline.py [device] [status] [seconds]
       defaults: motor_01 offline 12
"""
from __future__ import annotations

import sys
import time

from config import load_mapping
from adapters.trackc import TrackCBridge, build_payload, settings_from_env


def main() -> int:
    dev = sys.argv[1] if len(sys.argv) > 1 else "motor_01"
    status = sys.argv[2] if len(sys.argv) > 2 else "offline"
    secs = float(sys.argv[3]) if len(sys.argv) > 3 else 12.0
    reg = load_mapping().trackc
    d = reg.device(dev)
    if d is None or d.is_aggregate:
        plain = [k for k, x in reg.devices.items() if not x.is_aggregate]
        print(f"thiet bi {dev!r} khong ton tai / la aggregate. Chon mot trong: {plain}")
        return 2
    br = TrackCBridge(reg, settings_from_env())
    if not br.connect():
        print("broker connect FAIL — kiem tra mqtt.env")
        return 1
    vals = {k: {s.metric: 1.0 for s in x.signals}
            for k, x in reg.devices.items() if not x.is_aggregate}
    topic = f"{br.settings['topic_prefix']}{br.settings['env']}/telemetry"
    print(f"inject status={status} cho {dev} trong {secs:.0f}s -> {topic}")
    t0 = time.time()
    while time.time() - t0 < secs:
        br.publish_payload(build_payload(
            vals, br.settings["environment"], br.settings["team"],
            time.time(), status={dev: status}))
        time.sleep(0.5)
    br.close()
    print("done — xem Incident feed trong control room")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
