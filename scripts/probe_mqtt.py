"""Probe the hackathon MQTT broker: discover topics, capture payloads, infer schema.

Reads connection settings from environment variables, falling back to a
``mqtt.env`` file at the repo root (gitignored) so credentials never land in
shell history or git.

Usage::

    uv run python scripts/probe_mqtt.py                 # 30s discovery
    MQTT_SECONDS=120 uv run python scripts/probe_mqtt.py

Prints: broker connect status, granted subscriptions, per-topic message counts,
sample payloads, union of JSON field names, device codes seen, and data
freshness (now - payload timestamp) so the IoT Observation Agent contract can
be designed against the real stream.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

# Device codes from Track C brief — used to tag what the stream actually carries.
TRACK_C_DEVICES = ["MOTOR_01", "LINE_01", "CONVEYOR_01", "PRESS_01", "GAS_01", "PROBE_01"]


def load_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE parser (no python-dotenv dependency)."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values.setdefault(key.strip(), val.strip())
    return values


def _flatten(obj: Any, prefix: str = "", depth: int = 0, out: dict[str, str] | None = None) -> dict[str, str]:
    """Flatten nested JSON one union-key level: a.b notation, values -> type names."""
    out = out if out is not None else {}
    if depth > 3:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, (dict, list)):
                _flatten(v, key, depth + 1, out)
            else:
                out[key] = type(v).__name__
    elif isinstance(obj, list) and obj:
        _flatten(obj[0], f"{prefix}[]", depth + 1, out)
    return out


def _find_timestamp(fields: dict[str, str], samples: list[dict[str, Any]]) -> tuple[str | None, Any]:
    """Locate the likeliest timestamp field (name match first, value shape second)."""
    for name in fields:
        low = name.lower()
        if "time" in low or low.endswith("ts") or low == "ts" or "stamp" in low:
            for s in samples:
                if s.get(name) is not None:
                    return name, s[name]
    # fall back: numeric epoch-like value in range 1e9..1e13
    for s in samples:
        for k, v in s.items():
            if isinstance(v, (int, float)) and 1e9 <= float(v) <= 1e13:
                return k, v
    return None, None


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):  # Windows cp1252 console guard
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    env = load_env_file(REPO_ROOT / "mqtt.env")
    host = os.environ.get("MQTT_HOST") or env.get("MQTT_HOST", "")
    port = int(os.environ.get("MQTT_PORT") or env.get("MQTT_PORT", "1883"))
    user = os.environ.get("MQTT_USER") or env.get("MQTT_USER", "")
    password = os.environ.get("MQTT_PASS") or env.get("MQTT_PASS", "")
    topic = os.environ.get("MQTT_TOPIC") or env.get("MQTT_TOPIC", "")
    seconds = int(os.environ.get("MQTT_SECONDS", "30"))
    tls = os.environ.get("MQTT_TLS", "").lower() in ("1", "true") or port == 8883

    if not host:
        print("ERROR: MQTT_HOST chua co. Dien host vao mqtt.env hoac set env var.")
        print("       (kiem tra lai de/thread cua BTC xem dia chi broker la gi)")
        return 2

    import paho.mqtt.client as mqtt

    granted: dict[str, int] = {}
    topic_counts: Counter[str] = Counter()
    payloads: dict[str, list[bytes]] = {}
    connected = {"ok": False, "reason": ""}

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            connected["ok"] = True
            # Subscribe wildcard de phat hien topic anh em; broker ACL co the tu choi.
            client.subscribe([(topic, 1), ("#", 0)] if topic else [("#", 0)])
        else:
            connected["reason"] = str(reason_code)

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        for rc in reason_codes:
            granted[getattr(rc, "value", str(rc))] = granted.get(getattr(rc, "value", str(rc)), 0) + 1

    def on_message(client, userdata, msg):
        topic_counts[msg.topic] += 1
        payloads.setdefault(msg.topic, [])
        if len(payloads[msg.topic]) < 5:
            payloads[msg.topic].append(msg.payload)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"probe-{int(time.time())}")
    if user:
        client.username_pw_set(user, password)
    if tls:
        client.tls_set()  # defaults: system CA, no client cert
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    print(f"Connecting to {host}:{port} (tls={tls}, user={user or '-'}) ...")
    try:
        client.connect(host, port, keepalive=30)
    except Exception as exc:  # noqa: BLE001 - probe reports any transport failure
        print(f"CONNECT FAILED: {exc!r}")
        return 1
    client.loop_start()

    deadline = time.time() + seconds
    try:
        while time.time() < deadline:
            time.sleep(0.5)
    finally:
        client.loop_stop()
        client.disconnect()

    print(f"\nconnected={connected['ok']} {connected['reason']}")
    print(f"suback reason-code counts (128 = bi ACL tu choi): {granted or 'none'}")
    total = sum(topic_counts.values())
    print(f"messages in {seconds}s: {total} across {len(topic_counts)} topic(s)")

    devices_seen: set[str] = set()
    for tpc in sorted(topic_counts):
        print(f"\n=== topic: {tpc}  ({topic_counts[tpc]} msgs) ===")
        samples: list[dict[str, Any]] = []
        for raw in payloads[tpc]:
            text = raw.decode("utf-8", errors="replace")
            print(f"  payload: {text[:300]}{'...' if len(text) > 300 else ''}")
            try:
                parsed = json.loads(text)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                samples.append(parsed)
                blob = json.dumps(parsed)
                devices_seen.update(d for d in TRACK_C_DEVICES if d in blob)

        if samples:
            fields: dict[str, str] = {}
            for s in samples:
                fields.update(_flatten(s))
            print(f"  fields ({len(fields)}): {json.dumps(fields, ensure_ascii=False)}")
            ts_field, ts_val = _find_timestamp(fields, samples)
            if ts_field is not None:
                print(f"  timestamp field: {ts_field} = {ts_val!r}")
                try:
                    if isinstance(ts_val, (int, float)):
                        lag = time.time() - float(ts_val) / (1000 if float(ts_val) > 1e12 else 1)
                    else:
                        from datetime import datetime, timezone
                        dt = datetime.fromisoformat(str(ts_val).replace("Z", "+00:00"))
                        lag = time.time() - dt.timestamp()
                    print(f"  freshness: now-ts = {lag:.1f}s {'(OK - stream moi)' if 0 <= lag < 300 else '(CANH BAO: du lieu cu / lech clock)'}")
                except (ValueError, OverflowError):
                    print("  freshness: khong parse duoc timestamp")

    print(f"\ndevices seen in payloads: {sorted(devices_seen) or 'none'} "
          f"({len(devices_seen)}/6 Track C)")
    return 0 if total else 1


if __name__ == "__main__":
    raise SystemExit(main())
