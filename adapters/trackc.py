"""Track C ingress adapter (AD-9): the ONLY component connected to the hackathon
broker. It subscribes, normalizes the immutable contract payload to canonical
``TelemetryEnvelope``s on ``tele/<signal_id>`` (event-``ts`` from the source, never
wall clock), and (in publish mode) emits ready-made contract payloads for the
``judge`` / ``test`` topics. Everything else about the broker — host/port/TLS/auth,
topics, the device registry, units, relations — lives in ``mapping.yaml`` +
```mqtt.env``; nothing is hardcoded here.

The payload schema is a fixed contract from the organizer (2026-08-16): values may
change, the frame may not. Both directions honour it:
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config import TrackCRegistry
from bus.envelopes import TelemetryEnvelope


@dataclass
class ParseResult:
    """Outcome of normalizing one raw contract payload."""

    envelopes: list[TelemetryEnvelope] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (ref, reason)
    meta: dict[str, Any] = field(default_factory=dict)


def _iso_utc(epoch: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_device(code: Any) -> str:
    return str(code or "").strip().lower()


def _normalize_metric(key: Any) -> str:
    return str(key or "").strip().lower()


def _team_prefix(topic: str) -> str:
    """Normalize whatever ``MQTT_TOPIC`` holds to the team topic PREFIX. People
    naturally paste the full topic from the invite (``.../test/telemetry``); the
    code needs ``hackathon/<team>/`` because subscribe uses ``<prefix>+/telemetry``
    and publish builds ``<prefix><env>/telemetry``."""
    t = str(topic or "").strip().strip("/")
    for suffix in ("/test/telemetry", "/judge/telemetry", "/telemetry"):
        if t.lower().endswith(suffix):
            t = t[: -len(suffix)]
            break
    return f"{t}/" if t else ""


def parse_payload(raw: dict, registry: TrackCRegistry) -> ParseResult:
    """Normalize one contract payload to canonical envelopes (AD-9/AD-10/AD-11).

    Rules:
      - ``devices`` is an array; each ``deviceCode`` is UPPERCASE -> registry lowercase.
      - ``status`` (ok|error|offline) maps straight onto the envelope's ``quality``
        (ingest truth — never computed age, AD-10).
      - ts: prefer ``epoch`` (int seconds -> ISO-8601 UTC); fall back to the
        ``timestamp`` string; missing both -> ``ts=""`` with ``quality="missing_ts"``.
      - ``environment`` / ``teamCode`` ride into ``meta``; an unexpected ``teamCode``
        skips the whole message.
      - Unknown device/metric -> ``skipped`` (with reason), never a crash.
    """
    res = ParseResult()
    team = str(raw.get("teamCode", "")).strip()
    res.meta["environment"] = raw.get("environment", "")
    res.meta["teamCode"] = team
    res.meta["scenario"] = str(raw.get("scenario", ""))  # live frame carries NORMAL / anomaly

    # event-time clock: epoch wins (no TZ ambiguity), timestamp string fallback.
    ts, ts_quality = "", "ok"
    epoch = raw.get("epoch")
    if isinstance(epoch, (int, float)):
        ts = _iso_utc(float(epoch))
    else:
        ts_str = raw.get("timestamp")
        if isinstance(ts_str, str) and ts_str:
            try:
                from datetime import datetime, timezone

                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:  # naive string -> read as UTC, never local
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                res.skipped.append(("(message)", "unparseable timestamp"))
                return res
        else:
            ts, ts_quality = "", "missing_ts"

    devices = raw.get("devices", [])
    if not isinstance(devices, list):
        res.skipped.append(("(message)", "devices must be a list"))
        return res

    for dev in devices:
        if not isinstance(dev, dict):
            res.skipped.append(("(message)", "device entry not an object"))
            continue
        dev_id = _normalize_device(dev.get("deviceCode"))
        dev_obj = registry.device(dev_id)
        if dev_obj is None:
            res.skipped.append((dev_id, "unknown device"))
            continue
        status = _normalize_device(dev.get("status", "ok")) or "ok"
        quality = status  # ok | error | offline -> ingest quality
        if ts_quality == "missing_ts":
            quality = "missing_ts"
        metrics = dev.get("metrics", {})
        if not isinstance(metrics, dict):
            res.skipped.append((dev_id, "metrics must be an object"))
            continue
        for metric, value in metrics.items():
            sig = _normalize_metric(metric)
            signal_id = f"{dev_id}_{sig}"
            if signal_id not in dev_obj.signal_ids:
                res.skipped.append((signal_id, "unknown signal"))
                continue
            try:
                fval = float(value)
            except (TypeError, ValueError):
                res.skipped.append((signal_id, "non-numeric value"))
                continue
            res.envelopes.append(
                TelemetryEnvelope(
                    signal_id=signal_id,
                    ts=ts,
                    value=fval,
                    unit=registry.signal_unit(signal_id),
                    quality=quality,
                )
            )
    return res


def build_payload(device_values: dict[str, dict[str, float]], environment: str,
                  team_code: str, epoch: float, status: dict[str, str] | None = None,
                  scenario: str | None = None) -> dict:
    """Emit a payload that EXACTLY matches the immutable contract: correct nested
    shape, ``deviceCode`` uppercased. No added/removed/re-nested fields (the
    organizer forbids changing the frame). ``scenario`` is included only when given —
    the LIVE stream carries it ("NORMAL" observed 2026-08-16) while the written spec
    the organizer shared omits it, so it stays optional here.

    ``device_values`` maps registry ``device_id`` -> {metric: value}. A device absent
    from ``status`` defaults to ``"ok"``.
    """
    devices = []
    for dev_id, metrics in device_values.items():
        devices.append({
            "deviceCode": dev_id.upper(),
            "status": (status or {}).get(dev_id, "ok"),
            "metrics": {k: float(v) for k, v in metrics.items()},
        })
    frame: dict[str, Any] = {
        "timestamp": _iso_utc(epoch),
        "epoch": int(epoch),
        "environment": environment,
        "teamCode": team_code,
        "devices": devices,
    }
    if scenario:
        frame["scenario"] = scenario
    return frame


def settings_from_env(path: str | os.PathLike | None = None) -> dict[str, Any]:
    """Parse ``mqtt.env`` (gitignored, KEY=VALUE) into a settings dict. The broker
    connection settings (host/port/user/pass/tls) + runtime stream knobs live here,
    never in code."""
    cfg: dict[str, Any] = {
        "host": "", "port": 1883, "user": "", "password": "", "tls": False,
        "transport": "tcp", "ws_path": "/mqtt",
        "topic_prefix": "hackathon/underrated/", "env": "test",
        "team": "UNDERRATED", "environment": "FACTORY",
    }
    p = Path(path) if path else Path(__file__).resolve().parent.parent / "mqtt.env"
    vals: dict[str, str] = {}
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            vals.setdefault(k.strip(), v.strip())
    cfg["host"] = os.environ.get("MQTT_HOST") or vals.get("MQTT_HOST", cfg["host"])
    cfg["port"] = int(os.environ.get("MQTT_PORT") or vals.get("MQTT_PORT", cfg["port"]))
    cfg["user"] = os.environ.get("MQTT_USER") or vals.get("MQTT_USER", cfg["user"])
    cfg["password"] = os.environ.get("MQTT_PASS") or vals.get("MQTT_PASS", cfg["password"])
    port = cfg["port"]
    cfg["tls"] = (os.environ.get("MQTT_TLS") or vals.get("MQTT_TLS", "")).lower() in ("1", "true") \
        or port in (8883, 443)
    cfg["transport"] = (os.environ.get("MQTT_TRANSPORT") or vals.get("MQTT_TRANSPORT") or
                         ("websockets" if port == 443 else "tcp"))
    cfg["ws_path"] = os.environ.get("MQTT_WS_PATH") or vals.get("MQTT_WS_PATH", "/mqtt")
    cfg["env"] = os.environ.get("MQTT_ENV") or vals.get("MQTT_ENV", cfg["env"])
    cfg["topic_prefix"] = _team_prefix(
        os.environ.get("MQTT_TOPIC") or vals.get("MQTT_TOPIC", cfg["topic_prefix"]))
    cfg["team"] = os.environ.get("TRACKC_TEAM") or vals.get("TRACKC_TEAM", cfg["team"])
    cfg["environment"] = os.environ.get("TRACKC_ENVIRONMENT") or vals.get("TRACKC_ENVIRONMENT", cfg["environment"])
    return cfg


class TrackCBridge:
    """Sole egress to the hackathon broker (AD-9). Connect + subscribe here, with
    broker auth applied at this one boundary (``username_pw_set`` / ``tls_set``;
    PahoTransport stays credential-free). Publishing is done by the sim; this brid
    re-publishes the parsed telemetry onto the internal bus so the observers work
    against the live stream (loopback)."""

    def __init__(self, registry: TrackCRegistry, settings: dict[str, Any] | None = None) -> None:
        self.registry = registry
        self.settings = settings or settings_from_env()
        self._client = None
        self._bus = None
        self.connected = False

    def connect(self) -> bool:
        """Establish the broker connection (idempotent). Returns True when connected.
        Only this method builds/touches a paho client."""
        if self.connected and self._client is not None:
            return True
        host = self.settings.get("host")
        if not host:
            return False
        import paho.mqtt.client as mqtt

        transport = self.settings.get("transport", "tcp")
        # The organizer broker terminates MQTT-over-WebSocket (wss) on 443; raw TLS
        # TCP connect stays silent there. Force websockets transport for that port.
        if int(self.settings.get("port") or 1883) == 443:
            transport = "websockets"
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="trackc-bridge",
                             transport=transport)
        if self.settings.get("user"):
            client.username_pw_set(self.settings["user"], self.settings.get("password", ""))
        if transport == "websockets":
            client.tls_set()
            client.ws_set_options(path=self.settings.get("ws_path", "/mqtt"))
        elif self.settings.get("tls"):
            client.tls_set()
        # callbacks BEFORE connect()/loop_start(): the CONNACK (and thus _on_connect
        # -> subscribe + heartbeat) can be processed by the network thread the moment
        # loop_start() runs — assigning after would race and silently drop it.
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.connect(host, int(self.settings.get("port", 1883)), keepalive=30)
        client.loop_start()
        self._client = client
        self.connected = True
        return True

    def start(self, bus) -> None:
        """Start the internal-bus re-publisher. ``bus`` is a ``BusClient`` (or the
        InMemoryBus-driven client). Subscribes to the team's test+judge telemetry
        topics; each incoming contract payload is normalized and re-published on
        ``tele/*``. Offline/test harnesses drive :meth:`ingest_payload` directly."""
        self._bus = bus
        # on_message whenever a client exists (start() may run BEFORE connect());
        # _on_connect re-subscribes on every (re)connect, so subscribe here only
        # as an eager arm for the already-connected case.
        if self._client is not None:
            self._client.on_message = self._on_message
            if self.connected:
                self._client.subscribe(f"{self.settings['topic_prefix']}+/telemetry")

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        # paho drops subscriptions across reconnects — re-arm on every (re)connect.
        client.subscribe(f"{self.settings['topic_prefix']}+/telemetry")
        self.connected = True
        self._emit_heartbeat(True)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None) -> None:
        self.connected = False
        self._emit_heartbeat(False)

    def _emit_heartbeat(self, connected: bool) -> None:
        """bridge/heartbeat — sole publisher: the bridge (AD-5 source health chip)."""
        if self._bus is None:
            return
        try:
            self._bus.publish("bridge/heartbeat",
                              {"connected": connected, "ts": _iso_utc(time.time())},
                              qos=1)
        except Exception:  # noqa: BLE001 — heartbeat không được giết callback paho
            pass

    def _on_message(self, client, userdata, msg) -> None:
        """paho callback: one broker frame -> canonical envelopes on the internal
        bus. A malformed frame is skipped, never raised — the broker loop must
        survive bad input (AD-9 quarantine)."""
        try:
            raw = json.loads(msg.payload.decode("utf-8", "replace"))
        except ValueError:
            return
        if not isinstance(raw, dict):
            return
        try:
            self.ingest_payload(raw)
        except Exception:  # noqa: BLE001
            pass

    def ingest_payload(self, raw: dict) -> ParseResult:
        """Normalize a contract payload and (if a bus is bound) publish the canonical
        telemetry onto ``tele/<signal_id>``. Returns the parse result. This is the
        loopback path proving MQTT->internal works (AD-9)."""
        res = parse_payload(raw, self.registry)
        if self._bus is not None:
            for env in res.envelopes:
                try:
                    self._bus.publish_telemetry(
                        env.signal_id, env.ts, env.value, unit=env.unit, quality=env.quality
                    )
                except ConnectionError:
                    pass
        return res

    def publish_payload(self, raw: dict) -> bool:
        """Publish a contract payload to the configured test|judge topic. Returns True
        when the broker accepted it (or when the client is connected)."""
        if not self.connected or self._client is None:
            return False
        topic = f"{self.settings['topic_prefix']}{self.settings.get('env', 'test')}/telemetry"
        self._client.publish(topic, json.dumps(raw), qos=1)
        return True

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # noqa
                pass
        self.connected = False