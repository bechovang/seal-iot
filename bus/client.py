"""MQTT bus client wrapper.

The wrapper is the only place paho-mqtt is touched. It supports a pluggable
``transport`` so tests can run against an in-memory bus without a broker while the
production path uses paho-mqtt against local mosquitto (AD-7 contract).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Iterable

from .envelopes import command_topic, tele_topic, stage_topic

MessageHandler = Callable[[str, dict[str, Any]], None]


class InMemoryBus:
    """In-memory transport for tests and offline runs."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []
        self._subs: dict[str, list[MessageHandler]] = {}
        self.connected = True

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 0) -> None:
        self.messages.append((topic, payload))
        for prefix, handlers in self._subs.items():
            if topic.startswith(prefix):
                for h in handlers:
                    h(topic, payload)

    def subscribe(self, prefix: str, handler: MessageHandler) -> None:
        self._subs.setdefault(prefix, []).append(handler)


class PahoTransport:
    """Thin paho-mqtt transport used against a real broker."""

    def __init__(self, host: str, port: int, client_id: str = "") -> None:
        import paho.mqtt.client as mqtt

        self._mqtt = mqtt
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id or None)
        self._client.connect(host, port)

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 0) -> None:
        self._client.publish(topic, json.dumps(payload), qos=qos)

    def subscribe(self, prefix: str, handler: MessageHandler) -> None:
        def _on_message(m, userdata, msg):
            try:
                data = json.loads(msg.payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return
            handler(msg.topic, data)

        self._client.subscribe(f"{prefix}#")
        self._client.on_message = _on_message
        self._client.loop_start()

    def close(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


class FailingTransport:
    """Transport that simulates BROKER_DOWN: every publish raises ConnectionError.
    Used to prove the adapter degrades gracefully (no crash, no silent source-drop,
    state surfaced) when mosquitto is unreachable (AD-13 degraded-mode discipline)."""

    def __init__(self) -> None:
        self.connected = False

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 0) -> None:
        raise ConnectionError(f"broker unreachable for {topic}")

    def subscribe(self, prefix: str, handler: MessageHandler) -> None:
        pass


class BusClient:
    """High-level facade over a transport conforming to the AD-7 contract."""

    SCHEMA_VERSION = 1

    def __init__(self, transport: Any) -> None:
        self.transport = transport

    def publish_telemetry(
        self, signal_id: str, ts: str, value: float, unit: str = "", quality: str = "ok"
    ) -> None:
        from .envelopes import TelemetryEnvelope

        env = TelemetryEnvelope(signal_id=signal_id, ts=ts, value=value, unit=unit, quality=quality)
        self.transport.publish(tele_topic(signal_id), env.to_dict(), qos=1)

    def publish_event(
        self, stage: str, event: str, source: str, ts: str, payload: dict[str, Any],
        episode_key: str | None = None,
    ) -> None:
        from .envelopes import StageEvent

        env = StageEvent(event=event, source=source, ts=ts, payload=payload, episode_key=episode_key)
        self.transport.publish(stage_topic(stage), env.to_dict(), qos=1)

    def publish_command(self, command_name: str, target: float, ts: str) -> None:
        """Publish an approved actuator command on ``cmd/<command_name>``.
        Callers must not publish to ``cmd/*`` directly — the executor is the sole
        publisher (AD-7/AD-8). The LLM layer has no reference to this method."""
        self.transport.publish(
            command_topic(command_name),
            {"command": command_name, "target": target, "ts": ts},
            qos=1,
        )

    def subscribe(self, prefix: str, handler: MessageHandler) -> None:
        self.transport.subscribe(prefix, handler)


def round_trip_smoke(host: str = "localhost", port: int = 1883, signal_id: str = "P1_FC_01") -> str:
    """Publish and receive one envelope against a real broker; returns received ts."""
    transport = PahoTransport(host, port, client_id="seal-smoke")
    client = BusClient(transport)
    received: list[str] = []
    client.subscribe("tele/", lambda topic, payload: received.append(payload["ts"]))
    client.publish_telemetry(signal_id, "2026-08-15T12:00:00Z", 0.42, "m3/h")
    import time

    for _ in range(50):
        if received:
            break
        time.sleep(0.05)
    transport.close()
    if not received:
        raise RuntimeError("no telemetry echoed from broker — is mosquitto running?")
    return received[0]