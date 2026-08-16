"""Epic 0: Track C ingress — registry validation + immutable-contract payload
parse/publish + deterministic simulator. All runnable without a broker.

Contract source: organizer spec (2026-08-16). Sample payload shape:
{
  "timestamp": "2026-07-18T07:30:01.000Z", "epoch": 1784369401,
  "environment": "HOME", "teamCode": "HOME_T1",
  "devices": [{"deviceCode": "AC_01", "status": "ok", "metrics": {"power": 1160.4}}]
}
"""

from __future__ import annotations

import pytest

from config import TrackCRegistry, TrackCDevice, TrackCSignal, load_mapping

# The 6 / 10 expected identity from the brief table.
EXPECTED_DEVICES = {
    "motor_01": "Động cơ băng chuyền 1",
    "line_01": "Đường dây chính",
    "conveyor_01": "Băng chuyền 1",
    "press_01": "Cảm biến áp suất lò",
    "gas_01": "Cảm biến khí xưởng",
    "probe_01": "Đầu dò nhiệt lò",
}
EXPECTED_UNITS = {
    "motor_01_current": "A", "motor_01_vibration": "mm/s", "motor_01_temperature": "°C",
    "line_01_voltage": "V", "line_01_current": "A",
    "conveyor_01_speed": "m/s", "conveyor_01_load": "kg",
    "press_01_pressure": "bar", "probe_01_temperature": "°C", "gas_01_gas": "ppm",
}


@pytest.fixture(scope="module")
def registry() -> TrackCRegistry:
    m = load_mapping()
    assert m.trackc is not None, "mapping.yaml must carry a trackc: section"
    return m.trackc


def _contract_sample() -> dict:
    return {
        "timestamp": "2026-07-18T07:30:01.000Z",
        "epoch": 1784369401,
        "environment": "FACTORY",
        "teamCode": "UNDERRATED",
        "devices": [
            {"deviceCode": "MOTOR_01", "status": "ok",
             "metrics": {"current": 12.3, "vibration": 2.5, "temperature": 55.0}},
            {"deviceCode": "LINE_01", "status": "ok",
             "metrics": {"voltage": 400.0, "current": 15.0}},
        ],
    }


def test_registry_loads_six_devices(registry):
    assert set(registry.devices) == set(EXPECTED_DEVICES)
    assert len(registry.all_signal_ids()) == 10
    for did, name in EXPECTED_DEVICES.items():
        assert registry.device(did).name == name
        assert did == did.lower()
    for sid, unit in EXPECTED_UNITS.items():
        assert registry.signal_unit(sid) == unit, f"unit mismatch for {sid}"
    assert registry.validate() == []


def test_registry_rejects_bad_id_and_unknown_relation():
    bad = TrackCRegistry(devices={
        "Motor 01": TrackCDevice("Motor 01", "x", signals=[TrackCSignal("c", "A")]),
        "ok_01": TrackCDevice("ok_01", "y", signals=[TrackCSignal("c", "A")], relates=["ghost"]),
        "agg_01": TrackCDevice("agg_01", "z", members=["ok_01"]),
    })
    errs = bad.validate()
    assert any("not matching" in e for e in errs), errs
    assert any("unknown device" in e for e in errs), errs


def test_parse_contract_sample(registry):
    from adapters.trackc import parse_payload

    res = parse_payload(_contract_sample(), registry)
    assert len(res.envelopes) == 5  # MOTOR(3) + LINE(2)
    by_id = {e.signal_id: e for e in res.envelopes}
    assert by_id["motor_01_current"].value == pytest.approx(12.3)
    assert by_id["motor_01_current"].unit == "A"
    assert by_id["line_01_voltage"].unit == "V"
    # ts from epoch -> ISO-8601 UTC
    assert by_id["motor_01_current"].ts == "2026-07-18T10:10:01Z"
    assert res.meta["environment"] == "FACTORY"
    assert res.meta["teamCode"] == "UNDERRATED"


def test_parse_btc_sample_unknown_device_graceful(registry):
    from adapters.trackc import parse_payload

    btc = {
        "timestamp": "2026-07-18T07:30:01.000Z", "epoch": 1784369401,
        "environment": "HOME", "teamCode": "HOME_T1",
        "devices": [{"deviceCode": "AC_01", "status": "ok",
                     "metrics": {"power": 1160.4, "temperature": 24.7}}],
    }
    res = parse_payload(btc, registry)
    assert res.envelopes == []
    assert any(ref.lower() == "ac_01" for ref, _ in res.skipped), res.skipped


def test_parse_status_maps_to_quality(registry):
    from adapters.trackc import parse_payload

    payload = _contract_sample()
    payload["devices"][0]["status"] = "offline"
    res = parse_payload(payload, registry)
    mot = [e for e in res.envelopes if e.signal_id.startswith("motor_01_")]
    assert mot and all(e.quality == "offline" for e in mot)
    line = [e for e in res.envelopes if e.signal_id.startswith("line_01_")]
    assert line and all(e.quality == "ok" for e in line)


def test_parse_epoch_preferred_over_timestamp(registry):
    from adapters.trackc import parse_payload

    payload = _contract_sample()
    # timestamp lệch so với epoch; epoch => 10:10:01Z phải thắng
    payload["timestamp"] = "2026-07-18T07:30:01.000Z"
    res = parse_payload(payload, registry)
    assert all(e.ts == "2026-07-18T10:10:01Z" for e in res.envelopes)
    # thiếu cả hai timestamp + epoch
    for k in ("epoch", "timestamp"):
        payload[k] = None
    res2 = parse_payload(payload, registry)
    assert all(e.ts == "" and e.quality == "missing_ts" for e in res2.envelopes)


def test_build_payload_exact_contract(registry):
    from adapters.trackc import build_payload

    p = build_payload(
        {"motor_01": {"current": 12.3, "vibration": 2.5},
         "line_01": {"voltage": 400.0}},
        "FACTORY", "UNDERRATED", 1784369401, status={"motor_01": "offline"},
    )
    assert set(p) == {"timestamp", "epoch", "environment", "teamCode", "devices"}
    assert isinstance(p["epoch"], int) and isinstance(p["timestamp"], str)
    assert isinstance(p["devices"], list) and len(p["devices"]) == 2
    dev = {d["deviceCode"] for d in p["devices"]}
    assert dev == {"MOTOR_01", "LINE_01"}  # uppercased
    motor = next(d for d in p["devices"] if d["deviceCode"] == "MOTOR_01")
    assert motor["status"] == "offline" and set(motor["metrics"]) == {"current", "vibration"}
    # immutable contract: no nested re-layer, no extra keys
    assert set(motor) == {"deviceCode", "status", "metrics"}


def test_roundtrip_sim_parse():
    from adapters.trackc_sim import TrackCSim
    from adapters.trackc import parse_payload
    from config import load_mapping

    reg = load_mapping().trackc
    sim = TrackCSim(reg, seed=21)
    payload = sim.tick(7)
    res = parse_payload(payload, reg)
    assert len(res.envelopes) == 10
    # ts = base + 7
    from adapters.trackc_sim import BASE_TS
    from datetime import datetime, timezone
    t0 = datetime.fromtimestamp(BASE_TS + 7, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    assert all(e.ts == t0 for e in res.envelopes)
    assert res.skipped == []


def test_sim_covers_all_devices_deterministic():
    from adapters.trackc_sim import TrackCSim
    from adapters.trackc import parse_payload
    from config import load_mapping

    reg = load_mapping().trackc
    a = TrackCSim(reg, seed=21)
    b = TrackCSim(reg, seed=21)
    seen_sigs = set()
    for k in range(6):
        pa, pb = a.tick(k), b.tick(k)
        seen_sigs.update(e.signal_id for e in parse_payload(pa, reg).envelopes)
        assert pa["epoch"] == 1784369401 + k  # epoch tăng đúng 1s/bước
        assert pa["epoch"] == pb["epoch"]
        assert pa["devices"] == pb["devices"], "same seed -> identical payload"
    assert seen_sigs == set(EXPECTED_UNITS)
    # force_status chạy được
    pc = a.tick(1, force_status={"motor_01": "error"})
    motor = next(d for d in pc["devices"] if d["deviceCode"] == "MOTOR_01")
    assert motor["status"] == "error"