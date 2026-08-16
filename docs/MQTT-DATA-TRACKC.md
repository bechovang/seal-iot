# Dữ liệu MQTT Track C — mô tả stream thật (dò ngày 2026-08-16)

> Tài liệu này mô tả **dữ liệu thật** quan sát được trực tiếp từ broker hackathon bằng
> `scripts/probe_mqtt.py` (lần dò 25s + capture 12s + list 15s). Mọi số liệu dưới đây là
> đo được, không phải suy đoán. Credential kết nối nằm trong `mqtt.env` (gitignored).

## 1. Kết nối

| Tham số | Giá trị |
|---|---|
| Host | `mqtt-hackathon.lexatek.vn` |
| Port | `443` |
| Transport | **WebSocket + TLS (wss)** — KHÔNG phải raw TCP |
| WS path | `/mqtt` |
| Username | `UNDERRATED` (có trong `mqtt.env`) |
| Password | trong `mqtt.env`, không đưa vào docs |

paho-mqtt (v2) kết nối như sau:

```python
import paho.mqtt.client as mqtt
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="underrated",
                transport="websockets")
c.username_pw_set(user, password)
c.tls_set()                  # wss
c.ws_set_options(path="/mqtt")
c.connect("mqtt-hackathon.lexatek.vn", 443, keepalive=30)
```

## 2. Topic

- Quy ước: `hackathon/{teamCodeLowercase}/test/telemetry` và `.../judge/telemetry`.
- Topic của đội: **`hackathon/underrated/test/telemetry`** — đang phát dữ liệu thật.
- Broker broadcast stream cho **mọi team** (ACL cho subscribe `#`): thấy ~30+ topic
  `hackathon/<team>/test/telemetry` (winfast, veteran, torii, …). Team FACTORY nhận
  dữ liệu FACTORY giống hệt nhau, team HOME nhận dữ liệu HOME.
- **Chưa thấy topic `judge` nào** trên broker trong 3 lần dò (15s gần nhất chỉ toàn
  `/test/telemetry`). → Khi chấm chính thức BTC mới bật judge topic, hoặc sẽ công bố
  thêm. Cần hỏi lại BTC.

## 3. Cấu trúc payload (contract thật)

Mỗi message là JSON **chứa cả 6 thiết bị trong một mảng `devices`**. Ví dụ NGUYÊN VĂN
bắt từ stream (rút gọn indentation):

```json
{
  "timestamp": "2026-08-16T02:37:10.966Z",
  "epoch": 1786847830,
  "environment": "FACTORY",
  "scenario": "NORMAL",
  "devices": [
    {"deviceCode": "CONVEYOR_01", "status": "ok", "metrics": {"speed": 1.5, "load": 234.5}},
    {"deviceCode": "GAS_01",      "status": "ok", "metrics": {"gas": 251.8}},
    {"deviceCode": "LINE_01",     "status": "ok", "metrics": {"voltage": 403.9, "current": 38.5}},
    {"deviceCode": "MOTOR_01",    "status": "ok", "metrics": {"current": 23.4, "vibration": 4.6, "temperature": 55.7}},
    {"deviceCode": "PRESS_01",    "status": "ok", "metrics": {"pressure": 4.6}},
    {"deviceCode": "PROBE_01",    "status": "ok", "metrics": {"temperature": 52.9}}
  ],
  "teamCode": "UNDERRATED"
}
```

### Bảng field

| Field | Kiểu | Ghi chú (đo được) |
|---|---|---|
| `timestamp` | string ISO-8601 UTC, millisecond | **Khớp `epoch`** trong stream thật (khác sample BTC gửi ban đầu — cái đó lệch ~2h40m) |
| `epoch` | int, **giây** Unix UTC | Nguồn sự thật về thời gian — ta ưu tiên field này khi parse |
| `environment` | string | `"FACTORY"` cho Track C (đã xác nhận thật, hết phải đoán) |
| `scenario` | string | **`"NORMAL"` — field KHÔNG có trong spec thầy gửi**. Chắc chắn sẽ đổi giá trị khi BTC bật kịch bản ( anomalies / sự cố demo) → nên đưa vào `meta` và hiển thị lên UI |
| `teamCode` | string | `"UNDERRATED"` — check khớp rồi mới nhận message |
| `devices` | mảng object | Batch: 1 message = cả 6 thiết bị |
| `devices[].deviceCode` | string UPPERCASE | `MOTOR_01` … — lowercase hóa theo registry khi ingest |
| `devices[].status` | string | Chỉ thấy `"ok"` trong lần dò. Spec cho phép `error` / `offline` → map thẳng vào `quality` envelope (tín hiệu Kịch bản 3) |
| `devices[].metrics` | object metric → number | Tên metric trùng 100% `mapping.yaml: trackc:` |

## 4. Thiết bị & metric thật (khớp registry `mapping.yaml` 6/6, 10/10 signal)

| deviceCode | metrics (kiểu số) | Khoảng giá trị thấy trong 2 lần dò |
|---|---|---|
| `MOTOR_01` | current (A), vibration (mm/s), temperature (°C) | 23–24 A · 4.6 mm/s · ~55.7 °C |
| `LINE_01` | voltage (V), current (A) | ~404 V · ~38 A |
| `CONVEYOR_01` | speed (m/s), load (kg) | 1.3–1.5 m/s · 160–263 kg |
| `PRESS_01` | pressure (bar) | ~4.6 bar |
| `GAS_01` | gas (ppm) | **190–283 ppm** (dao động mạnh — ứng viên tín hiệu sự cố Kịch bản 2) |
| `PROBE_01` | temperature (°C) | ~53 °C |

Lưu ý: sim local (`adapters/trackc_sim.py`) dùng base thấp hơn nhiều (gas 35, line 18A…) —
khi demo bằng stream thật thì ngưỡng/cận cảnh báo nên tính theo dải trên, không theo sim.

## 5. Đặc điểm stream

- **Tần suất ~2 Hz** (27 messages / 15s), mỗi message chứa đủ cả 6 thiết bị.
- Dữ liệu **giống hệt nhau** cho mọi team cùng `environment` (BTC phát chung, mỗi team một topic bản sao).
- **`epoch`/`timestamp` trễ ~4–5 phút so wall-clock** ở lần dò (message đến "bây giờ" nhưng ts trong payload là ~268s trước — có thể BTC phát lại batch trễ hoặc lệch clock). Hệ quả: **không dùng wall-clock để đánh giá stale** — dùng event-time (`ref_ts`) như observer đang làm, hoặc so với **thời điểm ARRIVAL** (lúc nhận message) thì mới đúng.

## 6. Map vào hệ thống seal-iot

```
broker (wss 443)
  └─ TrackCBridge.subscribe "hackathon/underrated/+/telemetry"   (chỉ topic của mình)
       └─ parse_payload(raw, registry)                            adapters/trackc.py
            ├─ epoch → ISO-8601 UTC → TelemetryEnvelope.ts       (AD-9/AD-10)
            ├─ deviceCode → lowercase → signal_id `<dev>_<metric>` (AD-11)
            ├─ status → envelope.quality ("ok" | "error" | "offline")
            └─ environment/scenario/teamCode → ParseResult.meta  (UI + filter)
                 └─ bus.publish_telemetry → tele/<signal_id> → history → observer
```

Việc cần chỉnh trong code trước khi chạy live (đã ghi trong `docs/PLAN-TRACKC.md` + review):

1. `mqtt.env` đang đặt `MQTT_TOPIC=hackathon/underrated/test/telemetry` (full topic) nhưng
   code map vào `topic_prefix` → subscribe sai. Đặt lại `MQTT_TOPIC=hackathon/underrated/`
   hoặc chuẩn hóa trong `settings_from_env`.
2. `TrackCBridge.connect()` chưa bật transport **websockets** (đang default TCP) — với host
   thật bắt buộc `transport="websockets"` + `ws_set_options(path="/mqtt")` như probe.
3. `TrackCBridge.start()` gán `on_message = lambda *a: None` — cần wire
   `on_message → json.loads → ingest_payload` thì đường live mới chạy.
4. Đưa `scenario` vào `meta` của `ParseResult` (hiện bỏ qua).

## 7. Còn cần hỏi BTC

1. Topic `judge` khi nào bật / đội cần làm gì với nó (hiện không tồn tại trên broker)?
2. `scenario` có những giá trị nào ngoài `NORMAL` — semantic từng giá trị?
3. `status` thiết bị `error`/`offline` sẽ trông thế nào — thiết bị rời khỏi mảng `devices`
   hay nằm lại với status đổi?
