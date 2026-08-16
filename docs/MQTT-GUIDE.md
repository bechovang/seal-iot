# Hướng dẫn sử dụng MQTT Broker (Hackathon Track C)

> Tài liệu này được kiểm chứng với broker thật ngày **2026-08-16** bằng script
> `scripts/probe_mqtt.py` + `adapters/trackc.py`. Mọi thông tin host/port/topic/xác thực
> đều đã chạy được end-to-end.

---

## 1. Thông số kết nối (credentials)

Thông tin nằm trong file **`mqtt.env`** (đã bị gitignore → không bao giờ bị commit):

```
MQTT_HOST=mqtt-hackathon.lexatek.vn
MQTT_PORT=443
MQTT_USER=UNDERRATED
MQTT_PASS=(xem mqtt.env — khong dua secret vao docs/git)
MQTT_TOPIC=hackathon/underrated/test/telemetry
```

| Thành phần | Giá trị | Ghi chú |
|---|---|---|
| **Host** | `mqtt-hackathon.lexatek.vn` | ⚠️ **Phải là `hackathon`** (có đủ chữ "thon"). `mqtt-hackthon...` (BTC ghi thiếu chữ "t") **không resolve được**. |
| **Port** | `443` | **MQTT-over-WebSocket (WSS)**, không phải TLS-TCP thường. |
| **Transport** | WebSocket + TLS (`wss`) | Raw TLS-TCP sẽ *im lặng không kết nối*. |
| **Username** | `UNDERRATED` | Phân biệt hoa thường. |
| **Password** | `(xem mqtt.env — secret khong vao git)` | |
| **TEST Topic** | `hackathon/underrated/test/telemetry` | Kênh telemetry của team (giám khảo phát vào đây). |
| **TEST Key** | `(xem mqtt.env — key da rot trong git history, nho xin BTC rotate)` | Không dùng để kết nối paho; dùng cho cổng TEST của BTC (đánh dấu hành trình demo). |

> **Lưu ý quan trọng:** sau khi đổi host, `flags=websockets` + `tls` được tự nhận diện
> trong `adapters/trackc.py` khi `port == 443`. Không cần đặt thêm gì.

---

## 2. Cách kết nối (điểm chính)

Broker **bắt buộc MQTT-over-WebSocket trên cổng 443**. Trong `paho-mqtt`, để kết nối:

```python
import paho.mqtt.client as mqtt

host = "mqtt-hackathon.lexatek.vn"
port = 443
user = "UNDERRATED"
password = "(xem mqtt.env — khong dua secret vao docs/git)"

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="thu-nghiem-01",            # client_id tự do
    transport="websockets",               # QUAN TRỌNG: phải là websockets
)
client.username_pw_set(user, password)
client.tls_set()                          # bật TLS cho wss
client.ws_set_options(path="/mqtt")       # path của EMQX; "/" cũng chạy được
client.connect(host, port)
```

> Hệ quả cho project: `TrackCBridge.connect()` tự bật `transport="websockets"` + TLS khi
> `port == 443`, nên code demo dùng đúng, không cần chỉnh tay. Script probe
> `scripts/probe_mqtt.py` cũng đã dùng transport này.

---

## 3. Subscribe đọc dữ liệu telemetry

Dữ liệu giám khảo phát lên topic **`hackathon/underrated/test/telemetry`**. Muốn đọc, phải
subscribe **chính xác 1-sóng (single-topic)** hoặc theo prefix:

```python
def on_connect(client, userdata, flags, rc, properties=None):
    client.subscribe("hackathon/underrated/test/telemetry", qos=1)
    # client.subscribe("hackathon/underrated/+/telemetry", qos=1)  # tất cả kênh của team
    # client.subscribe("hackathon/underrated/#", qos=0)            # toàn bộ team

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)   # msg.topic + msg.payload
    print(msg.topic, payload)

client.on_connect = on_connect
client.on_message = on_message
client.loop_start()
```

> **Ghi chú quan trọng về wildcard:** broker **cho phép** subscribe `#` (thấy cả team khác
> như `torii`, `uni17`, `vamos`, `worka_gang`… cùng hàng trên broker). Chỉ dùng để kiểm
> thử/shell; trong chạy chính thức nên lọc đúng topic của đội mình.

### Cấu trúc 1 payload (contract cố định —— frame không được đổi)

```json
{
  "timestamp": "2026-08-16T02:28:10.266Z",
  "epoch": 1786847290,
  "environment": "FACTORY",
  "scenario": "NORMAL",
  "teamCode": "UNDERRATED",
  "devices": [
    {"deviceCode": "CONVEYOR_01", "status": "ok", "metrics": {"speed": 1.3, "load": 328.1}},
    {"deviceCode": "GAS_01",      "status": "ok", "metrics": {"gas": 311.4}},
    {"deviceCode": "LINE_01",     "status": "ok", "metrics": {"voltage": 398.1, "current": 45.1}},
    {"deviceCode": "MOTOR_01",    "status": "ok", "metrics": {"current": 21.7, "vibration": 4, "temperature": 65.5}},
    {"deviceCode": "PRESS_01",    "status": "ok", "metrics": {"pressure": 4.8}},
    {"deviceCode": "PROBE_01",    "status": "ok", "metrics": {"temperature": 59.8}}
  ]
}
```

Đọc - map trong repo:
- `deviceCode` **viết hoa** → registry **viết thường** (`MOTOR_01` → `motor_01_...`).
- `epoch` = unix **giây** (ưu tiên, tránh nhầm múi giờ); `timestamp` = ISO-8601 UTC (fallback).
- `status` (`ok|error|offline`) → chất lượng envelope (`quality`).
- `scenario` = `NORMAL` lúc thường; sẽ đổi (sang `anomaly`…) khi giám khảo đánh giá.
- `metrics` mỗi thiết bị → 10 tín hiệu: `conveyor_01_speed/load, gas_01_gas, line_01_voltage/current,
  motor_01_current/vibration/temperature, press_01_pressure, probe_01_temperature`.

---

## 4. Publish gửi dữ liệu lên broker

Muốn *gửi* (chủ yếu là kênh `judge` để demo hành động đã chấp thuận), publish đúng 1 frame
contract, giữ nguyên cấu trúc 5–6 field cấp cao:

```python
import json

frame = {
    "timestamp": "2026-08-16T10:00:00.000Z",
    "epoch": 1786847290,
    "environment": "FACTORY",
    "scenario": "NORMAL",
    "teamCode": "UNDERRATED",
    "devices": [
        {"deviceCode": "MOTOR_01", "status": "ok",
         "metrics": {"current": 21.0, "vibration": 4.2, "temperature": 66.0}}
    ],
}
client.publish("hackathon/underrated/judge/telemetry", json.dumps(frame), qos=1)
```

- Giữ `deviceCode` viết hoa; `status` hợp lệ; `metrics` giữ đúng tên tín hiệu.
- Không thêm bớt field cấp cao hay bọc thêm tầng (contract cố định).
- Trong repo, dùng `adapters/trackc.build_payload(...)` để tạo frame chuẩn rồi
  `publish_payload()` (kênh `test`/`judge` tuỳ `MQTT_ENV` trong `mqtt.env`).

---

## 5. Chạy thử từ repo (không cần viết code)

```bash
# 1. Điền credentials vào mqtt.env (đã có sẵn).
# 2. Probe: connect + subscribe 10s, in schema, thống kê topic, counters, độ trễ.
MQTT_SECONDS=10 uv run python scripts/probe_mqtt.py

# 3. Kết nối bridge wss + parse contract:
uv run python -c "from config import load_mapping; from adapters import TrackCBridge; \
b=TrackCBridge(load_mapping().trackc); print('connect(wss)=', b.connect()); b.close()"
```

Cấu hình tại `mqtt.env` (nếu cần chuyển qua kênh `judge`, đổi `MQTT_ENV=judge`:
```
MQTT_ENV=test        # kênh publish (test|judge)
MQTT_TRANSPORT=websockets
MQTT_WS_PATH=/mqtt
```

---

## 6. Xử lý lỗi thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `gaierror 11001 getaddrinfo failed` | Sai host | Dùng `mqtt-hackathon.lexatek.vn` (đủ "thon"). |
| Kết nối im lặng, không CONNECT/SUBACK | Dùng TLS-TCP thuần trên 443 | Đổi `transport="websockets"` + `tls_set()`. |
| `Not authorized` khi sub `#` | ACL riêng từng team | Chỉ sub đúng topic team (`hackathon/underrated/...`). |
| Sao chép 1 callback cho nhiều topic | Thiếu lọc | Lọc theo `msg.topic` hoặc sub prefix hẹp `+/telemetry`. |
| Payload parse ra 0 envelope | `deviceCode`/metric lạ, hoặc teamCode lạ | Kiểm `res.skipped`; chỉ parse device có trong `mapping.yaml trackc:`. |