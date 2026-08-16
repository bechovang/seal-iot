# Kế hoạch build Track C — Multi-Agent Factory Coordination

> **Nguồn thật là spine:** `docs/ARCHITECTURE-SPINE-TRACKC.md` (bản final 2026-08-16; gốc +
> memlog + 5 review ở `_bmad-output/planning-artifacts/architecture/architecture-seal-iot-2026-08-16/` — không commit).
> 13 AD + 9 AD kế thừa. Plan này chỉ định **thứ tự triển khai + chi tiết seed**; mọi chỗ mâu thuẫn → spine thắng.

## Nguyên tắc chung

- **Test trước** theo pattern repo: `tests/test_epic*.py`, fixture `InMemoryBus` + `BusClient`, `load_mapping()` từ `mapping.yaml` thật.
- Mọi lệnh chạy với `PYTHONUTF8=1` (console Windows cp1252).
- **Không thêm dependency** — paho-mqtt / sqlite3 / PyYAML / httpx có sẵn.
- Mỗi epic xong: `PYTHONUTF8=1 make test` xanh → commit.
- Đường được chấm là **request-driven**; auto-spawn là phần cộng thêm (AD-1: request path chạy được khi auto-loop chết).

---

## Epic 0 — Ingress (không cần broker host)

**Mục tiêu:** registry chuẩn + parser + simulator phát telemetry 6/6 thiết bị lên `tele/*`.

### 0.1 `mapping.yaml` — thêm section `trackc:` (cuối file)

```yaml
# ─── Track C stream (spine 2026-08-16: AD-9/AD-11) ───
# Registry thiết bị CHUẨN — device_id lowercase snake; signal_id = <device_id>_<metric>.
# relations = adjacency heuristic cho "thiết bị liên quan" (Kịch bản 1).
# Payload schema broker chưa biết — chỉ adapters/trackc.py được đụng vào.
trackc:
  topic_prefix: "hackathon/underrated/"
  devices:
    motor_01:
      name: "Động cơ băng chuyền 1"
      signals: [{metric: current, unit: 'A'}, {metric: vibration, unit: 'mm/s'}, {metric: temperature, unit: '°C'}]
      relates: [conveyor_01, line_01]
    conveyor_01:
      name: "Băng chuyền 1"
      signals: [{metric: speed, unit: 'm/s'}, {metric: load, unit: 'kg'}]
      relates: [motor_01]
    line_01:
      name: "Đường dây chính"
      signals: [{metric: voltage, unit: 'V'}, {metric: current, unit: 'A'}]
      relates: [motor_01]
    press_01:
      name: "Cảm biến áp suất lò"
      signals: [{metric: pressure, unit: 'bar'}]
      relates: [probe_01]
    probe_01:
      name: "Đầu dò nhiệt lò"
      signals: [{metric: temperature, unit: '°C'}]
      relates: [press_01, gas_01]
    gas_01:
      name: "Cảm biến khí xưởng"
      signals: [{metric: gas, unit: 'ppm'}]
      relates: [press_01, probe_01]
```

### 0.2 `config.py` — thêm 3 dataclass + loader

```python
@dataclass
class TrackCSignal:        # metric + unit
@dataclass
class TrackCDevice:        # device_id, name, members (aggregate, để trống), signals, relates
    @property
    def signal_ids(self) -> list[str]: ...   # f"{device_id}_{metric}"
class TrackCRegistry:      # devices: dict[str, TrackCDevice], topic_prefix
```

API: `device(id)`, `device_for_signal(signal_id)`, `related(device_id)`, `all_signal_ids()`, `signal_unit(signal_id)`.
`validate()` trả list lỗi: id không khớp `^[a-z0-9_]+$`; metric trùng trong 1 device; `relates`/`members` trỏ device không có; device thường mà 0 signal.
Nạp trong `load_mapping()`: `m.trackc = TrackCRegistry(...) if "trackc" in cfg else None`; lỗi validate raise như mapping lỗi hiện tại.

### 0.3 `adapters/trackc.py`

- `parse_payload(raw: dict, registry) -> ParseResult(envelopes, skipped, missing_ts)`:
  - **Shape A** (1 device): `{"device": "MOTOR_01", "ts": "...", "metrics": {"current": 12.3}}` — chấp nhận alias `values`/`data` cho metrics, `time`/`timestamp` cho ts.
  - **Shape B** (nhiều device): `{"ts": "...", "MOTOR_01": {"current": 12.3}}`.
  - Device/metric **case-insensitive** → chuẩn hóa lowercase theo registry.
  - ts numeric (epoch s/ms) → ISO-8601 UTC; string → passthrough; **thiếu ts → `ts=""`, `quality="missing_ts"`** (không bao giờ dùng wall clock — AD-9/10).
  - Device/metric lạ → bỏ vào `skipped` (có lý do), không crash.
- `TrackCSource(registry, host, port, username, password, topic, tls)` — **nơi duy nhất** gọi `username_pw_set()`/`tls_set()` (paho trực tiếp, `PahoTransport` giữ nguyên). `start(bus)` subscribe topic (mặc định `<topic_prefix>test/telemetry`, lấy từ `mqtt.env`), `on_message` → `parse_payload` → `bus.publish_telemetry(...)`.
- `settings_from_env(path)` đọc `mqtt.env` (parser KEY=VALUE tối giản ~10 dòng).

### 0.4 `adapters/trackc_sim.py` — nguồn tạm khi chưa có broker

`TrackCSim(registry, seed=21)`, `tick(seq) -> list[TelemetryEnvelope]`: giá trị = `base + amp*sin(2π·seq/period + phase) + noise(seeded)`; **ts = BASE_TS + seq giây** (event-time, AD-10 — không dùng wall clock). Profiles:

| signal | base | amp | unit |
|---|---|---|---|
| motor_01_current | 12.0 | 2.0 | A |
| motor_01_vibration | 2.5 | 0.4 | mm/s |
| motor_01_temperature | 55.0 | 3.0 | °C |
| line_01_voltage | 400.0 | 4.0 | V |
| line_01_current | 15.0 | 1.5 | A |
| conveyor_01_speed | 0.8 | 0.05 | m/s |
| conveyor_01_load | 120.0 | 10.0 | kg |
| press_01_pressure | 6.0 | 0.3 | bar |
| probe_01_temperature | 185.0 | 5.0 | °C |
| gas_01_gas | 35.0 | 3.0 | ppm |

### 0.5 `tests/test_trackc_ingress.py`

1. `test_registry_loads_six_devices` — 6 device, 10 signal, id lowercase, unit khớp bảng đề, `validate() == []`.
2. `test_registry_rejects_bad_id_and_unknown_relation` — registry sửa tay → có lỗi validate.
3. `test_parse_shape_a_single_device` — envelope `motor_01_current`, unit A, ts passthrough.
4. `test_parse_shape_b_multi_device` — 1 message → envelope cho ≥2 device.
5. `test_parse_normalizes_uppercase` — `MOTOR_01`/`Current` → id thường.
6. `test_parse_skips_unknown_without_crash` — device lạ vào `skipped`.
7. `test_parse_missing_ts_marks_quality` — `quality == "missing_ts"`, ts rỗng.
8. `test_sim_covers_all_devices_deterministic` — `tick(0..5)` đủ 6 device / 10 signal; seed giống → giá trị giống; ts tăng đúng 1s/bước.

**Khi có host:** điền `mqtt.env` → `PYTHONUTF8=1 uv run python scripts/probe_mqtt.py` → sửa `parse_payload` cho đúng shape thật (chỉ file này + test).

---

## Epic 1 — Task store + FSM (AD-2)

`orchestration/task_store.py` — `TaskStore` SQLite WAL (chép discipline `IncidentStore`):

- Bảng `tasks`: `task_id` (`t-`+8hex, store là nơi mint duy nhất), `origin` (human|auto), `request_text`, `source_incident_id` (NULLable), `playbook_id`, `priority` (SAFETY|URGENT|ROUTINE), `state`, `stage_cursor`, `replan_count`, `evidence_json`, `created`, `updated`.
- States: `RECEIVED → PLANNING → COORDINATING → AWAITING_APPROVAL → EXECUTING → VERIFYING → REPORTED` + `AWAITING_CLARIFICATION`, `PREEMPTED`, terminal `PARTIAL|FAILED|CANCELLED`. Bảng transition tĩnh, transition hợp lệ mới ghi; `PARTIAL/FAILED` phải kèm `failed_step`.
- **Unique index `(source_incident_id, playbook_id)` WHERE state không terminal** → spawn idempotent (AD-1); re-spawn chỉ được *hạ* priority thành cao hơn, không mint mới.
- `resume()` khi khởi động; TTL wall-clock cho `AWAITING_APPROVAL/AWAITING_CLARIFICATION` (config trong `harness.yaml: trackc:`).

Tests: transition hợp lệ/bị chặn; resume đúng state; spawn 2 lần chỉ 1 task; TTL hết → CANCELLED.

## Epic 2 — Envelopes + Supervisor + Playbooks + Agents (AD-1/3/4/5)

- `bus/envelopes.py`: thêm `TaskEvent`, `ToolEvent` — field **top-level** `task_id, stage_name, agent, tool, priority`; **schema version riêng từng family** (`TASK_SCHEMA_VERSION = 1`, không đụng `SCHEMA_VERSION` toàn cục); `stage_name` là enum đóng `observe|analyze|adjudicate|plan|act|verify|report` (đặt tên rời khỏi stage `ops/` cũ). Topic helpers: `task_topic(task_id, event)`, `tool_topic(tool, event)`, `request_topic()`, `approval_topic(task_id)`.
- `orchestration/playbooks.py`: `Playbook(stages=[{name, agent, devices, approval_marked, inputs}], priority, back_edges: {stage → stage_trước, cap})`. Seed 4: `prepare_inspection`, `conflict_assessment`, `line_inspection_timeout`, `generic` (generic chỉ được xếp từ menu đóng).
- `orchestration/supervisor.py`: consume `request/in` → mint task → đi từng stage → publish `task/<id>/handoff`; stage fail → back-edge (publish `task/<id>/replan`), vượt cap → PARTIAL; expose `spawn_from_incident(incident_id, severity)` cho auto-loop (severity→priority từ bảng config bắt buộc). **Supervisor là publisher duy nhất của `task/*`.**
- `orchestration/agents/`: `BaseAgent` (mỗi role 1 `LLMClient` riêng — budget từng role); `observer` (đọc `history/` + registry: value + age + staleness class theo threshold `harness.yaml` — AD-10, stale không được bịa giá trị); `maintenance` (đọc `knowledge/runbooks` + CMMS history); `production` (đọc CMMS production_context); `safety`; `action`. Mỗi role có fallback degraded (observer→summary deterministic, maintenance→template…) — AD-4/AD-13.

Tests: request → đúng thứ tự stage; generic chỉ chọn stage trong menu (stage lạ bị code chặn); back-edge vượt cap → PARTIAL; 2 agent cùng task (≥2 agent phối hợp — điều kiện chấm); chỉ supervisor publish `task/*`.

## Epic 3 — Tool port + CMMS sim (AD-6/11/12)

- `tools/cmms_sim.py` (SQLite): `work_orders(wo_)`, `incident_records`(FK `source_incident_id` — **không bao giờ mint incident_id**, AD-11), `notifications(ntf_)`, `approval_requests(apr_, status PENDING), reports(rpt_)`, `maintenance_history(mxt_, seed sẵn)`, `production_context(seed từ mapping)`.
- `tools/port.py` — `ToolPort`: `create(kind, payload)` luôn đi **validate → create → read-back**; validate = intent schema + `device_id` thuộc registry + task đang ở state được phép act + priority hợp lệ (shield-equivalent, AD-6). **Bảng `port_keys(key PK, backend_id)` thuộc port** (idempotency registry sống sót qua vụ swap API BTC). Timeout/ambiguous → `list_by_key()` trước khi retry — thấy rồi thì reuse, không tạo trùng. `lookup(kind)` cho history/production_context.
- Mọi artifact mang **evidence block** (device, signal, value, event-time, age, staleness) copy từ observer payload — read-back kiểm tra có mặt.

Tests: create+read-back ok; timeout → list → không dup; retry reuse; device không có trong registry bị chặn ở validate; evidence block thiếu → fail.

## Epic 4 — Approval + ingress HTTP (AD-5/8)

- `ui/dashboard.py`: thêm `POST /api/request` (body: text) và `POST /api/decision` (body: task_id, approval_id, decision) → publish `request/in` / `approval/<task_id>`. **Dashboard server là publisher duy nhất của 2 family này** (browser không cần MQTT client — zero-build giữ nguyên).
- Supervisor: consume `approval/*` → validate (approval_id khớp bản `apr_` PENDING + task đang `AWAITING_APPROVAL`) → port cập nhật status (read-back) → transition + publish `approval_granted/denied`. TTL wall-clock hết → CANCELLED/PARTIAL + notification. Approval criticality theo **playbook mark** (mặc định: safety-class, adjudication, thay đổi lớn — không phải mọi bước act).

Tests: approve end-to-end trên InMemoryBus; deny path; TTL expire; approval_id sai bị chặn; chỉ dashboard publish được `approval/*`.

## Epic 5 — UI task-first (AD-14)

`ui/app.html` mở rộng: panel **Task hiện tại** (state FSM + stage + latency từng chặng), **agent trace** (timeline từ `task/*`), **approval inbox** (POST /api/decision), **MQTT status** (connected/last-msg age/staleness — dùng cho Kịch bản 2/3), lưới thiết bị **ưu tiên theo task đang chạy**. Render throttle (~500ms / rAF) — không lag khi metric updates liên tục. TTS: mở rộng `EVENT_CLIPS` cho task events (tuỳ).

## Epic 6 — Replay render + degraded drill + e2e (AD-13)

- `mode: replay`: store mở read-only, suppress spawn/publish/tool, recorder ghi **file mới**, event chỉ route tới UI. Replay không mint/transition/publish.
- `harness_loop.py`: flag `--trackc-sim`, `--task-demo`; `make e2e` mở rộng chạy đủ 3 kịch bản đề (kèm timeout drill — Tool port chế độ slow để diễn Kịch bản 3).
- Drill: LLM off (per-role fallback), broker off (replay), tool timeout.

---

## Thứ tự & phụ thuộc

```
Epic 0 → Epic 1 → (Epic 2 ∥ Epic 3) → Epic 4 → Epic 5 → Epic 6
```
- Epic 2 cần Epic 1 (task store). Epic 4 cần 2+3. Epic 5 cần 4 (approval inbox). Epic 6 cần hết.

## Map điều kiện chấm (đối chiếu nhanh)

| Điều kiện | Nơi đảm bảo |
|---|---|
| ≥4/6 thiết bị ingest + hiển thị | Epic 0 (sim 6/6) + Epic 5 (MQTT status) |
| ≥3 agent, ≥1 task 2+ agent | Epic 2 (6 role) |
| Quyết định dùng dữ liệu MQTT | observer stage + evidence block (Epic 2/3) |
| Tool tạo WO/incident/notification/approval/report | Epic 3 catalog |
| Verification + không trùng khi retry | Epic 3 validate→create→read-back + port_keys |
| Dashboard rõ khi nhiều metric | Epic 5 throttle + task-first |

## Chờ BTC (không chặn build)

1. **Broker host** → điền `mqtt.env` → chạy probe → sửa `parse_payload` (1 file).
2. **`tk_…` key / API thật** → thêm backend thứ hai cho `ToolPort` (không đụng agents).
