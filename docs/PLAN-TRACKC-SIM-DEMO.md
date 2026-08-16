# PLAN-TRACKC-SIM-DEMO — Digital-twin data source + kịch bản dựng sẵn (2026-08-16)

> **Bối cảnh:** broker BTC chết (lần 2) ngay trước демo → không thể phụ thuộc MQTT lúc
> chấm. Quyết định: **tự tạo data stream** (digital twin theo đúng contract BTC) +
> **kịch bản dựng sẵn theo timeline**, chạy nguyên stack như chạy live. Mục tiêu duy
> nhất lúc này: **showoff độ khó + độ kỹ thuật cao**, view-only (không thao tác).
>
> Nguyên tắc vàng: **KHÔNG đụng gì hạ tầng** — chỉ thay NGUỒN tại biên AD-9. Sim phát
> ra đúng contract frame (như broker thật), qua đúng `parse_payload` → `tele/*` →
> history → detector → supervisor → tool port → dashboard. Đổi nguồn = 1 flag.
> Đây không phải "hạ cấp demo" — đây chính là minh chứng sống cho AD-9 (broker
> quarantined in adapters): *"broker chết, ta đổi nguồn, cả hệ thống không đổi một
> dòng downstream"* — câu chuyện kỹ thuật đắt nhất buổi chấm.

## 0. Gì giữ nguyên / gì thêm

| Giữ nguyên (không sửa) | Thêm mới |
|---|---|
| `parse_payload` / `build_payload` (contract 2 chiều) | `adapters/trackc_scenarios.py` — ScenarioPlayer + kịch bản |
| bridge loopback `ingest_payload` → `tele/*` | harness `--trackc-sim <kịch bản>` (+ `--trackc-auto` fallback live→sim) |
| AutoLoopDetector / ApprovalAutopilot / policy | rule **escalate severity** trong detector (2 điều kiện xấu → critical → SAFETY) |
| Supervisor / ToolPort / FSM / replan / PARTIAL | chip nguồn + scenario trên UI; `--fresh` dọn DB cho demo lặp lại được |
| Toàn bộ SPA 8 panel view-only | 5–7 test mới |

## 1. Kiến trúc nguồn sim (đi QUA biên AD-9, không chui)

```
ScenarioPlayer (2Hz, epoch = wall − 245s)
   │  build_payload(...)  ← cùng contract frame như broker thật
   ▼
TrackCBridge.ingest_payload(raw)      ← ĐÚNG đường MQTT đi (AD-9 quarantine giữ nguyên)
   ▼  parse_payload → TelemetryEnvelope (quality, event-time ts)
tele/* ─► history (arrival = wall) ─► AutoLoopDetector ─► spawn_from_incident
                                  └─► Dashboard snapshot ─► SPA (view-only)
ApprovalAutopilot ──publish_decision──► approval/<id> ──► Supervisor ──► tool/*
```

Chi tiết đắt giá (nói với BTC):
- **Epoch giả lệch như broker thật**: sim clock = wall − 245s → chip "broker epoch
  −245s vs wall" vẫn đúng, câu chuyện event-time vs wall-clock là THẬT chứ không dàn.
- **Arrival vs event-time 2 đồng hồ**: arrival ghi wall-clock tại ingest (HistoryBuffer
  đã có) — kịch bản "stopped-stream" chỉ xảy ra được nhờ cặp đồng hồ này.
- **Heartbeat nguồn**: player phát `bridge/heartbeat {"connected": true, "source":
  "sim", "scenario": "<tên beat>"}` mỗi 5s → chip hiện `SIM ● LIVE · scenario:
  ANOMALY_MOTOR` — trung thực về nguồn (không giả mạo MQTT) và chính là bằng chứng
  source-swap theo thiết kế.

## 2. Kịch bản (beats) — mỗi kịch bản map vào AD nào

Quy ước beat: `{t, việc}` với việc ∈ `status(dev, st)` · `omit(dev)` (không gửi frame
thiết bị đó — giả broker ngừng gửi) · `resume(dev)` · `flaky_cmms(on)` · `lag(ms)`.
Mọi kịch bản bắt đầu bằng ≥30s healthy (cho grid đầy), giá trị dạng sóng sin+noise
deterministic (seed cố định — dùng lại PROFILES trong `adapters/trackc_sim.py`,
không viết lại waveform).

| Kịch bản | Timeline | Kỹ thuật showoff | AD |
|---|---|---|---|
| **S0 healthy** | mọi thiết bị ok mãi | baseline: 2Hz, event-time, 6/6 device | 9/10/11 |
| **S1 motor_offline** | t30 `status motor_01=offline` 25s → resume | detector → incident `high→URGENT` → task tự chạy đủ vòng: gate TTL 4s → REPORTED → tool audit 4 artifact | 1/2/3/4/5/6/8 |
| **S2 press_escalate** | t30 `press_01=error`; **t40** (task đang PLANNING) `press_01=offline` + đã stale | **idempotent + raise**: cùng cửa sổ 300s → KHÔNG mint task mới, chỉ nâng URGENT→**SAFETY** (feed hiện "đã có task — chỉ nâng priority"); rule escalate = quality xấu VÀ arrival-stale → `critical` | 1 (bảng đóng severity) |
| **S3 line_stopped** | t30 `omit line_01` (frame vẫn có 5 thiết bị khác) | **stopped-stream**: event-time vẫn "tươi ở frame cuối" nhưng arrival già đi → sau `critical_seconds` (demo để 20s qua flag) detector bắt được — chứng minh cặp 2 đồng hồ | 10 |
| **S4 gas_flaky_partial** | t30 `gas_01=error` + `flaky_cmms on` | **act-fail thật**: tool port `failed` đỏ → replan `↺ act→plan 1/2` → fail nữa → **PARTIAL** + reason — không success ảo, không retry mù | 3/6 |
| **S5 deny_first** (tập/Q&A) | policy `deny-first` + S1 | cổng từ chối → PARTIAL `approval_denied` — human-in-the-loop thật | 8 |
| **full_show** | chuỗi S1→S2→S3→S4 dồn vào ~4 phút (bảng mục 4) | bản chạy lúc chấm | tất cả |

## 3. Code việc cần làm (user tự code — chi tiết từng file)

### 3.1 `adapters/trackc_scenarios.py` (MỚI)
```python
"""Digital-twin source (AD-9): scenario player phát contract frame Y HỆT broker
thật, qua bridge.ingest_payload — toàn bộ pipeline downstream không biết nguồn
là sim. Deterministic: seed cố định, epoch = wall − lag (mặc định 245s như broker
BTC thật để câu chuyện 2 đồng hồ là thật)."""
@dataclass
class Beat:
    t: float; action: str; device: str = ""; status: str = "ok"

SCENARIOS: dict[str, list[Beat]] = {
    "healthy": [],
    "motor_offline":  [Beat(30, "status", "motor_01", "offline"),
                       Beat(55, "status", "motor_01", "ok")],
    "press_escalate": [Beat(30, "status", "press_01", "error"),
                       Beat(40, "status", "press_01", "offline"),
                       Beat(90, "status", "press_01", "ok")],
    "line_stopped":   [Beat(30, "omit", "line_01"), Beat(120, "resume", "line_01")],
    "gas_flaky_partial": [Beat(30, "status", "gas_01", "error"),
                          Beat(30, "flaky_cmms", "", "on"),
                          Beat(120, "status", "gas_01", "ok"),
                          Beat(120, "flaky_cmms", "", "off")],
    "full_show": [ ... ghép beats của S1..S4 lệch nhau 60–70s ... ],
}

class ScenarioPlayer:
    def __init__(self, registry, bridge, scenario="full_show", hz=2.0,
                 epoch_lag_s=245.0, seed=7): ...
    # state: status[dev], omitted=set(dev), flaky, t0
    def tick(self) -> None:
        """1 frame: áp beats đến giờ hiện tại, dựng values từ PROFILES của
        TrackCSim (theo (t, seed)), gọi bridge.ingest_payload(build_payload(...)).
        Thiết bị trong omitted bị BỎ KHỎI devices[] (giả broker ngừng gửi)."""
    def start(self): ...   # thread 2Hz + heartbeat bridge/heartbeat mỗi 5s
```
Ghi chú:
- Values: dùng lại `adapters/trackc_sim.py` PROFILES (sin theo phase + noise seed) —
  không copy-paste; import thẳng.
- `flaky_cmms on` → set env/note cho CMMSSim (xem `tools/cmms_sim.py` đã có cơ chế
  fail theo chu kỳ chưa; nếu chưa: thêm attr `flaky=True` khiến create rơi khoảng
  50% theo chu kỳ deterministic — KHÔNG random không seed).
- Heartbeat: publish `bridge/heartbeat` với thêm `"source": "sim"`, `"scenario":
  tên kịch bản` — Dashboard `_ingest_bridge` giữ 2 field đó (thêm 2 dòng).

### 3.2 Detector — rule escalate (SỬA `orchestration/auto_loop.py`, ~6 dòng)
Trong `_assess`: thu cả 2 tín hiệu (quality xấu, arrival-stale). severity:
```python
quality_bad = ...   # như hiện tại
stopped     = ...   # như hiện tại
if quality_bad and stopped:  severity = "critical"   # → SAFETY qua bảng đóng
elif quality_bad or stopped: severity = "high"
```
Giữ nguyên: severity phải nằm trong `SEVERITY_PRIORITY` (bảng đóng, không default).

### 3.3 Harness `--trackc-sim` / `--trackc-auto` / `--fresh` (SỬA `harness_loop.py`)
```python
ap.add_argument("--trackc-sim", default="", metavar="SCENARIO",
                help="chạy control room trên nguồn digital-twin (không MQTT); "
                     "scenarios: healthy|motor_offline|press_escalate|line_stopped|"
                     "gas_flaky_partial|full_show")
ap.add_argument("--sim-critical-seconds", type=float, default=20.0,
                help="critical_seconds cho run này (demo 8 phút cần ngắn hơn 120s)")
ap.add_argument("--sim-epoch-lag", type=float, default=245.0)
ap.add_argument("--trackc-auto", action="store_true",
                help="thử MQTT live 15s; không có tele → TỰ ĐỔI nguồn sang sim "
                     "(chip đổi LIVE→SIM) — source-swap theo AD-9")
ap.add_argument("--fresh", action="store_true",
                help="dọn tasks.db/cmms.db/port_keys.db/history trước khi chạy — "
                     "demo lặp lại y hệt")
```
- Nhánh `--trackc-sim` = clone nhánh `--trackc-live` (dashboard thresholds, detector,
  autopilot, recorder…) nhưng **không TrackCBridge.connect()** — thay bằng
  `ScenarioPlayer(reg, bridge_ảo, args.trackc_sim, ...)`; detector nhận
  `critical_seconds=args.sim_critical_seconds`. Cần một object bridge "loopback-only"
  (`ingest_payload` hoạt động, không paho) — TrackCBridge hiện đã vậy: tạo bình
  thường, GỌI `start(bus)`, KHÔNG gọi `connect()`.
- `--trackc-auto`: khởi bridge live + 1 thread đếm `dash.snapshot()["counters"]["tele"]`;
  sau 15s mà =0 → tắt bridge, dựng ScenarioPlayer, chip tự đổi vì heartbeat đổi.
- `--fresh`: xóa 4 file DB (an toàn: chỉ trong nhánh trackc runs).

### 3.4 UI chip nguồn + scenario (SỬA `ui/dashboard.py` + `app.html`, ~10 dòng)
- `_ingest_bridge`: lưu thêm `source`, `scenario` từ payload.
- `renderHeader`: chip `SIM ● LIVE · scenario: <tên>` (source=sim) hoặc `MQTT ● LIVE`
  (source=mqtt) — TRUNG THỰC nguồn. Banner đỏ vẫn là disconnected.

### 3.5 README/MQTT-GUIDE (SỬA, nhỏ)
Thêm mục "Digital-twin demo mode": vì sao tồn tại (broker chết), lệnh chạy, và ghi
rõ dữ liệu rehearsal trên broker thật (probe 2026-08-16, smoke 12 400 tele) — bằng
chứng hệ đã chạy thật.

## 4. Bảng chạy `full_show` lúc chấm (8 phút, real-time 1x)

`PYTHONUTF8=1 uv run python harness_loop.py --trackc-sim full_show --approval-policy auto --approval-delay 4 --sim-critical-seconds 20 --fresh --serve-ui 8765`

| t | Beat (tự xảy ra) | Trên màn hình | Câu thoại |
|---|---|---|---|
| 0:00–0:40 | healthy 2Hz, epoch −245s | chip SIM LIVE, grid 6 ok, chart chạy | "Đây là digital twin phát đúng contract của BTC — cùng parser, cùng pipeline như broker thật. Từ giờ tôi không đụng gì" |
| 0:40 | motor_01 offline | feed `▲ inc-motor_01` → task RECEIVED | "Bảng đóng severity→priority, spawn idempotent" |
| 0:50 | task chạy | stage chips, finding | "Playbook deterministic, 6 agent context" |
| 0:55 | gate TTL ~4s → approve | REPORTED + tool audit 4 dòng | "Cổng AD-8 + TTL; policy auto — production 1 flag sang human" |
| 1:30 | press_01 error rồi offline+stale | feed dòng 2: "chỉ nâng priority → SAFETY" | "Không mint task 2 — idempotent, chỉ raise. Đây là bảng đóng, không heuristic" |
| 2:10 | line_01 biến mất khỏi frame | ~2:30 feed `stopped-stream` | "Event-time của frame cuối vẫn 'mới' — chỉ arrival-vs-now bắt được im lặng. 2 đồng hồ" |
| 3:00 | gas_01 error + CMMS flaky | tool `failed` đỏ → `↺ 1/2` → PARTIAL | "Từ chối là thật. Re-plan trong cap, hết cap PARTIAL" |
| 3:40– | resume hết, grid xanh lại | trail đầy, footer 13 AD | "Mọi thứ là JSONL — replay render-only. Broker thật chết giữa chừng? Đổi 1 flag" |
| 4:00–8:00 | Q&A, mở `?json=1`, mở `<details>` | | "Không có state ẩn — tất cả là bus events" |

## 5. Tests (thêm ~6)

1. `test_scenario_player_contract_frames` — tick N frame → mỗi frame qua
   `parse_payload` sạch (0 skipped), đủ 6 devices, epoch lệch đúng lag, event ts ISO.
2. `test_scenario_omit_device_stops_arrival` — chạy player 3s có `omit line_01` →
   history KHÔNG có dòng mới của line_01 (arrival cũ dần) trong khi 5 thiết bị khác vẫn ghi.
3. `test_detector_escalates_to_critical` — device offline + stale → severity
   `critical` → priority `SAFETY` qua bảng đóng.
4. `test_press_escalate_raises_not_mints` — fast-forward kịch bản (speed 20x hoặc
   gọi `tick` tay theo đồng hồ giả): trigger 1 → mint; trigger 2 escalate cùng window
   → `minted=False`, priority SAFETY.
5. `test_full_show_runs_to_completion` — `--trackc-sim full_show` tốc độ nhanh
   (inject clock giả): cuối chạy có ≥3 task terminal (REPORTED/PARTIAL), ≥3 dòng feed,
   tools audit không rỗng.
6. `test_chip_source_sim_vs_mqtt` — heartbeat source=sim → snapshot
   `bridge.source=="sim"`.

Acceptance: `PYTHONUTF8=1 uv run pytest tests/ -q` xanh; `--trackc-sim full_show
--fresh` chạy 2 lần liên tiếp cho kết quả giống hệt (deterministic — cùng số task,
cùng state, cùng artifact count).

## 6. Thảo luận — vì sao hướng này MẠNH hơn live lúc chấm (và cái giá)

**Mạnh:**
- Kiểm soát 100% timeline 8 phút — không cầu may BTC bật kịch bản.
- Câu chuyện AD-9 trở thành DEMO thay vì slide: "broker chết, đổi nguồn 1 flag, 0 dòng
  downstream sửa" — đây là định nghĩa của boundary làm đúng.
- Deterministic → tập bao nhiêu lần cũng y hệt → tự tin tuyệt đối trên sân khấu.
- Vẫn chạy ĐÚNG đường MQTT đã đi (contract frame → parse → tele/*) nên không ai bắt
  bẻ "demo giả": parser, detector, FSM, tool port đều thật.

**Cái giá + cách trả lời:**
- BTC hỏi "sao không dùng dữ liệu thật?" → "Chúng tôi chạy thật từ sáng (12 400 tele,
  probe 2Hz, 6/6 device — có log + screenshot trong README); broker chết ngoài tầm
  kiểm soát của team nên hệ thống chuyển nguồn digital-twin theo đúng thiết kế
  AD-9 — và đây chính là điểm chúng tôi muốn show: degraded mode có kiểm soát."
  → NÊN quay sẵn 1 video màn hình bản chạy LIVE hôm nay làm bằng chứng dự phòng.
- `--trackc-auto` là "best of both": nếu phút chấm broker sống lại → chip MQTT LIVE
  thật; chết → tự chuyển SIM ngay trên sân khấu (khoảnh khắc đắt nhất nếu xảy ra).

**Đề xuất xếp hạng ưu tiên:** (1) `--trackc-sim` + S1 + full_show — tối thiểu chạy
được; (2) escalate + S2 — khoảnh khắc idempotent/raise; (3) S3 stopped-stream — khoảnh
khắc 2 đồng hồ; (4) S4 PARTIAL; (5) `--trackc-auto`; (6) S5 + video live dự phòng.

## 7. Rủi ro

- **Window 300s của detector**: full_show dài 480s — nếu beat lặp cùng thiết bị sau
  >300s sẽ mint task mới (không phải bug, là window roll) → giữ mỗi thiết bị 1 chuỗi
  beat trong 300s, hoặc để ý khi soạn `full_show`.
- **TTL 300s mặc định vs delay 4s** — không đổi; gate chỉ hiện ~4s là đúng kịch bản.
- **DB tồn dai dẳng**: luôn `--fresh` khi tập/chấm (TaskStore sống qua lần chạy).
- **flaky CMMS phải deterministic** (chu kỳ theo tick, không `random` trần) — nếu
  không test 5 sẽ nhấp nhô.
- **Epoch lag 245s + staleness 20s**: detector dùng ARRIVAL (wall) cho stopped-stream
  nên lag không ảnh hưởng — nhưng đừng ai "sửa" thành so event-time.

## 8. Thứ tự làm + commit

1. `adapters/trackc_scenarios.py` (player + S0/S1) + `--trackc-sim` + `--fresh` + tests 1–2
2. escalate rule + S2 + test 3–4
3. S3 + `--sim-critical-seconds` + S4 (flaky CMMS) + test 5
4. UI chip nguồn + test 6 + `full_show` tinh chỉnh timeline theo tập thật
5. `--trackc-auto` + quay video bản live dự phòng + cập nhật README/MQTT-GUIDE

Commit gợi ý: `feat(demo): digital-twin source + scripted scenarios qua AD-9 boundary
(--trackc-sim/--trackc-auto, escalate severity, --fresh deterministic demo)` (+ trailer
`Co-Authored-By: Claude <noreply@anthropic.com>`).
