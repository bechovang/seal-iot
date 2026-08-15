# Kiến trúc & Cơ chế Vận hành — SEAL IoT Smart Ops Harness

> Tài liệu mô tả chi tiết cách thức hệ thống vận hành, cấu trúc từng tầng, các
> ràng buộc kiến trúc (đặt tên AD-1 … AD-14) và luồng dữ liệu theo event-time.
> Nội dung phản ánh đúng code hiện tại trên `master`.

---

## 1. Nguyên tắc thiết kế nền

### 1.1. Đồng hồ sự kiện duy nhất (AD-10)
- Toàn bộ pipeline dùng **event-time** lấy từ dữ liệu nguồn (timestamp của bộ dataset),
  **không bao giờ** dùng `datetime.now()`/`utcnow()` cho timestamp sự kiện trong pipeline.
- Vì vậy, khi *replay nhanh* hoặc *attack hết hạn tự nhiên*, hệ thống không nhầm
  "thời gian trôi qua" với "đã cải thiện". VERIFY chỉ đọc cửa sổ mẫu **sau** mốc
  `after_epoch_ms` (AD-10 được thực thi trong `verify/`).
- Ngoại lệ hợp lệ: đồng hồ wall-clock chỉ dùng trong UI để **hiển thị** độ trễ
  (không phải để định mốc sự kiện) và trong LLM để **giới hạn thời gian gọi** (AD-13/5.5).

### 1.2. Single-writer (một kẻ ghi duy nhất)
- `history/` (HistoryBuffer) chỉ được ghi bởi **tầng ingest**; mọi khối khác chỉ đọc.
- `learn/` (MetricStore) có một live slot ghi duy nhất cho từng sự cố.
- **Executor** trong `act/` là **người publish duy nhất** lên `cmd/*` (AD-7/AD-8).

### 1.3. Cách ly dữ liệu gắn nhãn (AD-11)
- Các cột label (`P*_attack*`…) bị **quarantine**: không bao giờ được publish lên bus.
- Chỉ duy nhất `score/` (ScoreboardBuilder) được đọc label — phục vụ đánh giá thực hành
  (P/R/F1, MTTD, MTTR…) sau khi vòng lặp chạy, không dùng để dẫn dắt quyết định.

### 1.4. Không dùng ngưỡng tĩnh để phát hiện (AD-4)
- PERCEIVE dùng **EMA/ADWIN** và biến thể **Isolation Forest** — không có
  stop-threshold cứng. Ngưỡng tĩnh duy nhất nằm trong `SafetyShield` (với tư cách
  *interlock* cho hành động, không phải bộ phát hiện bất thường).

### 1.5. MQTT-only, QoS 1
- Mọi stage giao tiếp qua `bus/` bằng MQTT (Mosquitto), QoS 1, không retain,
  JSON snake_case, envelope có **schema version**.

---

## 2. Bản đồ module & trách nhiệm

```
seal iot/
├─ config.py            # khai báo dataclass cấu hình; nạp harness.yaml + mapping.yaml
├─ harness.yaml         # knobs chạy (taxonomy, runbook, diagnosis, llm, debate, decide,
│                        #   verify, incident, variant, demo)
├─ mapping.yaml         # mô hình tín hiệu chuẩn (signal_id, source HAI, kind, area,
│                        #   pairing D/Z, commands registry, topology, label quarantine)
├─ harness_loop.py      # vòng demo: record nhà máy-ảo -> smoke/ replay từ JSONL
├─ adapters/ hai.py     # adapter HAI 21.03 -> publish telemetry chuẩn hóa
├─ bus/
│  ├─ client.py         # BusClient(MQTT) + InMemoryBus + transport paho
│  ├─ envelopes.py      # TelemetryEnvelope, StageEvent, topic builder
│  └─ recorder.py       # JsonlRecorder (JSONL append-only) + replay + smoke
├─ perceive/            # phát hiện: AdaptiveDetector (EMA/ADWIN), IsolationForestVariant
│  └─ episode.py        # Episode (bằng chứng: score, innovation, window_stats)
├─ history/ buffer.py   # HistoryBuffer — SQLite WAL, single-writer ingest
├─ diagnose/
│  ├─ matcher.py        # RunbookMatcher + extract_symptoms (taxonomy đóng)
│  ├─ causal.py         # CausalGraphBuilder (lag-correlation + Granger)
│  ├─ rca.py            # RCAAgent + compute_divergence (D/Z)
│  ├─ debate.py         # DebateGate (proposer/critic/arbiter) + Diagnosis
│  └─ pipeline.py       # Diagnoser: runbook-first -> graph -> RCA -> debate/rule_only
├─ knowledge/ runbooks.py # RunbookStore: match (Jaccard) + record_resolution (chưng cất)
├─ llm/ client.py       # LLMClient + MockLLM + OpenRouterBackend + wall-clock guard
├─ decide/ action.py    # CandidateGenerator, ObjectiveScorer, Decider, do(∅)
├─ plant_model/ model.py# PlantModel: predict(cmd, target, baseline) + calibrate()
├─ act/
│  ├─ guard.py          # SafetyShield (xác định) + ActExecutor (sole publisher)
│  └─ pipeline.py       # GuardedActionPipeline: DECIDE->shield->execute->VERIFY->FSM
├─ incident/ fsm.py     # IncidentFSM + IncidentStore (SQLite) + distill_hook
├─ verify/              # OutcomeClassifier (improved|no_change|worsened)
├─ learn/ metrics.py    # MetricStore/MetricRow (single-writer)
├─ score/ scoreboard.py # ScoreboardBuilder — độc giả của label
└─ ui/
   ├─ dashboard.py      # Dashboard subscribe-only + HTTP worst (stdlib)
   └─ tts.py            # VTTSAnnouncer (tiếng Việt, queued + dedupe)
```

---

## 3. Cơ chế vận hành theo từng stage

### 3.1. Ingest / hấp thụ dữ liệu
- `adapters/hai.py` đọc bộ dữ liệu HAI **thật** (hỗ trợ `.csv` và `.csv.gz`), maps cột gốc
  (vd `P1_FT01`, cặp setpoint/feedback `P1_FCV01D`/`P1_FCV01Z`) → `signal_id` chuẩn
  (`P1_FC_01`, `P1_FC_01_D`, `P1_FC_01_Z`) theo `mapping.yaml` (đơn vị, scaling, vùng `area`, cặp D/Z).
- Cột `time` của HAI (`'2020-07-07 15:00:00'`, space-separated) được parse thành **event-time**
  (epoch ms, coi là UTC) ngay tại ingest — nguồn dữ liệu thời lượng đường ống (AD-10).
- Các cột `label_columns` (`attack`, `attack_P1|P2|P3`) bị **quarantine** — không publish.
- Khi không cấp slice dữ liệu, adapter tự stream `synthetic_rows()` (seeded) để pipeline
  chạy/test mà không cần file HAI thật (`adapters/hai.py::synthetic_rows`; `tests/test_epic6`
  tự bỏ qua nếu thiếu file trên đĩa).
- Telemetry được ghi vào `history/` (chỉ ingest được ghi) và publish lên `tele/<signal_id>`.
- event-rate mặc định 1.0 Hz; tốc độ load cold-start là tiêu chí theo dõi (< 60 min).

### 3.2. PERCEIVE — phát hiện bất thường
- `AdaptiveDetector` theo dõi phân bố trực tuyến (EMA/ADWIN) và `IsolationForestVariant`
  (nền) — chọn qua `variant.detector`.
- Khi lệch lạc vượt ngưỡng thích nghi, phát ra `Episode` với bằng chứng:
  `episode_key`, `signal_id`, `score`, `filter_innovation`, `window_stats`,
  `ts_epoch_ms` (event-time), `detector`.
- Không dùng ngưỡng tĩnh (xem 1.4).

### 3.3. DIAGNOSE — chẩn đoán (AD-3, AD-5)
1. **Runbook-first**: `RunbookMatcher.extract_symptoms()` ánh xạ bằng chứng episode +
   phân kỳ **D/Z** (điểm đặt D, phản hồi Z) vào **mã triệu chứng trong taxonomy đóng**
   (`symptom_taxonomy` trong `harness.yaml`). So `Jaccard` với kho `knowledge/`:
   nếu khớp (≥ `min_tokens_match`, ≥ `jaccard_threshold`) → **hit**, không cần suy luận sâu.
   Văn bản triệu chứng tự do **không bao giờ** được lưu/so khớp.
2. **Causal graph** (`CausalGraphBuilder`): xây đồ thị có hướng với **trọng số heuristik**
   từ lag-correlation / Granger (`statsmodels`) trên HistoryBuffer; các cạnh được gắn
   cờ `heuristic: true`, không tuyên bố quan hệ nhân quả chứng thực.
3. **RCA** (`RCAAgent`): xếp hạng giả thuyết root-cause kèm *độ tin cậy + khoảng tin cậy*,
   coi **phân kỳ D/Z là bằng chứng hạng nhất** — phân biệt `actuator_manipulation`
   (điểm đặt di chuyển) vs `sensor_fault` (điểm đặt ổn định, phản hồi lệch).
4. **Debate gate** (`DebateGate`): bộ ba proposer → critic → arbiter (LLM) xác nhận
   chẩn đoán cuối; theo `budget` nếu cạn → hạ cấp single-pass. Ở chế độ
   `variant.diagnosis_mode = rule_only` (hoặc `llm_offline`) → dùng **rule xác định**
   (chọn giả thuyết top mà không gọi LLM — AD-13).

### 3.4. DECIDE — quyết định (AD-6, AD-12)
- `CandidateGenerator`: luôn sinh **`do(∅)`** (không hành động — dòng *counterfactual*)
   + **≥ `candidate_count` ứng viên thật** từ command registry. Ở `variant.red_team=true`
   (chỉ để thực hành) **chèn ứng viên xấu cố ý** để đo shield.
- `ObjectiveScorer` chấm mỗi ứng viên:
  `objective = alpha·safety + beta·energy − gamma·downtime − delta·risk`
  (trọng số trong `harness.yaml`, chuẩn hóa → kết quả lặp lại được).
- `do(∅)` có chi phí downtime/risk cao để không được chọn nếu có phương án khả thi.
- Ở `variant.decide_mode = static_priors`: **bỏ mô phỏng what-if**, chỉ chấm theo
  prior tĩnh trong command registry (safe envelope + default + target).
- LLM **chỉ đề xuất**, không bao giờ là nguồn giá trị điều khiển.

### 3.5. Guard + ACT + VERIFY (AD-8, AD-9)
- `SafetyShield.evaluate()` (xác định): chặn nếu target **ngoài ranh an toàn**
  `[safe_min, safe_max]` hoặc **ngoài phạm vi vật lý** `[min, max]` của lệnh,
  hoặc không phải command. **Không cờ biến thể nào tắt được shield.**
- `ActExecutor`: **người publish duy nhất** lên `cmd/<command_name>` (QoS 1) —
  LLM không có tham chiếu tới phương thức này (AD-7).
- `GuardedActionPipeline.run()`: × tạo sự cố trong FSM → transition diagnose → decide
  → plan → execute (shield) → verify.
- `OutcomeClassifier` (event-time): so **post-action window** với **baseline**.
  `rel ≥ +improved_threshold (0.3)` → `improved`; `rel ≤ −worsened_threshold (0.1)` →
  `worsened`; còn lại `no_change`. **Chỉ `improved` mới resolve.**

### 3.6. Incident FSM (AD-2)
Trạng thái:

```
NONE -> DETECTED -> DIAGNOSING -> PLANNING -> ACTING -> VERIFYING -> RESOLVED
                \        \            \            \         |
                 ESCALATED ────────────┴────────────┘        +-> DIAGNOSING (retry)
```

- `IncidentStore` (SQLite, WAL) lưu chuỗi sự kiện; `IncidentFSM` là **người đặt mã
  incident_id duy nhất**.
- TTL tự động hết hạn → `ESCALATED`. `retry_max` giới hạn số vòng
  `VERIFYING -> DIAGNOSING`. `human_approve` cho phép quay lại PLANNING sau escalate.
- Khi đạt `RESOLVED`, `distill_hook` chạy **trong cùng transaction**: chưng cất runbook
  (mới hoặc tăng `occurrences`/`reliability`) + ghi metric sự cố.

### 3.7. SELF-LEARN & đánh giá (AD-3, AD-11)
- **Runbook distillation**: `RunbookStore.record_resolution(tokens, root_cause, hint)`
  — chuyển prose của LLM thành mã triệu chứng đóng rồi lưu/tăng tần suất.
- **Metric log**: `learn/metrics.py` ghi 1 dòng/sự cố (delay phát hiện, latency RCA,
  thời gian resolve, outcome, arm, signal).
- **Scoreboard** (`score/scoreboard.py`): đọc metric log + label quarantine → tính
  Precision/Recall/F1, **MTTD** (mean time to detect), **MTTR** (mean time to restore),
  Top-1/Top-3 RCA accuracy, **tỷ lệ hành động mất an toàn** (quãng hành động bị miss),
  **số can thiệp sai**, **downtime tránh được**.

---

## 4. Kiến trúc quyết định (AD — Architectural Decisions)

| ID | Quyết định | Áp dụng tại |
|---|---|---|
| AD-1 | Ngưỡng tĩnh duy nhất nằm trong **safety shield** (interlock); detection không dùng ngưỡng tĩnh | `act/guard.py`, `perceive/` |
| AD-2 | Sự cố là **FSM** với lưu trữ **SQLite** và trạng thái resolve/retry/escalate | `incident/fsm.py` |
| AD-3 | Chẩn đoán **runbook-first** trên **taxonomy triệu chứng đóng** (Jaccard); LLM prose chỉ khi chưng cất | `diagnose/matcher.py`, `knowledge/` |
| AD-4 | Phát hiện bằng **EMA/ADWIN + Isolation Forest** (thích nghi), không stop-threshold | `perceive/detector.py` |
| AD-5 | **Phân kỳ D/Z** là bằng chứng hạng nhất; đồ thị **topology + causal** là heuristic | `diagnose/rca.py`, `diagnose/causal.py` |
| AD-6 | **DECIDE** qua objective trọng số (α,β,γ,δ) + `do(∅)` counterfactual | `decide/action.py` |
| AD-7 | **Executor** là người publish duy nhất lên `cmd/*` | `act/guard.py`, `bus/client.py` |
| AD-8 | **Command registry** (safe envelope) dùng chung bởi shield **và** plant model | `mapping.yaml`, `act/`, `plant_model/` |
| AD-9 | **VERIFY** phân loại kết quả theo event-time; chỉ `improved` resolve | `verify/` |
| AD-10 | Một đồng hồ **event-time** cho pipeline; không wall-clock cho timestamp sự kiện | toàn hệ thống |
| AD-11 | Label bị **quarantine**; chỉ `score/` đọc | `adapters/`, `score/scoreboard.py` |
| AD-12 | Giá trị điều khiển **không bao giờ đến từ LLM**; LLM chỉ đề xuất | `decide/` |
| AD-13 | **Degraded mode** (rule-only / static-priors / llm_offline) — hệ thống vẫn chạy offline | `diagnose/`, `decide/`, `llm/` |
| AD-14 | **JSONL record/replay** làm bộ chứng cứ + fixture phục hồi khi lỗi | `bus/recorder.py`, `harness_loop.py` |

---

## 5. Các chế độ vận hành / biến thể (`harness.yaml → variant`)

| Cờ | Giá trị | Ý nghĩa |
|---|---|---|
| `detector` | `adwin` / `isolation_forest` | Chọn bộ phát hiện PERCEIVE |
| `diagnosis_mode` | `full` / `rule_only` | Suy luận đầy đủ (LLM) hoặc **rule xác định** (offline) |
| `decide_mode` | `whatif` / `static_priors` | Chấm điểm có mô phỏng hay theo prior tĩnh |
| `red_team` | `true/false` | **Chỉ thực hành**: chèn ứng viên xấu để đo shield |
| `llm_offline` | `true/false` | Ép mock-LLM / chạy hoàn toàn offline |

> Ghi chú an toàn: **không có cờ nào tắt `SafetyShield`.** `red_team` chỉ tạo
> ứng viên xấu thuộc *tập đề xuất* — shield vẫn chặn khi chọn.

---

## 6. Luồng dữ liệu tổng thể (1 sự cố)

```
[HSI/HAI] --tele--> INGEST --(write history)--> PERCEIVE --ops/perceive-->
   DIAGNOSE --ops/diagnose--> DECIDE --ops/decide--> SHIELD --(block?)-- executor
   --cmd/<name> (QoS1)--> VERIFY --ops/verify--> INCIDENT FSM
        (RESOLVED) --> distill runbook + metric --> (scoreboard đọc label quarantine)
```

**Thời gian / sự kiện:** mọi payload đều mang `ts` (ISO) hoặc `ts_ms` (epoch) theo
**event-time**, để replay phục hồi được và không nhầm lẫn do attack hết hạn.

---

## 7. Vòng lặp demo (`harness_loop.py`)

- `--log <file>`: ghi mọi sự kiện vào **JSONL append-only** (`bus/recorder.py`).
- `--replay`: đọc lại log đã ghi, feed qua handler, rồi `smoke()` kiểm tra điều kiện
  tối thiểu (có perceive, diagnose, **có command đã thực thi**, có result RESOLVED).
  Replay ghi ra **file `.replay` riêng** để không mọc-vô-hạn khi append trong lúc đọc.
- `--serve-ui PORT`: mở dashboard **subscribe-only** (chỉ stdlib, không build step,
  dashboard chết thì vòng lặp vẫn chạy). Hiển thị: dải pipeline trực quan
  (PERCEIVE→DIAGNOSE→DECIDE→ACT→VERIFY→INCIDENT + số đếm), **card từng sự cố**
  (diễn tiến các stage, trạng thái FSM như RESOLVED, độ trễ theo **event-time** giữa
  các stage dạng thanh), và trail sự kiện gần nhất. Endpoint `/?json=1` trả snapshot
  JSON thô để tích hợp.
- `--tts`: bật announcer tiếng Việt (queued + dedupe; nếu thiếu clip/synthesizer
  thì *im lặng*, không làm hỏng vòng lặp — AD-13).

Make targets: `make lint`, `make test`, `make e2e` (ghi+replay+smoke), `make demo`
(cùng vòng lặp dưới nhãn `red-team`), `make check` (lint+test+e2e).

## 8. Xác nhận với dữ liệu HAI thật
- `tests/test_epic6_real_hai.py` đọc **đúng file HAI 21.03** (`tai lieu/bo-du-lieu/03-HAI/`,
  gzip) và kiểm: mọi `source` của `mapping.yaml` tồn tại trong file; cột `time` parse thành
  event-time đơn điệu; ingest 2600 dòng thật → telemetry + history; **phát hiện được vùng
  tấn công thật** (for inside test1 mở đầu ở dòng ~2111). Nếu file không tồn tại trên máy,
  5 bài này **tự bỏ qua** (không chặn `make test`).