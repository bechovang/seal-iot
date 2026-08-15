# Hướng dẫn Cài đặt & Sử dụng — SEAL IoT Smart Ops Harness

> Hướng dẫn bằng tiếng Việt: cài đặt từ đầu (Windows/macOS/Linux), lệnh chạy hàng ngày,
> chạy vòng demo (record/replay, dashboard, TTS), và cách thay đổi cấu hình.

---

## 1. Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Python | **3.12** | Khuyến nghị 3.12.12; dự án hỗ trợ >=3.11.9,<3.13 |
| uv | bản mới nhất | Trình quản lý môi trường/dependency |
| Mosquitto | (tùy chọn) | Cần để dùng MQTT broker thật; có thể chạy offline với `InMemoryBus` |
| Ổ đĩa | — | Vòng demo cần vài MB (JSONL/log) |

Trên **Windows**, bắt buộc đặt `PYTHONUTF8=1` khi chạy Python để tránh lỗi mã hóa
`cp1252` với tiếng Việt (các lệnh bên dưới đều đã kèm sẵn).

---

## 2. Cài đặt

Clone repo rồi vào thư mục dự án:

```bash
git clone <repo-url> seal-iot
cd "seal iot"
```

Tạo môi trường và cài dependency bằng `uv`:

```bash
uv sync
```

(uv tự tạo `.venv` và cài đúng phiên bản Python + tất cả thư viện khai báo trong
`pyproject.toml`: paho-mqtt, river, numpy, scikit-learn, networkx, statsmodels,
pandas, PyYAML, pytest.)

> **Gợi ý Windows:** nếu chưa đặt `PYTHONUTF8=1`, hãy set biến môi trường dùng chung
> trong PowerShell: `$env:PYTHONUTF8="1"` (hoặc trong System Properties) để không phải
> gõ lại mỗi lệnh.

---

## 3. Kiểm tra nhanh

```bash
PYTHONUTF8=1 make lint      # kiểm tra cú pháp toàn bộ module
PYTHONUTF8=1 make test      # chạy 54 bài kiểm tra
PYTHONUTF8=1 make e2e       # ghi + replay một vòng sự cố qua JSONL rồi smoke-assert
PYTHONUTF8=1 make check     # lint + test + e2e (kiểm tra tổng hợp)
```

Khi `make test` in ra `54 passed in ...` là môi trường đã sẵn sàng.

---

## 4. Chạy vòng lặp demo

### 4.1. Record một sự cố mẫu (ghi log JSONL)

```bash
PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl
```

Kết quả mong đợi (in ra JSON tóm tắt + `demo recorded -> demo/e2e.jsonl`):

```
{"perceive": 1, "diagnose": 1, "commands": 1, "telemetry": 1, "incident_id": "..."}
demo recorded -> demo/e2e.jsonl
```

Nghĩa là: PERCEIVE phát hiện, DIAGNOSE chẩn đoán, một lệnh được thực thi có kiểm soát,
và vòng lặp **RESOLVED**.

### 4.2. Replay lại log từ chính file đó

```bash
PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl --replay
```

Kết quả: `replayed N lines from demo/e2e.jsonl; smoke PASS` — đây chính là fixture
phục hồi khi trình diễn có lỗi thời điểm (replay từ log đã ghi).

### 4.3. Demo có dashboard + TTS tiếng Việt

```bash
PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl --serve-ui 8799 --tts
```

- Mở trình duyệt **http://127.0.0.1:8799** để xem dashboard trực tiếp:
  bảng đếm sự kiện theo giai đoạn, độ trễ theo **event-time** giữa các stage, danh sách
  sự cố gần nhất. Dashboard tự refresh 2 giây/lần, **subscribe-only** (không build step;
  tắt dashboard thì vòng lặp vẫn chạy).
- `--tts` bật announcer tiếng Việt. Các clip đã tạo sẵn nằm trong `ui/tts_clips/`
  (`perceive.mp3`, `diagnose.mp3`, `decide.mp3`, `act.mp3`, `verify.mp3`, `incident.mp3`).
  Nếu thiếu clip hoặc không có synthesizer → announcer **im lặng** (không làm hỏng vòng lặp).

> Lưu ý: `--replay` ghi ra file `demo/e2e.jsonl.replay` riêng — không bao giờ append
> vào chính file đang đọc (tránh log phình vô hạn).

---

## 5. Chạy qua make targets

| Lệnh | Chức năng |
|---|---|
| `make lint` | Kiểm tra cú pháp |
| `make test` | Chạy toàn bộ pytest |
| `make e2e` | Record + replay vòng sự cố mẫu (smoke ASSERT) |
| `make demo` | Vòng demo dưới nhãn biến thể `red-team` |
| `make check` | `lint` + `test` + `e2e` |

---

## 6. Cấu hình

Hai file YAML dẫn đường mọi hành vi:

### 6.1. `mapping.yaml` — mô hình nhà máy (tín hiệu chuẩn)
- **`columns`**: map cột HAI **thật** → `signal_id` chuẩn (đơn vị, loại `kind`:
  `flow`/`pressure`/`setpoint`/`feedback`, vùng `area`). Đã căn chính theo file
  `hai-21.03_test1.csv.gz` thật: flow `P1_FT01` + cặp `P1_FCV01D`/`P1_FCV01Z`;
  áp lực `P1_PIT01` + cặp `P1_PCV01D`/`P1_PCV01Z`.
- **`pairing`**: cặp `<signal, setpoint(D), feedback(Z)>` — nguồn bằng chứng **phân kỳ D/Z**.
- **`commands`**: registry actuator (AD-8) — `target` (tín hiệu bị điều khiển),
  phạm vi vật lý `[min,max]`, **ranh an toàn `[safe_min,safe_max]`** dùng CHUNG cho
  shield và plant model, `default`, `energy_cost`, `risk_baseline`, `effect_gain`.
- **`topology`**: tài sản + kết nối (heuristic cho RCA).
- **`label_columns`**: các cột giữ gắn nhãn bị **quarantine** — chỉ `score/` đọc.

Ví dụ muốn thêm lệnh cho P2 với ranh an toàn khác: thêm phần tử vào `commands`
và đảm bảo `name` duy nhất + `safe_min <= safe_max` + ranh an toàn nằm trong phạm vi vật lý.

### 6.2. `harness.yaml` — knobs chạy
- `runbook`: `jaccard_threshold` (0.6), `min_tokens_match` (2), `store_path`.
- `diagnosis`: lag, `correlation_min`, Granger (`granger_enabled`, `maxlag`, `pvalue`), `max_hypotheses`.
- `llm`: `provider` (`openrouter`), `default_model`, `budget_per_call_path`, `fallback`,
  `max_wall_seconds` (giới hạn thời gian gọi — vượt ngưỡng → hạ cấp).
- `debate`: `role_order`, `max_turns`.
- `decide`: `weights {alpha,beta,gamma,delta}` + `candidate_count`.
- `verify`: `window_samples`, `improved_threshold` (0.3), `worsened_threshold` (0.1).
- `incident`: `retry_max`, `ttl_seconds_event`, `db_path`.
- `variant`: chọn chế độ vận hành (mục 7).
- `demo`: `ui_dir`, `tts_clips_dir`, `e2e_jsonl`.

---

## 7. Chế độ vận hành (biến thể)

Sửa khối `variant` trong `harness.yaml`:

```yaml
variant:
  detector: adwin            # adwin | isolation_forest
  diagnosis_mode: full        # full | rule_only
  decide_mode: whatif         # whatif | static_priors
  red_team: false             # chỉ thực hành: chèn ứng viên xấu để đo shield
  llm_offline: false          # ép mock-LLM / chạy offline hoàn toàn
```

- **`diagnosis_mode: rule_only`** → chẩn đoán theo **rule xác định**, không gọi LLM
  (chạy được khi offline).
- **`decide_mode: static_priors`** → DECIDE chấm theo prior tĩnh, bỏ mô phỏng what-if.
- **`red_team: true`** → DECIDE chèn ứng viên ngoài ranh an toàn vào *tập đề xuất*;
  quan sát `SafetyShield` chặn để minh họa an toàn. **Không tắt choke — shield vẫn hoạt động.**
- **`llm_offline: true`** → không bao giờ gọi mạng; dùng `MockLLM` (replay xác định).

---

## 8. Chạy trên MQTT broker thật (Mosquitto)

Mặc định vòng demo dùng `InMemoryBus` — không cần broker. Để chạy qua MQTT thật:

1. Cài & khởi động Mosquitto (mặc định `localhost:1883`).
2. Đảm bảo `mapping.yaml → broker.host/port` đúng (mặc định `localhost:1883`).
3. Dùng `bus/client.py` (BusClient/PahoTransport) thay cho `InMemoryBus` trong
   các stage adapter — các khối khác giữ nguyên vì mọi thứ nói chuyện qua interface `bus`.

> Toàn bộ staging giao tiếp QoS 1, không retain, JSON snake_case, envelope có schema
> version trong `bus/envelopes.py`.

---

## 9. Cấu trúc thư mục chính

```
README.md                          # tổng quan

docs/ARCHITECTURE.md               # kiến trúc & cơ chế vận hành chi tiết

docs/HUONG-DAN-CAI-DAT-VA-SU-DUNG.md   # hướng dẫn cài đặt & sử dụng (file này)
config.py, harness.yaml, mapping.yaml
adapters/ bus/ perceive/ history/ diagnose/ knowledge/ llm/
decide/ plant_model/ act/ incident/ verify/ learn/ score/ ui/
harness_loop.py            # vòng demo record/replay
tests/                     # 54 bài kiểm tra (Epic 1-5 + 5 bài trên dữ liệu HAI thật)
```

---

## 10. Xử lý sự cố thường gặp

| Vấn đề | Cách xử lý |
|---|---|
| Lỗi mã hóa `UnicodeEncodeError` (`cp1252`) | Chạy với `PYTHONUTF8=1` (hoặc đặt biến môi trường) |
| Thiếu Python 3.12 | Cài qua uv: `uv python install 3.12` rồi `uv sync` |
| `make e2e` báo "no command executed" | Do `commands` trong `mapping.yaml` bị thừa/lỗi hoặc signal không khớp; kiểm tra `command_for` / `pair_for` |
| Dashboard không mở | Kiểm tra cổng (mặc định 8799), trình duyệt truy cập http://127.0.0.1:<port> |
| LLM không kết nối | Hệ thống **tự hạ cấp** về MockLLM/rule-only; đặt `OPENROUTER_API_KEY` để dùng backend thật |
| Log JSONL phình to | Không append vào chính file đang replay; dùng `--log` khác cho mỗi lần chạy |