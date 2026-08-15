# SEAL IoT Smart Ops Harness

**Hệ thống vận hành thông minh tự-phục-hồi cho nhà máy IoT (SEAL Summer 2026)**

Hệ thống liên tục **cảm nhận (Perceive)** tín hiệu nhà máy, **chẩn đoán (Diagnose)**, quyết định và thực thi hành động **có kiểm soát/kèm xác minh (Guarded Action & Verification)**, rồi **tự học (Self-Learning)** từ kết quả — qua đó phát hiện, xử lý và nhớ lại cách xử lý sự cố, góp phần giảm downtime và nguy cơ.

---

## Tổng quan

| Thuộc tính | Giá trị |
|---|---|
| Ngôn ngữ | Python 3.12 (quản lý bằng `uv`) |
| Giao tiếp giữa các khối | MQTT (Mosquitto) — QoS 1 |
| Lưu trữ | SQLite (WAL) cho history buffer & incident |
| Chẩn đoán | Runbook (Jaccard) → Causal graph → RCA → Debate gate (LLM tùy chọn) |
| Quyết định | Objective: `alpha·safety + beta·energy − gamma·downtime − delta·risk` |
| An toàn | `SafetyShield` xác định (không bao giờ bị tắt bởi cờ biến thể) |
| Bộ dữ liệu | HAI 21.03 (virtual plant) |
| Cờ biến thể | `adwin`/`isolation_forest`, `full`/`rule_only`, `whatif`/`static_priors`, `red_team`, `llm_offline` |

---

## Kiến trúc phân khối

```
                     ┌──────────────────────────────────────────────┐
         telemetry   │                                              │
 HAI  ──►  ingest  ──┼─►  PERCEIVE ─► DIAGNOSE ─► DECIDE ─► GUARDED ─┘
 (adapter) single-writer │  (EMA / ADWIN /      │        │     ACT/safety
          history buffer │   Isolation Forest)  ▼        ▼      + VERIFY
                     ┌───┴───────────────────────► INCIDENT FSM ─► SELF-LEARN
                     │ (event-time, D/Z pairs)      (SQLite)     (runbook+metrics)
                     └──► UI dashboard + TTS (subscribe-only) + JSONL recorder
```

**Các khối (module):**

| Module | Vai trò |
|---|---|
| `config.py`, `mapping.yaml`, `harness.yaml` | Cấu hình trung tâm + mô hình tín hiệu chuẩn + knobs |
| `adapters/` | Adapter bộ dữ liệu HAI → telemetry chuẩn hóa |
| `bus/` | Transport MQTT, các "envelope" phiên bản hóa, recorder JSONL |
| `history/` | History buffer (SQLite WAL) — **single-writer** |
| `perceive/` | Phát hiện bất thường (EMA/ADWIN + Isolation Forest) |
| `diagnose/` | Runbook match, causal graph, RCA, debate gate |
| `knowledge/` | Kho runbook (mã thông báo triệu chứng đóng) |
| `llm/` | Client LLM (OpenRouter / MockLLM) + budget wall-clock |
| `decide/` | Sinh ứng viên (`do(∅)` + ≥2) + chấm điểm mục tiêu |
| `plant_model/` | Mô hình nhà máy (dự đoán hiệu quả, hiệu chuẩn online) |
| `act/` | `SafetyShield` (xác định) + executor (người publish duy nhất) |
| `incident/` | FSM sự cố + lưu trữ SQLite |
| `verify/` | Phân loại kết quả theo event-time (improved/no_change/worsened) |
| `learn/` + `score/` | Với mỗi sự cố + bảng điểm thực hành |
| `ui/` | Dashboard subscribe-only + TTS tiếng Việt |
| `harness_loop.py` | Vòng demo record/replay (nền tảng cho `make e2e`) |

Chi tiết đầy đủ về cơ chế vận hành và kiến trúc: **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)**.
Hướng dẫn cài đặt & sử dụng: **[`docs/HUONG-DAN-CAI-DAT-VA-SU-DUNG.md`](docs/HUONG-DAN-CAI-DAT-VA-SU-DUNG.md)**.

---

## Bắt đầu nhanh

Yêu cầu: **Python 3.12**, **uv**, và (tùy chọn) **Mosquitto** cho môi trường MQTT thật.

```bash
uv sync                      # cài dependencies (3.12.12)

PYTHONUTF8=1 make lint       # kiểm tra cú pháp (bắt buộc trên Windows)
PYTHONUTF8=1 make test       # chạy toàn bộ 54 bài kiểm tra (49 logic + 5 dữ liệu HAI thật)
PYTHONUTF8=1 make e2e        # ghi + replay một sự cố mẫu qua JSONL rồi smoke-assert
```

Chạy vòng lặp mẫu kèm dashboard và TTS:

```bash
PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl --serve-ui 8799 --tts
# mở http://127.0.0.1:8799 để xem dashboard trực tiếp
```

---

## Luồng hoạt động tổng quát (1 vòng sự cố)

1. **PERCEIVE** — phát hiện lệch lạc tín hiệu (EMA/ADWIN hoặc Isolation Forest) → tạo `Episode` có bằng chứng (score, innovation, window stats).
2. **DIAGNOSE** — tìm runbook khớp theo **mã triệu chứng đóng** (Jaccard). Nếu miss → dựng cụm nhân quả (lag-correlation/Granger) → RCA xếp hạng giả thuyết (tận dụng **phân kỳ D/Z**) → debate proposer/critic/arbiter gating.
3. **DECIDE** — sinh ít nhất **2 ứng viên thật + `do(∅)`** (không hành động), chấm điểm mục tiêu; LLM **không bao giờ** quyết định giá trị điều khiển.
4. **Guard + ACT** — `SafetyShield` xác định kiểm tra ranh an toàn/phạm vi vật lý; **executor là người publish duy nhất** trên `cmd/*`.
5. **VERIFY** — phân loại kết quả theo **event-time** (post vs baseline). Chỉ `improved` mới **resolve** sự cố; không cải thiện → retry hoặc escalate.
6. **SELF-LEARN** — khi resolve: **chưng cất** runbook mới + ghi metric cho từng sự cố; `score/` dựng bảng điểm thực hành (P/R/F1, MTTD, MTTR, độ chính xác RCA, tỷ lệ hành động mất an toàn…).

**Ràng buộc then chốt (AD-1…AD-14)** — xem chi tiết trong `docs/ARCHITECTURE.md`.

---

## Trạng thái triển khai

| Epic | Nội dung | Trạng thái |
|---|---|---|
| 1 | Nền tảng & Cảm nhận nhà máy | ✅ |
| 2 | Chẩn đoán & bộ nhớ runbook | ✅ |
| 3 | Hành động có kiểm soát & xác minh | ✅ |
| 4 | Tự học & đánh giá | ✅ |
| 5 | Demo, khả năng phục hồi & trình bày | ✅ |

Toàn bộ repo có **54 bài kiểm tra** (`PYTHONUTF8=1 make check`): 49 bài kiểm tra logic/an toàn + **5 bài test trên dữ liệu HAI 21.03 thật**
(chạy trên `tai lieu/bo-du-lieu/03-HAI/`, tự bỏ qua nếu thiếu file). Một dòng thời gian sự kiện duy nhất, không dùng wall-clock trong pipeline.