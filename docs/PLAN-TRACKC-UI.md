# PLAN-TRACKC-UI — Frontend "control room" demo showoff (2026-08-16)

> Mục tiêu: biến những điểm kỹ thuật **đã có thật trong backend** thành thứ **nhìn thấy và
> bấm được** trong 8 phút demo. Nguồn dữ liệu: **MQTT live** (đã probe lại 2026-08-16:
> 2Hz, 6/6 thiết bị, `scenario=NORMAL`, epoch lag ~245s — stream ổn). Broker chết thì
> UI phải **báo đỏ rõ ràng** (chip + banner), không im lặng.
>
> Nguyên tắc: giữ zero-build (SPA 1 file `ui/app.html` + stdlib backend, không npm).
> Mọi panel mới phải map được sang một AD trong spine — đó chính là "câu thoại" khi demo.

## 0. Hiện trạng — đã có gì, thiếu gì

**Backend đã có sẵn (không cần viết mới):**
- `POST /api/request` → `request/in` (dashboard là sole publisher AD-5) — `ui/dashboard.py:479`
- `POST /api/decision` → `approval/<task_id>` — `ui/dashboard.py:483`
- Supervisor auto-drive (P0-1 đã fix): request publish → task tự chạy tới AWAITING_APPROVAL
- `tele/*` payload **đã mang `quality`** (`bus/client.py:53`) — SPA chưa đọc
- `tool/<tool>/<event>` (invoked/result/failed, ToolEvent) — dashboard **chưa ingest**,
  rơi vào `_ingest_op` thành stage_count rác
- Task snapshot có `state` FSM đúng (đã fix), nhưng chỉ lưu tên event — mất approval_id,
  options, evidence, finding, replan count, reason PARTIAL

**SPA đã có:** pipeline ribbon (loop SEAL cũ), SVG chart D/Z, incidents FSM, diagnose/
decide/shield/verify/learn (loop cũ), task card mỏng (chỉ event chips), trail.

→ Demo Track C hiện **không có chỗ bấm**, không thấy approval, không thấy tool port,
không thấy thiết bị offline. Đó là khoảng trống cần lấp.

## 1. Bảng mapping: điểm kỹ thuật → panel → AD → câu thoại demo

| # | Panel mới/nâng cấp | Điểm kỹ thuật sau sau | AD | Câu thoại khi demo |
|---|---|---|---|---|
| 1 | **Request console** (gửi request từ UI) | request-driven + playbook auto-match; guard về generic khi id lạ | AD-3/AD-5 | "Operator nói tiếng người, hệ thống tự chọn playbook từ menu đóng" |
| 2 | **Approval console** (card + nút APPROVE/DENY + TTL) | human-in-the-loop bắt buộc; port artifact `apr_`; TTL wall-clock | AD-8 | "Bước critical phải qua người — không có đường tắt" |
| 3 | **Task card v2**: FSM stepper + stage chips theo agent + **vòng replan** + PARTIAL reason | playbook deterministic + back-edge cap; act-fail → replan → PARTIAL | AD-2/AD-3 | "Tool từ chối thì hệ thống tự re-plan, hết cap thì PARTIAL thành thật" |
| 4 | **Device grid** 6 thiết bị: value + quality + staleness bar | event-time freshness + ingest-truth status + **arrival-based stopped-stream** | AD-10 | "Offline không cần chờ age — broker nói là thật; stream dừng thì arrival clock phát hiện" |
| 5 | **Tool port audit**: validate→create→read-back, `reused` idempotency | port là sole writer; read-back verify; idempotency key | AD-6 | "Mọi artifact tạo xong phải đọc lại xác nhận; retry không bao giờ đúp" |
| 6 | **Bridge health chip + 2 đồng hồ** | wss bridge + epoch-lag → vì sao freshness theo event-time | AD-9/AD-10 | "Clock của broker trễ 4 phút — so wall-clock sẽ báo giả; ta dùng event-time" |
| 7 | **Trail/replay per task** (expand payload) | replay = render only; full audit | AD-13 | "Toàn bộ diễn biến là một log JSONL tua lại được, không tái thực thi" |
| 8 | **Footer AD map đúng Track C** | spine 13 AD | — | "Mỗi hành vi trên màn hình là một invariant được viết sẵn" |

## 2. Backend nhỏ (ui/dashboard.py + adapters/trackc.py) — làm trước

### BE-1 · Ingest `tool/*` (`_ingest_tool`)
Route **trước** `_ingest_op` trong `handler()` (nhánh `topic.startswith("tool/")`):

```python
    def _ingest_tool(self, topic: str, inner: dict) -> None:
        """tool/<kind>/<event> -> audit trail cho Tool port panel (AD-6)."""
        parts = topic.split("/")
        kind = parts[1] if len(parts) > 2 else parts[-1]
        evt = parts[-1]
        p = inner.get("payload") if isinstance(inner.get("payload"), dict) else {}
        rec = self._tools.setdefault(inner.get("tool") or kind,
                                     {"kind": kind, "events": [], "last": "", "id": ""})
        rec["events"].append({"event": evt, "id": p.get("id", ""),
                              "reused": bool(p.get("reused")),
                              "reason": p.get("reason", ""), "ts": inner.get("ts", "")})
        del rec["events"][:-64]
        rec["last"] = evt
        rec["id"] = p.get("id") or rec["id"]
        self._counters["tool"] = self._counters.get("tool", 0) + 1
        if evt == "failed":
            self._markers.append({"ts_ms": iso_ms(inner.get("ts")), "kind": "tool_failed",
                                  "label": kind, "incident_id": p.get("task_id")})
        self._prune(self._tools, cap=100)
```

(thêm `self._tools: "OrderedDict[str, dict]" = OrderedDict()` trong `__init__`;
`snapshot()` trả thêm `"tools": dict(self._tools)`.) ToolPort đã publish đủ
`invoked/result/failed` — không sửa `tools/port.py`.

### BE-2 · Device grid data: đọc `quality` + age theo event-time
`_ingest_tele` thêm `quality: inner.get("quality", "ok")` vào point; `snapshot()` nhóm
thêm `"devices"`: `signal_id.rsplit("_",1)[0]` → {signal mới nhất: value/unit/quality/
ts_ms, age_ms = max_ts_tele − ts}. Không dùng wall-clock ở backend — age so với **mới
nhất across signals** (ref_ts đúng như observer). SPA tự tô màu.

### BE-3 · Bridge health: `bridge/heartbeat` (sole publisher = TrackCBridge)
Trong `TrackCBridge`: gán thêm `on_disconnect`/`on_connect` → `bus.publish(
"bridge/heartbeat", {"connected": bool, "ts": iso_now})`. Là family mới — ghi memlog:
`decision: topic family bridge/* sole-published by TrackCBridge (AD-5 mở rộng cho
source health)`. Dashboard ingest → snapshot `"bridge": {"connected":…, "since":…}`.
Ngoài ra đếm tele 2Hz: nếu `counters.tele` không tăng ≥10s mà bridge connected →
chip vàng "stream im lặng" (đúng nghĩa stopped-stream ở tầng hệ thống).

### BE-4 · Task snapshot giàu dữ liệu (cho card v2)
`_ingest_task` lưu thêm (nhỏ, có chọn lọc — đừng nhét cả payload):
- `approval`: từ event `approval_requested` → `{approval_id, options, stage, device, ts}`
  (SPA cần approval_id để POST `/api/decision`); clear khi `approval_granted/denied`
- `finding`: từ `stage_done` stage `observe` → `payload.output.finding`
- `report`: từ `stage_done` stage `report` → `payload.output.summary`
- `replans`: đếm event `replan` + `failed_stage` cuối; `fail_reason`: từ `closed` PARTIAL
- `stage_outputs`: list `{stage, agent, ts}` cho latency bar (delta ts event-time)
`snapshot()` trả `tasks` như cũ + các field mới.

## 3. Frontend (ui/app.html) — theo thứ tự demo-value

### FE-1 · Request console (đỉnh trang, cạnh pipeline) — *làm đầu tiên*
```html
<div class="panel" id="reqbox">
  <textarea id="reqText" rows="2" placeholder="vd: prepare inspection of motor 1 — kịch bản 1"></textarea>
  <select id="reqPb"><option value="">auto-match</option><option>prepare_inspection</option>
    <option>conflict_assessment</option><option>line_inspection_timeout</option><option>generic</option></select>
  <select id="reqPri"><option value="">playbook default</option><option>ROUTINE</option>
    <option>URGENT</option><option>SAFETY</option></select>
  <button onclick="sendReq()">▶ Send request</button>
  <span class="muted">dashboard là sole publisher của request/in (AD-5)</span>
</div>
```
```js
function sendReq(){
  fetch("/api/request",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({text:document.getElementById("reqText").value,
      playbook_id:document.getElementById("reqPb").value||null,
      priority:document.getElementById("reqPri").value||null})})
  .then(function(r){return r.json();}).then(function(j){/* flash chip gửi xong */});
}
```
+ 3 nút preset (Kịch bản 1/2/3) điền sẵn text+playbook+priority — để demo không gõ tay.

### FE-2 · Approval console — *khoảnh khắc đắt nhất của demo*
Ngay trên Task card v2: khi `task.state === "AWAITING_APPROVAL"` và `task.approval`:
```html
<div class="approval">
  <div class="card-h">⚠ PHÊ DUYỆT BƯỚC CRITICAL · <b>{stage}</b> · thiết bị {device}</div>
  <div class="muted">evidence: {finding rút gọn + N signals, stale count}</div>
  <div class="muted">TTL còn <span id="ttl-{task_id}">mm:ss</span> (AD-8 wall-clock)</div>
  <button class="ok" onclick="decide('{tid}','{approval_id}','APPROVED')">✓ APPROVED</button>
  <button class="no" onclick="decide('{tid}','{approval_id}','DENIED')">✗ DENIED</button>
</div>
```
`decide()` POST `/api/decision`. TTL countdown chạy client-side từ `approval.ts`
(+ttl đọc từ config, demo để 120s cho nhìn được — set `ttl_seconds` khi dựng store).
DENIED → card chuyển PARTIAL đỏ ngay — nhấn mạnh "người nói không, máy nghe".

### FE-3 · Task card v2 (thay `renderTasks`)
- **FSM stepper**: `RECEIVED→PLANNING→COORDINATING→AWAITING_APPROVAL→EXECUTING→VERIFYING→REPORTED`
  (style lại `renderIncidents` đang có — node on/done/term; PARTIAL/FAILED/CANCELLED term đỏ).
- **Stage chips theo agent**: mỗi stage_done 1 chip `stage·agent` (observe·observer,
  adjudicate·production…) — thấy ngay "6 agent contexts khác nhau" (AD-4).
- **Replan loop**: badge `↺ act→plan 1/2` màu vàng; hết cap → badge PARTIAL + reason.
- **Finding + report** hiển thị text (bằng chứng observer + summary supervisor).
- **Latency bar event-time** mỗi stage (delta ts giữa stage_done liên tiếp).

### FE-4 · Device grid (thay panel chart cho demo Track C — chart D/Z giữ cho loop cũ)
6 card thiết bị: tên · signal value mới nhất (num + unit) · dot màu quality
(ok=green/error=amber/offline=red) · bar age_ms theo thang stale_seconds/critical_seconds
(runtime config đẩy sang SPA qua snapshot thêm `"thresholds"`). Device nào offline →
card viền đỏ nhấp nháy 1 lần. Đây là chỗ diễn "arrival-based stopped-stream": stream
dừng → tele ngừng → age kéo dài theo ref_ts + chip "no new samples".

### FE-5 · Tool port audit panel
Từ `snapshot.tools`: mỗi kind một card: `work_order wo_0003 ✓ result` · badge `reused`
khi idempotency hit (demo retry an toàn) · dòng đỏ `failed: <reason>` khi port từ chối
(đúng lúc act-fail → replan ở task card — hai panel sáng cùng lúc = câu chuyện AD-6).

### FE-6 · Bridge health + 2 đồng hồ (header)
- Chip `MQTT ● LIVE 2Hz` (xanh) / `DISCONNECTED` (đỏ + banner full-width) từ BE-3.
- Nhỏ, bên phải header: `broker epoch −245s vs wall-clock` —**khoan** nói: "lag này là
  lý do freshness theo event-time" (AD-10). Tính = now − max(ts_ms tele).

### FE-7 · Trail per task
Trong card v2, `<details>` cuối: timeline đầy đủ event + `<details>` lồng cho payload
(finding, evidence list, replan reason). Câu AD-13: "đây là replay-as-render".

### FE-8 · Footer đúng Track C
Thay list AD SEAL cũ bằng: AD-1 single-queue supervisor · AD-2 task FSM · AD-3 playbook
deterministic + back-edge · AD-4 agent = LLM context · AD-5 sole-publisher topics ·
AD-6 tool port read-back · AD-8 approval TTL · AD-9 broker quarantined · AD-10
event-time freshness · AD-11 registry identity · AD-12 task≠cmd · AD-13 replay=render.
Sửa luôn dòng arch footer: `MQTT wss bridge → tele/* → history → observer → supervisor
(task/*) → tool port (tool/*) → dashboard → approval → …` (vòng kín human-in-loop).

## 4. Demo script 8 phút (theo hướng showoff, đã tập dượt)

| Phút | Hành động | Trên màn hình | Câu thoại |
|---|---|---|---|
| 0–1 | Mở control room; MQTT LIVE chip; device grid 6 thiết bị nhảy 2Hz | FE-4/FE-6 | "Dữ liệu thật từ broker BTC, MQTT-over-WebSocket; mọi thứ sau đây là phản ứng tự động" |
| 1–2 | Bấm preset **Kịch bản 1** (prepare_inspection) | Task card chạy: RECEIVED→…stage chips lần lượt→AWAITING_APPROVAL | "Playbook deterministic — agent không tự chọn bước tiếp" |
| 2–3 | Đọc evidence, bấm **APPROVE** | approval biến mất, EXECUTING→VERIFYING→REPORTED; tool panel sáng work_order ✓ read-back | "Tool port: validate→create→read-back, idempotent" |
| 3–4 | Preset **Kịch bản 2** (conflict_assessment, URGENT) rồi bấm **DENIED** | PARTIAL đỏ + reason approval_denied | "Người trong vòng — từ chối là từ chối, có audit" |
| 4–6 | **Kịch bản showoff kỹ thuật**: kích act-fail (env demo `CMMS_FLAKY=1` hoặc gọi 2 lần cùng request idempotency-key sai) | replan `↺ act→plan 1/2` → chạy lại → hết cap → PARTIAL thật; tool panel failed đỏ | "Hệ thống tự re-plan theo back-edge có cap — không retry mù, không success ảo" |
| 6–7 | Chỉ device grid: giải thích event-time + epoch lag; (nếu bật kịch bản offline) LINE_01 viền đỏ | FE-4 + FE-6 | "Freshness theo event-time; offline là ingest-truth; stopped-stream theo arrival — 3 lớp" |
| 7–8 | Mở trail details; tóm tắt footer AD; (nếu còn giờ) `make trackc --trackc-approve deny` terminal chạy song song | FE-7/FE-8 | "Toàn bộ audit được — replay là render, không tái thực thi" |

Ghi chú kỹ thuật tập dượt: TTL 120s; `PYTHONUTF8=1`; mở sẵn `?json=1` tab phụ để thuyết
minh "không có state ẩn — tất cả là bus events".

## 5. Tests & acceptance

Thêm vào `tests/test_trackc_dashboard.py`:
1. `test_snapshot_exposes_tools_audit` — publish vài tool/* frames (invoked/result +
   failed có reason) → `snap["tools"]` đủ kind/last/id, marker `tool_failed` tồn tại.
2. `test_devices_grid_quality_grouping` — publish tele quality "offline" cho
   `line_01_current` → `snap["devices"]["line_01"]` mang quality offline.
3. `test_task_snapshot_carries_approval_and_replans` — chạy fabric tới AWAITING_APPROVAL
   → `snap["tasks"][tid]["approval"]["approval_id"]` khớp store; ép replan → counter.
4. `test_bridge_heartbeat_publishes_on_disconnect` — bridge với stub client gọi
   `on_disconnect` → bus có `bridge/heartbeat` connected=False.
5. `test_api_decision_roundtrip_via_serve` (nếu chưa có) — POST /api/decision với id
   thật → task rời AWAITING_APPROVAL.

Acceptance: `PYTHONUTF8=1 uv run pytest -q` toàn xanh; thủ công `make ui` + POST request
từ console browser thấy task chạy tới AWAITING_APPROVAL và bấm APPROVE được tới REPORTED;
rút cáp/tắt wifi → chip DISCONNECTED trong ~5s (keepalive 30s — xem mục 6).

## 6. Rủi ro & chú ý

- **paho reconnect**: `loop_start()` tự reconnect; chip đỏ dựa on_disconnect — thử thật
  trước khi demo (tắt wifi 10s). Keepalive 30 → phát hiện chậm nhất ~45s; muốn nhanh hơn
  thì set keepalive 10 trong `connect()`.
- **`_drive` đồng bộ trong POST /api/decision**: stage chạy LLM (mode live, OpenRouter)
  có thể block request vài giây — chấp nhận được; nếu demo thấy lag thì chuyển
  `handle_approval` sang `_spawn_drive` (fire-and-forget) + SPA poll state.
- **Suite full chậm** (148s lần gần nhất): đã đo bằng `--durations` — riêng các file
  `tests/test_trackc_*.py` chỉ ~1.1s/44 test, tức bản fix autodrive **không** làm chậm;
  thời gian nằm ở các test LLM/network (epic4–6). Cần chạy nhanh khi lặp UI:
  `PYTHONUTF8=1 uv run pytest tests/test_trackc_*.py -q`.
- **Chart D/Z cũ** giữ nguyên — đừng xóa: còn dùng kể chuyện loop SEAL (HAI) nếu BTC hỏi
  về phần nền tảng cũ.

## 7. Thứ tự làm (đề xuất, ước lượng)

1. BE-1→BE-4 (dashboard data) + tests — ~nửa buổi
2. FE-1 request console + FE-2 approval console — demo đã "sống"
3. FE-3 task card v2 (FSM stepper + replan badge)
4. FE-4 device grid + BE-2 quality/age
5. FE-5 tool audit + FE-6 bridge health (BE-3)
6. FE-7/FE-8 trail + footer; tập theo script mục 4 hai lần

Commit gợi ý: `feat(ui): Track C control room — interactive request/approval console,
task FSM + replan viz, device staleness grid, tool port audit, bridge health` (+ trailer
Co-Authored-By như cũ).
