# PLAN-TRACKC-DEMO-UI — Showoff frontend + sửa data-story (2026-08-16)

> **Bối cảnh:** digital-twin sim đã chạy được (`--trackc-sim full_show`, chip SIM ● LIVE,
> 2Hz, epoch lag 245s). Nhưng snapshot sống của bản chạy đầy đủ cho thấy **3 vấn đề**
> làm giảm giá trị buổi chấm — và nửa màn hình là panel trống. Plan này: sửa data-story
> trước (đúng story), dọn giao diện sau (đẹp story). User tự code, từng mục độc lập.
>
> **Bằng chứng từ bản chạy 2026-08-16 04:49 (snapshot `?json=1`):**
> - 24 task (23 REPORTED + 1 PARTIAL) cho một full_show lẽ ra ~4 task.
>   `inc-gas_01-…` mint **7 task**, `inc-line_01-…` **6**, `inc-press_01-…` **4** —
>   tất cả trong CÙNG window 300s → mâu thuẫn trực tiếp với câu thoại "idempotent,
>   không mint task 2".
> - Feed `auto_incidents` đầy 100 dòng, phần lớn là cùng incident lặp mỗi tick 2s
>   ("silent 36s → 38s → 40s…") → khoảnh khắc escalate bị chìm.
> - 5 panel legacy trống hoàn toàn (`diagnoses/decisions/shield/verifies/learn` = 0)
>   + chart trộn 403V với 0.77 m/s trên cùng một trục Y.
> - **Race S2:** dòng SAFETY trong feed đến từ task *mint mới* với severity critical
>   (`minted: true`), không phải raise trên task đang sống — vì task đã REPORTED trước
>   khi offline+stale đủ 20s để escalate.

## 0. Giữ nguyên / sửa / thêm

| Giữ nguyên | Sửa | Thêm |
|---|---|---|
| `adapters/trackc_scenarios.py` (player + beats) | `orchestration/task_store.py` (+`any_pair`) | feed theo chuyển tiếp (trong detector) |
| `harness_loop.py` nhánh `--trackc-sim` | `orchestration/supervisor.py` (`spawn_from_incident`) | SPA: syncSections, sparkline, banner SAFETY, flash |
| guard test zero-button, sole publisher AD-5 | `orchestration/auto_loop.py` (`scan()` phát theo chuyển tiếp) | dashboard: counters `operator_inputs`/`auto_decisions` |
| `parse_payload`/`build_payload`, bridge, autopilot | `ui/app.html` (G1+G3+G4 + polish), `ui/dashboard.py` (nhỏ) | ~5 test mới |
| beats S1/S3/S4 | beats S2 (tinh chỉnh race, mục 4) | |

## 1. G2 — Chữa data-story (làm TRƯỚC, mọi thứ khác phụ thuộc vào nó)

### 1.1 `orchestration/task_store.py` — thêm `any_pair` (~8 dòng, sau `live_pair`)

```python
@_sync
def any_pair(self, source_incident_id: str, playbook_id: str) -> Optional["Task"]:
    """Task GẦN NHẤT theo (incident, playbook) ở MỌI state — kể cả terminal.
    Cho spawn_from_incident biết cửa sổ này ĐÃ có một chu kỳ xử lý (AD-1)."""
    row = self._conn.execute(
        "SELECT * FROM tasks WHERE source_incident_id=? AND playbook_id=? "
        "ORDER BY updated DESC LIMIT 1",
        (source_incident_id, playbook_id),
    ).fetchone()
    return self._row_to_task(row) if row else None
```
(Không đụng index `idx_live_pair` — nó vẫn đúng cho tính chất "một task sống".)

### 1.2 `orchestration/supervisor.py` — `spawn_from_incident`: 1 window = 1 task

Sau nhánh `live is not None` (giữ nguyên — raise trên task đang sống), THÊM:

```python
prior = self.store.any_pair(incident_id, playbook_id)
if prior is not None:
    # Cửa sổ này đã có một chu kỳ xử lý (task đã đóng) -> KHÔNG mint lại.
    # Sự cố còn kéo dài sẽ được cửa sổ kế tiếp (window id mới) nhận — giống
    # back-edge cap AD-3: hết cap thì PARTIAL, không retry mù.
    return {"task_id": prior.task_id, "state": prior.state, "minted": False,
            "window_done": True}
```
Giữ: severity phải nằm trong `SEVERITY_PRIORITY` (bảng đóng); nhánh live vẫn
`raise_priority_if_lower` + trả `priority_raised`.

### 1.3 `orchestration/auto_loop.py` — feed chỉ phát KHI CÓ CHUYỂN TIẾP

`scan()` hiện phát `ops/incident_feed` mỗi tick cho mỗi device xấu → spam. Sửa:
giữ `self._last: dict[dev, tuple]` = `(severity, task_id)` lần trước; chỉ publish khi:

| Chuyển tiếp | Điều kiện | Entry thêm field |
|---|---|---|
| `detected` | `"" → severity` | `kind: "detected"`, `minted`, `task_id` |
| `escalated` | severity tăng hạng (high→critical) | `kind: "escalated"`, `priority_raised`, `window_done` |
| `recovered` | `severity → ""` | `kind: "recovered"`, `task_id` (task đã đóng) |

```python
RANK = {"low": 0, "high": 1, "critical": 2}          # cục bộ, đủ dùng cho so sánh
...
prev = self._last.get(dev_id, ("", ""))
if severity != prev[0]:
    kind = ("detected" if not prev[0] else
            "recovered" if not severity else
            "escalated" if RANK.get(severity, 0) > RANK.get(prev[0], 0) else "downgraded")
    ev = {... như cũ ..., "kind": kind,
           "priority_raised": bool(r.get("priority_raised")),
           "window_done": bool(r.get("window_done"))}
    raised.append(ev); self.bus.publish(...)          # CHỈ trong nhánh này
self._last[dev_id] = (severity, r.get("task_id", ""))
```
Vẫn **gọi `spawn_from_incident` mỗi tick** cho device xấu (đường raise phải chạy),
chỉ việc *phát feed* theo chuyển tiếp. Kết quả full_show: ~4 detected + 1 escalated +
4 recovered ≈ 9 dòng thay vì 100.

### 1.4 `ui/dashboard.py` — 2 counter cho HUD tự chủ (~6 dòng)

- `self._counters["auto_decisions"] = 0` trong `__init__`; tăng trong
  `publish_decision()` sau khi publish thành công.
- `self._counters["operator_inputs"] = 0`; tăng trong `_do_post` của `serve()`
  (mọi POST `/api/decision` `/api/request`) — tức CHỈ đếm thao tác từ ngoài vào.
  Demo auto policy: số này đứng im ở **0** → tile "thao tác người: 0" là SỐ THẬT.

## 2. G1 — Dọn sân khấu (`ui/app.html`)

### 2.1 Bọc section + `syncSections(s)` — panel rỗng tự ẩn

Mỗi khối `<h2>+panel` bọc thành `<section id="sec-…">`; thêm 1 hàm chạy đầu mỗi `tick`:

```js
function syncSections(s){
  var empty = function(n){ return !n; };
  var pipe = s.pipeline_counts || {};
  var pipeOn = Object.keys(pipe).some(function(k){ return pipe[k] > 0; });
  var hasDZ = Object.keys(s.signals || {}).some(function(k){ return /_(D|Z)$/.test(k); });
  var trackc = (s.auto_incidents || []).length > 0;
  var vis = {
    "sec-pipeline": pipeOn,
    "sec-chart":    hasDZ,                       // Track C không có _D/_Z -> ẩn chart trộn
    "sec-incidents": !trackc,                    // feed mới thay thế panel legacy
    "sec-diagnose": empty, "sec-decide": empty, "sec-shield": empty,
    "sec-verify": empty, "sec-learn": empty,     // điều kiện: dữ liệu tương ứng rỗng
  };
  Object.keys(vis).forEach(function(id){
    var el = document.getElementById(id); if (!el) return;
    var on = typeof vis[id] === "function" ? vis[id](s) : vis[id];
    el.style.display = on ? "" : "none";
  });
}
```
(Điều kiện từng panel legacy: `!(s.diagnoses && Object.keys(s.diagnoses).length)`…
viết thẳng cho từng panel.) Biến HAI cũ có data → panel tự hồi sinh — không phá
chế độ demo kia.

### 2.2 HUD tiles — thông điệp autonomous nằm ngay header

Sửa `renderHeader`: tiles thành
`tele · tasks · tool · quyết định tự chủ (auto_decisions) · thao tác người (operator_inputs · luôn 0 lúc chấm)` —
tile "thao tác người" tô xanh khi = 0 (`0 = view-only đạt chuẩn`). Thêm tile
`tasks` = `Object.keys(s.tasks||{}).length`.

### 2.3 Việt hóa tiêu đề panel (chỉ panel Track C)

- `Auto incidents · detector → task (AD-1)` → **"Sự cố tự phát hiện · detector → task (AD-1)"**
- `Track C · coordination tasks…` → **"Nhiệm vụ phối hợp · FSM · phê duyệt · tái kế hoạch"**
- `Devices · quality + staleness (AD-10)` → **"Thiết bị · chất lượng & độ tươi dữ liệu (AD-10)"**
- `Tool port audit…` → **"Kiểm toán Tool port · validate → create → read-back (AD-6)"**
- `Event trail` → **"Dòng sự kiện trên bus"**

## 3. G3 + G4 — Nhấn khoảnh khắc & chart nhỏ

### 3.1 Banner SAFETY (S2) — tạm thời, tự tắt (~15 dòng)

Thêm `<div id="safetyBanner" style="display:none; …viền đỏ, nền #3a0d0d…">` ngay trên
`#banner`. Trong `tick()`, giữ `seenSafety` set: entry feed mới có `priority==="SAFETY"`
chưa seen → hiện banner ~8s (`setTimeout` ẩn) với text:
`⛔ SAFETY · {incident_id} — CÙNG cửa sổ, không mint task mới, chỉ nâng priority`.
Đây là render thuần — không phải control (guard test không vi phạm).

### 3.2 Flash card task khi đổi state (~10 dòng)

Giữ `lastStates = {}` (task_id → state). Trong `renderTasks`, card nào có
`lastStates[tid] !== state` → thêm class `flash` (CSS `@keyframes` viền sáng 1.2s,
giữ className `fnode on` hiện tại). Cập nhật `lastStates` sau khi render.

### 3.3 Small-multiples: sparkline trong từng card thiết bị

Bỏ phụ thuộc chart lớn (đã ẩn ở 2.1). Trong `renderDevices`, mỗi signal vẽ 1
sparkline SVG từ `s.signals[sig].points` (đã có 240 điểm, đã có `quality`):

```js
function spark(pts, unit){
  if (!pts || pts.length < 2) return "";
  var vs = pts.map(function(p){ return p.value; });
  var lo = Math.min.apply(null, vs), hi = Math.max.apply(null, vs);
  if (hi === lo) hi = lo + 1;
  var W = 150, H = 26;
  var d = pts.map(function(p, i){
    var x = i / (pts.length - 1) * W;
    var y = H - 2 - (p.value - lo) / (hi - lo) * (H - 6);
    return x.toFixed(1) + "," + y.toFixed(1);
  }).join(" ");
  // dải đỏ dưới các điểm quality xấu (offline/error) — bằng chứng thị giác
  var bad = "";
  pts.forEach(function(p, i){
    if (p.quality && p.quality !== "ok")
      bad += "<rect x='" + (i / pts.length * W - W / pts.length / 2).toFixed(1) +
             "' y='0' width='" + (W / pts.length + 0.6).toFixed(1) +
             "' height='" + H + "' fill='rgba(255,77,77,.18)'/>";
  });
  return "<svg width='" + W + "' height='" + H + "' viewBox='0 0 " + W + " " + H + "'>" +
         bad + "<polyline points='" + d + "' fill='none' stroke='#4ea1ff' stroke-width='1.4'/>" +
         "</svg>";
}
```
Mỗi signal trong card: dòng `tên · giá trị · đơn vị` + sparkline bên dưới. Xét
nghiệm: S1 motor offline → dải đỏ hiện đúng khoảng beat; S3 line omitted →
sparkline line_01 "đứt" (không điểm mới) trong khi 5 thiết bị khác vẫn chạy.

### 3.4 Polish nhìn được ngay (dễ nhìn)

- `header{position:sticky;top:0;z-index:50;backdrop-filter:blur(6px)}` — chips +
  tiles luôn trên màn khi cuộn.
- `?big=1` (URL param, đọc `location.search`): `body{zoom:1.18}` — chiếu máy chiếu,
  không cần zoom tay (không phải control trong trang).
- Font cơ sở 13 → 14px; `.tile b` 16 → 18px.
- Card feed: hover không đổi cursor (giữ tinh thần view-only, không click-bait).

## 4. Tinh chỉnh beats S2 (race escalate-vs-terminal)

Vấn đề: press error t110 → mint ~t112; offline+omit t120 → cần 20s im lặng mới
critical (~t141). Task có thể REPORTED trước đó → escalate rơi vào nhánh
`window_done` (không còn task để nâng) → story S2 biến mất.

2 vít vặn (chọn 1, xác nhận bằng test 5.3):

1. **Rút ngắn đường tới critical**: `--sim-critical-seconds 12` (flag đã có) →
   critical ~t133, sát hơn; hoặc omit ngay sau error: `Beat(113,"offline")`,
   `Beat(113.5,"omit")` → critical ~t126 (task 14s tuổi, còn đang
   AWAITING_APPROVAL/EXECUTING).
2. **Kéo dài vòng đời task**: `--approval-delay 8` → cổng chờ lâu hơn, task sống
   qua mốc escalate (TTL 300s vẫn thoải mái).

Kỳ vọng cuối (tiêu chí nghiệm thu full_show sau khi fix G2 + tune S2):

| Số liệu | Giá trị đúng |
|---|---|
| Tổng task | **đúng 4** (motor, press, line, gas) — không remint |
| gas_01 | 1 task, terminal **PARTIAL**, có `↺` badge |
| press_01 | 1 task, feed có 1 dòng `escalated` với `priority_raised: true` |
| Feed entries | ~9 (4 detected + 1 escalated + 4 recovered) |
| Tile "thao tác người" | 0 suốt buổi |

## 5. Tests (~5 mới + guard)

1. `test_any_pair_and_window_suppression` — mint + đóng task (REPORTED) →
   `spawn_from_incident` cùng inc_id trả `minted: False, window_done: True`,
   số task không tăng; inc_id khác (window mới) mint bình thường.
2. `test_detector_feed_transitions_only` — detector + sup thật + hist giả theo
   tick: fault giữ nguyên qua 3 scan → 1 entry `detected`; sang critical → +1
   `escalated`; hết fault → +1 `recovered`; tổng không tăng theo scan.
3. `test_press_escalate_raises_on_live_task` — fast-forward beats S2 (clock giả,
   critical_seconds ngắn): entry `escalated` có `priority_raised: true` VÀ
   `window_done: false` (nâng trên task đang sống — đúng story S2).
4. `test_operator_inputs_zero_under_autopilot` — chạy autodrive + autopilot auto:
   `snapshot()["counters"]["operator_inputs"] == 0`, `auto_decisions >= 1`.
5. `test_spa_has_safety_banner_sparkline_sections` — static: `app.html` chứa
   `id="safetyBanner"`, hàm `spark(`, `syncSections`, `<section id="sec-…`;
   guard cũ (`test_spa_has_no_operator_controls`) vẫn xanh.
   Lưu ý: test nào đang assert feed tăng theo từng scan thì sửa theo語 nghĩa mới.

Acceptance: `PYTHONUTF8=1 uv run pytest tests/ -q` xanh toàn bộ; chạy
`--trackc-sim full_show --fresh` 2 lần → bảng kỳ vọng mục 4 giống hệt cả 2 lần.

## 6. Thứ tự làm + chạy + commit

1. **G2 backend** (1.1 → 1.3) + test 1–2 → chạy `full_show --fresh` kiểm "đúng 4 task".
2. Tune S2 (mục 4) + test 3 → kiểm dòng escalated `priority_raised: true`.
3. **G1** (2.1–2.3) + **G3** (3.1–3.2) + test 4–5.
4. **G4** (3.3) + polish (3.4) — soi bằng mắt trên `full_show`.
5. Counters (1.4) gộp vào bước 3.

Chạy kiểm mỗi bước (server phải khởi động lại — SPA đọc 1 lần lúc import; browser Ctrl+F5):

```
set PYTHONUTF8=1
uv run python harness_loop.py --trackc-sim full_show --approval-policy auto --approval-delay 5 --sim-critical-seconds 20 --fresh --serve-ui 8765
```

Commit gợi ý: `feat(demo): showoff UI theo PLAN-TRACKC-DEMO-UI — window-suppressed spawn (1 window=1 task), feed theo chuyển tiếp, ẩn panel rỗng, HUD 0 thao tác người, banner SAFETY + flash, sparkline từng thiết bị` (+ trailer `Co-Authored-By: Claude <noreply@anthropic.com>`).

## 7. Rủi ro / lưu ý

- `any_pair` trả task gần nhất theo `updated` — nếu sau này muốn cho phép retry có
  kiểm soát, thêm flag `allow_reopen` thay vì sửa ngược `live_pair`.
- Ẩn panel theo điều kiện dữ liệu: điều kiện phải là "có data thì hiện" (không hard-code
  theo biến thể) để demo HAI cũ không bị phá.
- `zoom` CSS chỉ chạy chuẩn trên Chromium/Edge — trình duyệt chấm dự kiến Edge/Chrome,
  nếu lo thì bỏ `?big=1` (chỉ là phần thưởng, không phải tính năng).
- Feed bớt dòng nhưng KHÔNG bớt证据: mọi tick vẫn vào `recent` (event trail) và
  JSONL record — mất khả năng "im lặng từng giây" chỉ ở FEED HIỂN THỊ, không mất audit.
- Sau khi sửa `app.html`: **phải restart harness** (SPA cache ở module-level `_SPA`).
