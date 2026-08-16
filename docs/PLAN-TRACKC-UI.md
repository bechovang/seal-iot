# PLAN-TRACKC-UI v2.1 — Control room VIEW-ONLY (chi tiết code, user tự code)

> ⚠️ **Ràng buộc luật thi:** lúc chấm team **chỉ được XEM và PHÂN TÍCH** — không thao tác
> tác nghiệp (không request, không approve từ UI). Toàn bộ hướng interactive của bản
> v1 bỏ. UI là **kính lúp** vào một hệ thống **tự chạy**: tự phát hiện → tự điều phối →
> tự phê duyệt theo policy → tự sinh artifact. Showoff = **"không ai bấm gì mà nó vẫn
> đúng"**. Nguồn: MQTT live (probe 2026-08-16: 2Hz, 6/6 device, NORMAL, epoch lag
> ~245s). Broker chết → UI báo đỏ. Zero-build giữ nguyên (SPA 1 file + stdlib).
>
> Mọi địa chỉ hàm/dữ liệu trong doc này đã đối chiếu code hiện tại (commit `b40f59b`
> + working tree). Code mẫu chạy được nguyên văn trừ nơi ghi rõ "điều chỉnh".

## 0. Hiện trạng: ĐÃ CÓ / CÒN THIẾU

Working tree đang có **chưa commit**: `ui/dashboard.py` (+124 dòng) — phần ingest của
plan v1 bạn đã tự làm. Bảng trạng thái (không làm lại phần DONE):

| Việc | Trạng thái | Ghi chú |
|---|---|---|
| `handler` route `task/`→raw envelope, `tool/`, `bridge/` | ✅ DONE (WIP) | giữ nguyên |
| `_ingest_tool` (audit tool/*, marker `tool_failed`) | ✅ DONE (WIP) | còn thiếu: **snapshot chưa trả `"tools"`** |
| `_ingest_task` giàu (approval/finding/report/replans/stage_outputs) | ✅ DONE (WIP) | còn thiếu: **snapshot trả đủ `"tasks"` đã có — ok**; thiếu `"state"` khi task dict không kèm (đã xử lý) |
| `_ingest_bridge` (chip trạng thái) | ✅ DONE (WIP) | còn thiếu: **snapshot chưa trả `"bridge"`**; **TrackCBridge chưa publish heartbeat** |
| `Dashboard(thresholds=…)` | ✅ DONE (WIP) | harness_loop vẫn gọi `Dashboard()` — **chưa truyền thresholds** |
| `_ingest_tele` lưu `quality` | ❌ | cần cho device grid |
| snapshot: `"devices"`, `"tools"`, `"bridge"`, `"thresholds"`, `"policy"`, `"auto_incidents"`, `"clocks"` | ❌ | FE ăn từ đây |
| `orchestration/auto_loop.py` (detector + autopilot) | ❌ | **việc quan trọng nhất** — không có nó không có gì tự chạy |
| `adapters/trackc.py` bridge heartbeat on connect/disconnect | ❌ | |
| `harness_loop.py --trackc-live` (mode chấm: live MQTT + auto-loop + serve UI) | ❌ | `--trackc` hiện tại là one-shot sim, approve trực tiếp `_sup.handle_approval` (ngoài sole publisher) |
| SPA panels mới (FE-1..FE-8) | ❌ | app.html hiện chỉ có card task mỏng + footer SEAL cũ |

## 1. Panel → điểm kỹ thuật → AD → câu thoại

| # | Panel | Điểm kỹ thuật | AD | Câu thoại |
|---|---|---|---|---|
| 1 | Incident feed | detector ngưỡng đóng → bảng đóng severity→priority → idempotent spawn | AD-1/10/11 | "Thiết bị lệch, hệ thống TỰ sinh sự cố theo bảng đóng — spawn idempotent" |
| 2 | Approval gate read-only + TTL + policy chip | bước critical qua cổng, TTL wall-clock; policy=auto duyệt thay người (đi qua sole publisher) | AD-8/5 | "Bước nguy hiểm luôn qua cổng — hôm nay policy tự duyệt, production đặt human không đổi code" |
| 3 | Task card v2 (FSM stepper + stage chips + replan + PARTIAL) | playbook deterministic, back-edge cap, act-fail → replan | AD-2/3/4 | "Agent không tự chọn bước; tool chối → tự re-plan trong cap, hết cap PARTIAL thật" |
| 4 | Device grid 6 thiết bị (value + quality + staleness) | event-time freshness + ingest-truth status + arrival stopped-stream | AD-10 | "Ba lớp nhận biết dữ liệu xấu — clock broker trễ 4 phút cũng không lỏi" |
| 5 | Tool port audit | validate→create→read-back, idempotency `reused` | AD-6 | "Artifact đọc lại xác nhận; retry không đúp" |
| 6 | Bridge health + 2 đồng hồ epoch-lag | wss bridge; vì sao freshness theo event-time | AD-9/10 | "Clock broker trễ — so wall-clock sẽ báo giả" |
| 7 | Trail per task (details) | replay = render only | AD-13 | "Tất cả là JSONL — tua lại được, không tái thực thi" |
| 8 | Footer Track C 13 AD | spine | — | "Mỗi hành vi trên màn hình là một invariant" |

## 2. BACKEND

### 2.1 `orchestration/auto_loop.py` (MỚI — làm đầu tiên)

Module gom 2 class. Detector biến telemetry xấu thành incident (đi qua cửa
`spawn_from_incident` của supervisor — không mint tay); autopilot duyệt approval
**qua `dash.publish_decision`** (dashboard vẫn là sole publisher `approval/<id>`,
AD-5/AD-8 — bus audit y hệt quyết định người).

```python
"""Auto-loop (AD-1/AD-10): telemetry detector -> incident -> task, và approval
autopilot theo policy. Chế độ demo view-only: không ai bấm gì — detector sinh sự
cố theo bảng đóng, autopilot phát quyết định QUA dashboard (sole publisher của
approval/<id>, AD-5/AD-8) nên audit trail giống hệt một quyết định người.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from orchestration.playbooks import SEVERITY_PRIORITY


def _iso(epoch: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# ingest-truth quality -> severity (bảng đóng; không map -> không phải sự cố)
SEVERITY_FOR_QUALITY = {"offline": "high", "error": "high", "missing_ts": "high"}


class AutoLoopDetector:
    """Mỗi tick quét 1 lần: (a) stopped-stream theo arrival-age (ngo lệ wall-clock
    được phép, cùng họ approval TTL — AD-10), (b) quality xấu theo dòng telemetry
    mới nhất mỗi signal (ingest truth). Có sự cố -> spawn_from_incident với
    incident_id theo cửa sổ thời gian -> cùng cửa sổ là idempotent (chỉ raise
    priority), cửa sổ mới mint task mới."""

    def __init__(self, sup, registry, hist, bus=None, window_s: float = 300.0,
                 playbook_id: str = "prepare_inspection", interval: float = 2.0,
                 stale_seconds: float = 30.0, critical_seconds: float = 120.0) -> None:
        self.sup = sup
        self.registry = registry
        self.hist = hist
        self.bus = bus
        self.window_s = window_s
        self.playbook_id = playbook_id
        self.interval = interval
        self.stale_seconds = stale_seconds
        self.critical_seconds = critical_seconds

    def scan(self) -> list[dict]:
        """Một lượt quét (gọi được từ test). Trả về các sự cố vừa nêu lên feed."""
        now = time.time()
        arrivals = self.hist.last_arrival()
        raised: list[dict] = []
        for dev_id, dev in self.registry.devices.items():
            if dev.is_aggregate():
                continue
            severity, reason = self._assess(dev, arrivals, now)
            if not severity:
                continue
            window = int(now // self.window_s)
            inc_id = f"inc-{dev_id}-{window}"
            r = self.sup.spawn_from_incident(inc_id, severity, self.playbook_id)
            ev = {"incident_id": inc_id, "device": dev_id, "severity": severity,
                  "priority": SEVERITY_PRIORITY[severity], "reason": reason,
                  "ts": _iso(now), "task_id": r.get("task_id", ""),
                  "minted": bool(r.get("minted"))}
            raised.append(ev)
            if self.bus is not None:
                self.bus.publish("ops/incident_feed", ev, qos=1)
        return raised

    def _assess(self, dev, arrivals: dict[str, float], now: float):
        # (a) stopped-stream: không dòng nào đến trong critical_seconds
        for sid in dev.signal_ids:
            arr = arrivals.get(sid)
            if arr is not None and now - arr >= self.critical_seconds:
                return "high", f"stopped-stream {sid} silent {now - arr:.0f}s"
        # (b) ingest-truth quality ở dòng mới nhất mỗi signal
        rows = self.hist.recent(dev.signal_ids, limit=len(dev.signal_ids) * 4)
        seen: dict[str, str] = {}
        for sid, _ts, _val, q in rows:          # recent() trả DESC -> giữ mẫu mới nhất
            seen.setdefault(sid, q)
        for sid, q in seen.items():
            if q in SEVERITY_FOR_QUALITY:
                return SEVERITY_FOR_QUALITY[q], f"quality {q} on {sid}"
        return "", ""

    def start(self) -> None:
        def _loop():
            while True:
                try:
                    self.scan()
                except Exception:               # detector chết không được kéo cả watch
                    pass
                time.sleep(self.interval)

        threading.Thread(target=_loop, daemon=True, name="auto-loop").start()


class ApprovalAutopilot:
    """Policy autopilot cho cổng AD-8. Đọc ``dash.snapshot()`` (đúng view-model UI
    đang thấy), thấy AWAITING_APPROVAL -> chờ ``delay`` giây (cho TTL chạy trên
    màn hình) -> PHÁT quyết định qua ``dash.publish_decision``. Không bao giờ gọi
    thẳng supervisor (giữ sole publisher AD-5).

    policy: "auto" duyệt tất (demo mặc định) · "human" không động vào (production,
    cổng chờ người) · "deny-first" từ chối approval ĐẦU của mỗi task (tập kịch bản
    PARTIAL approval_denied)."""

    def __init__(self, dash, policy: str = "auto", delay: float = 4.0,
                 interval: float = 1.0) -> None:
        self.dash = dash
        self.policy = policy
        self.delay = delay
        self.interval = interval
        self._first_seen: dict[str, float] = {}
        self._denied: set[str] = set()
        self.decisions: list[dict] = []          # audit nội bộ cho test/log
        dash.set_policy(policy, delay)

    def poll(self) -> list[dict]:
        acts: list[dict] = []
        for tid, t in self.dash.snapshot().get("tasks", {}).items():
            if t.get("state") != "AWAITING_APPROVAL":
                self._first_seen.pop(tid, None)
                continue
            apr = t.get("approval") or {}
            if not apr.get("approval_id"):
                continue
            t0 = self._first_seen.setdefault(tid, time.time())
            if time.time() - t0 < self.delay:
                continue
            if self.policy == "human":
                continue
            decision = "APPROVED"
            if self.policy == "deny-first" and tid not in self._denied:
                decision = "DENIED"
            r = self.dash.publish_decision(tid, apr["approval_id"], decision)
            if r.get("published"):
                if decision == "DENIED":
                    self._denied.add(tid)
                self._first_seen.pop(tid, None)
                self.decisions.append({"task_id": tid, "decision": decision,
                                       "approval_id": apr["approval_id"]})
                acts.append(self.decisions[-1])
        return acts

    def start(self) -> None:
        def _loop():
            while True:
                try:
                    self.poll()
                except Exception:
                    pass
                time.sleep(self.interval)

        threading.Thread(target=_loop, daemon=True, name="approval-autopilot").start()
```

Ghi chú:
- Không thêm band ngưỡng giá trị ở v1 này — quality + stopped-stream đã đủ cho kịch
  bản BTC (status `offline`/`error`). Muốn thêm sau: bảng `bands:` trong `mapping.yaml`
  `trackc:` rồi đọc qua registry — KHÔNG hardcode trong detector.
- Nếu `orchestration/__init__.py` export kiểu convenience thì thêm
  `from .auto_loop import AutoLoopDetector, ApprovalAutopilot`.

### 2.2 `adapters/trackc.py` — heartbeat (SỬA, nhỏ)

`TrackCBridge` hiện set `on_connect` trong `start()` (chỉ resubscribe) và
`self.connected=True` chủ quan trong `connect()` — paho disconnect thật không ai
biết. Sửa: gắn đủ callback + phát heartbeat (family `bridge/heartbeat`, sole
publisher = bridge):

```python
# thêm import time ở đầu file

    # trong connect(), sau client.ws_set_options/tls_set, TRƯỚC client.connect(...):
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        # paho dropped subscriptions across reconnects — re-arm every (re)connect.
        client.subscribe(f"{self.settings['topic_prefix']}+/telemetry")
        self.connected = True
        self._emit_heartbeat(True)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None) -> None:
        self.connected = False
        self._emit_heartbeat(False)

    def _emit_heartbeat(self, connected: bool) -> None:
        if self._bus is None:
            return
        try:
            self._bus.publish("bridge/heartbeat",
                              {"connected": connected, "ts": _iso_utc(time.time())},
                              qos=1)
        except Exception:  # noqa: BLE001 — heartbeat không được giết callback paho
            pass
```

Chú ý: `start(bus)` hiện gán `self._client.on_message`/`on_connect` — bỏ phần gán
`on_connect` ở đó (đã gán trong `connect()`), giữ `on_message`. Thứ tự khởi động ở
harness: `start(bus)` TRƯỚC `connect()` để heartbeat có bus ngay từ on_connect đầu
tiên. Muốn phát hiện mất nối nhanh hơn: `keepalive=30` → `10` trong `client.connect`.

### 2.3 `ui/dashboard.py` — phần CÒN THIẾU (đã có WIP ingest, chỉ bổ sung)

**(a) `__init__`: thêm feed list**
```python
        self._incfeed: list[dict] = []          # auto_incident feed (FE-1)
```

**(b) `_ingest_tele`: lưu quality + bookmark clock**
```python
        pts.append({"ts_ms": iso_ms(inner.get("ts") or inner.get("ts_ms")),
                    "value": inner.get("value"), "phase": inner.get("phase", ""),
                    "quality": inner.get("quality", "ok")})
        del pts[:-self.series_points]
        now = time.time()
        self._last_tele_at = now
        ts_ms = pts[-1]["ts_ms"] if pts else None
        if ts_ms:
            self._last_tele_ts_ms = max(self._last_tele_ts_ms or 0, ts_ms)
```

**(c) `_ingest_op`: nhánh feed (đặt cạnh các `elif leaf ==` hiện có)**
```python
        elif leaf == "incident_feed":
            self._incfeed.append(dict(inner))
            del self._incfeed[:-100]
            self._markers.append({"ts_ms": iso_ms(inner.get("ts")),
                                  "kind": "auto_incident",
                                  "label": str(inner.get("device") or "?"),
                                  "incident_id": inner.get("incident_id")})
```

**(d) `set_policy` (đặt cạnh `set_publisher`)** — autopilot gọi lúc dựng:
```python
    def set_policy(self, mode: str, delay_s: float) -> None:
        """Approval policy hiển thị lên UI (FE-2): 'auto' demo / 'human' production."""
        self._policy = {"mode": mode, "delay_s": delay_s}
```
và trong `__init__`: `self._policy = {"mode": "human", "delay_s": 0.0}`.

**(e) `snapshot()`: exposure + device grouping.** Trong scope của lock thêm:
```python
            tools = dict(self._tools)
            bridge = dict(self._bridge)
            incfeed = list(self._incfeed)
            policy = dict(self._policy)
            thresholds = dict(self._thresholds)
            last_tele_at = self._last_tele_at
            last_tele_ts_ms = self._last_tele_ts_ms
```
Sau khối lock (trước return), tính device grid + clocks:
```python
        # device grid (AD-10): nhóm theo prefix signal, tuổi tính theo ref_ts = ts LỚN
        # NHẤT across tất cả signal (đúng semantics observer), không dùng wall clock
        ref = max((p["ts_ms"] for s in series.values() for p in s["points"]
                   if p["ts_ms"]), default=None)
        devices: dict[str, dict] = {}
        for sig, s in series.items():
            dev_id = sig.rsplit("_", 1)[0]
            last = s["points"][-1] if s["points"] else {}
            ts_ms = last.get("ts_ms")
            age_s = round((ref - ts_ms) / 1000.0, 1) if (ref is not None and ts_ms) else None
            q = last.get("quality", "ok")
            if q in ("offline", "error", "missing_ts"):
                worst = q
            elif age_s is not None and age_s >= thresholds["critical_seconds"]:
                worst = "stale-critical"
            elif age_s is not None and age_s >= thresholds["stale_seconds"]:
                worst = "stale"
            else:
                worst = "ok"
            d = devices.setdefault(dev_id, {"device": dev_id, "signals": [],
                                            "worst": "ok", "age_s": None,
                                            "value": None, "unit": ""})
            d["signals"].append({"signal": sig, "value": last.get("value"),
                                 "unit": s.get("unit", ""), "quality": q,
                                 "age_s": age_s})
            order = {"ok": 0, "stale": 1, "stale-critical": 2,
                     "error": 3, "offline": 4, "missing_ts": 3}
            if order.get(worst, 0) > order.get(d["worst"], 0):
                d.update(worst=worst, age_s=age_s, value=last.get("value"),
                         unit=s.get("unit", ""))
        # 2 đồng hồ (AD-10): lag epoch của broker so wall clock — minh chứng event-time
        clocks = {
            "tele_ts_ms": last_tele_ts_ms,
            "epoch_lag_s": round((time.time() - last_tele_ts_ms / 1000.0), 1)
                           if last_tele_ts_ms else None,
            "tele_idle_s": round(time.time() - last_tele_at, 1),
        }
```
và trong dict return thêm:
```python
            "tools": tools,
            "bridge": bridge,
            "auto_incidents": incfeed,
            "policy": policy,
            "thresholds": thresholds,
            "devices": list(devices.values()),
            "clocks": clocks,
```

### 2.4 `harness_loop.py` — `--trackc-live` (MỚI) — mode chấm view-only

Thêm args (cạnh `--trackc` hiện có):
```python
    ap.add_argument("--trackc-live", action="store_true",
                    help="control room view-only trên MQTT LIVE: bridge + auto-loop "
                         "detector + approval autopilot; không thao tác gì")
    ap.add_argument("--approval-policy", default="auto",
                    choices=["auto", "human", "deny-first"],
                    help="policy cổng phê duyệt trong --trackc-live (default auto)")
    ap.add_argument("--approval-delay", type=float, default=4.0,
                    help="giây chờ trước khi autopilot phát quyết định (cho TTL hiện)")
```
Block mới đặt TRƯỚC `if args.trackc:` (và dashboard section ở trên phải chạy trước —
chú ý hiện `dash` chỉ dựng khi `serve_ui or watch`; điều kiện đó đổi thành
`if args.serve_ui or args.watch or args.trackc_live:`):
```python
    if args.trackc_live:
        from config import TrackCRegistry, load_mapping as _lm
        from history import HistoryBuffer
        from orchestration import TaskStore, Supervisor
        from orchestration.auto_loop import AutoLoopDetector, ApprovalAutopilot
        from tools import CMMSSim, ToolPort
        from adapters.trackc import TrackCBridge, settings_from_env

        reg = _lm().trackc
        rt = harness.trackc                      # TrackCRuntimeConfig (ngưỡng AD-10)
        store = TaskStore("orchestration/tasks.db")
        cmms = CMMSSim("tools/cmms.db")
        port = ToolPort(cmms, reg, lambda tid: store.get(tid).state if tid else None,
                        db_path="tools/port_keys.db", bus=bus)
        hist = HistoryBuffer("demo/trackc_history.db")
        sup = Supervisor(store, reg, rt, bus=bus, port=port, history=hist,
                         cmms=cmms, mode="live")
        sup.subscribe(bus)
        dash.set_publisher(bus)
        dash.set_policy(args.approval_policy, args.approval_delay)
        Dashboard_kwargs = {}                    # (đã truyền thresholds ở (f) dưới)

        br = TrackCBridge(reg, settings_from_env())
        br.start(bus)                            # bind bus TRƯỚC connect để có heartbeat
        if not br.connect():
            print("MQTT bridge: KHÔNG kết nối được broker — kiểm tra mqtt.env "
                  "(UI vẫn chạy, chip sẽ đỏ)")
        detector = AutoLoopDetector(sup, reg, hist, bus=bus,
                                    stale_seconds=rt.stale_seconds,
                                    critical_seconds=rt.critical_seconds)
        autopilot = ApprovalAutopilot(dash, policy=args.approval_policy,
                                      delay=args.approval_delay)
        detector.start()
        autopilot.start()
        print(f"trackc-live: policy={args.approval_policy} delay={args.approval_delay}s "
              f"— control room: http://127.0.0.1:{port_of_srv} — Ctrl-C để dừng")
        try:
            while True:
                _t.sleep(1)
        except KeyboardInterrupt:
            pass
        br.close()
        recorder.close()
        return 0
```
Điểm cần chỉnh khi ghép thật:
- `dash` phải được dựng **có thresholds**: chỗ `Dashboard()` đổi thành
  `Dashboard(thresholds={"stale_seconds": harness.trackc.stale_seconds, "critical_seconds": harness.trackc.critical_seconds, "approve_ttl_seconds": harness.trackc.approve_ttl_seconds, "clarify_ttl_seconds": harness.trackc.clarify_ttl_seconds})`.
- `port_of_srv`: biến `port` tên đang trùng với ToolPort `port` — đặt `ui_port = args.serve_ui or 8765` khi dựng server.
- Telemetry từ bridge vào bus nhưng **chưa ai ghi HistoryBuffer** (detector đọc hist).
  Cần 1 subscriber ghi hist: `bus.subscribe("tele/", lambda t, p: hist.write(p.get("signal_id") or t.rsplit("/",1)[-1], _iso_ms(p.get("ts")), float(p.get("value") or 0), p.get("quality","ok")))` — viết hàm `_hist_ingest(topic, payload)` riêng cho sạch (memo: payload tele là envelope `TelemetryEnvelope`, đọc đúng trường).
- `import time as _t` đã có sẵn trong nhánh watch — kéo lên dùng chung.
- (P2, tùy chọn) `--trackc` one-shot cũ đang gọi `_sup.handle_approval` trực tiếp —
  sửa qua `dash.publish_decision` cho đúng AD-5 một cách nhất quán.

### 2.5 (đối chiếu) các API mình gọi — đã có sẵn, không sửa
- `Supervisor.spawn_from_incident(incident_id, severity, playbook_id, drive=True)` →
  `{"task_id","state","minted","priority"|"priority_raised"}` — idempotent theo
  `store.live_pair(incident_id, playbook_id)`.
- `SEVERITY_PRIORITY = {critical:SAFETY, high:URGENT, medium:URGENT, low:ROUTINE, info:ROUTINE}`.
- `HistoryBuffer.recent(signal_ids, limit)` → list `(signal_id, ts_epoch_ms, value, quality)` DESC; `last_arrival()` → `{signal_id: wall_s}`.
- `TrackCRuntimeConfig`: `stale_seconds=30, critical_seconds=120, approve_ttl_seconds=300, clarify_ttl_seconds=600`.
- `dash.publish_decision(task_id, approval_id, "APPROVED"|"DENIED")` → publish `approval/<task_id>`.
- Tool event envelope: `env["tool"]`, `env["payload"]={"id","reused","reason",…}` (đã khớp `_ingest_tool` WIP của bạn; nếu key khác khi chạy test 4 thì chỉnh đúng theo `tools/port.py`).

## 3. FRONTEND `ui/app.html` — READ-ONLY, KHÔNG MỘT NÚT

Nguyên tắc: **không `<button>`, không POST, không form**. Chỉ `fetch("?json=1")` GET 1s/lần như hiện tại. Panel toggle là `<details>` (view, không thao tác).

### 3.1 Header: bridge chip + policy chip + 2 đồng hồ (sửa `renderHeader`)
```js
function renderHeader(s){
  // ... giữ nguyên variant/red/elapsed/tiles ...
  var br=s.bridge||{}, ck=s.clocks||{}, pol=s.policy||{};
  var bh=document.getElementById("bridgeChip");
  var live=br.connected && (ck.tele_idle_s==null||ck.tele_idle_s<10);
  var silent=br.connected && ck.tele_idle_s!=null && ck.tele_idle_s>=10;
  bh.textContent= live?("MQTT ● LIVE · "+(s.counters.tele||0)+" tele")
    :(silent?"MQTT ● SILENT "+ck.tele_idle_s+"s":"MQTT ✕ DISCONNECTED");
  bh.className="chip"+(live?"":(silent?" red":" red"));
  var pc=document.getElementById("policyChip");
  pc.textContent="approval policy: "+esc(pol.mode||"human")+
    (pol.mode==="auto"?" · auto sau "+esc(pol.delay_s)+"s":" · chờ người duyệt");
  var lc=document.getElementById("lagChip");
  lc.textContent="broker epoch "+(ck.epoch_lag_s!=null?("−"+Math.round(ck.epoch_lag_s)+"s vs wall"):"–");
}
```
HTML header thêm 3 span chip `id="bridgeChip" "policyChip" "lagChip"`. Khi
`bridge.connected===false` → thêm banner đỏ trên đầu `wrap`:
`document.getElementById("banner").style.display = ...` (thêm `<div id="banner">`).

### 3.2 FE-1 Incident feed (panel mới, đặt trên Track C tasks)
```js
function renderFeed(s){
  var el=document.getElementById("feed");
  var f=(s.auto_incidents||[]).slice().reverse();
  if(!f.length){el.innerHTML="<div class='empty'>chưa có sự cố tự phát hiện — stream đang lành</div>";return;}
  el.innerHTML=f.slice(0,20).map(function(e){
    var sev=e.severity==="high"||e.severity==="critical"?"b-red":"b-gold";
    return "<div class='card'><div class='card-h'><span class='iid'>"+esc(e.incident_id)+
      "</span><span class='badge "+sev+"'>"+esc(e.severity)+" → "+esc(e.priority)+"</span>"+
      "<span class='muted'>"+esc(e.ts||"")+"</span></div>"+
      "<div class='muted'>"+esc(e.reason)+" · task "+esc(e.task_id)+
      " · "+(e.minted?"auto-spawned (mới)":"đã có task — chỉ nâng priority (idempotent)")+"</div></div>";
  }).join("");
}
```
HTML: `<h2>Auto incidents · detector → task (AD-1)</h2><div class="panel" id="feed"></div>`.

### 3.3 FE-2/3 Task card v2 + approval gate read-only (thay `renderTasks`)
```js
var FSM=["RECEIVED","PLANNING","COORDINATING","AWAITING_APPROVAL","EXECUTING","VERIFYING","REPORTED"];
var TERM={"PARTIAL":"b-red","FAILED":"b-red","CANCELLED":"b-pink"};
function renderTasks(s){
  var el=document.getElementById("tdtasks");
  var thr=(s.thresholds||{}).approve_ttl_seconds||300, pol=s.policy||{};
  var tasks=(s.tasks?Object.keys(s.tasks).map(function(k){return s.tasks[k];}):[]).reverse();
  if(!tasks.length){el.innerHTML="<div class='empty'>no coordination tasks yet</div>";return;}
  el.innerHTML=tasks.map(function(t){
    var state=t.state||"RECEIVED";
    var term=TERM[state];
    var fsmHtml=FSM.map(function(st,i){
      var on=state===st, done=FSM.indexOf(state)>i;
      return "<span class='fnode "+(on?"on":(done?"done":""))+"'>"+st.slice(0,4)+"</span>";
    }).join("<span class='flink'>→</span>")+ (term?"<span class='fnode term'>"+state+"</span>":"");
    // stage chips theo agent (AD-4) + latency event-time
    var so=t.stage_outputs||[]; var chips=""; var prev=null;
    so.forEach(function(o){
      var d=prev?((Date.parse(o.ts)-prev)/1000):0; prev=Date.parse(o.ts);
      chips+="<span class='chip' style='border-color:var(--blue)'>"+esc(o.stage)+"·"+esc(o.agent)+
        (d>0?("<span class='num'> +"+d.toFixed(1)+"s</span>"):"")+"</span> ";
    });
    // approval gate READ-ONLY (FE-2)
    var gate="";
    if(state==="AWAITING_APPROVAL"&&t.approval){
      var apr=t.approval; var left=(Date.parse(apr.ts)/1000+thr)-(Date.now()/1000);
      gate="<div class='card' style='border-color:var(--gold);margin-top:8px'>"+
        "⚠ CỔNG PHÊ DUYỆT · "+esc(apr.stage)+" · "+esc(apr.device)+"<br>"+
        "<span class='muted'>options: "+(apr.options||[]).join(" | ")+" · TTL còn "+
        "<b class='num'>"+Math.max(0,Math.round(left))+"s</b> (AD-8)</span><br>"+
        "<span class='muted'>policy "+esc(pol.mode||"human")+(pol.mode==="auto"?
          " — autopilot sẽ phát approval/"+esc(apr.approval_id)+" sau "+esc(pol.delay_s)+"s":
          " — chờ người duyệt (production)")+"</span></div>";
    }
    var rp=(t.replans>0)?("<span class='badge b-gold' style='background:#2b270d;color:var(--gold)'>↺ "+
      esc(t.replan_stage)+"→"+esc(t.replan_target)+" "+(t.replan_attempts||t.replans)+" lần</span>"):"";
    var fr=(t.fail_reason?("<div class='muted' style='color:var(--red)'>✗ "+esc(t.fail_reason)+"</div>"):"");
    var body="";
    if(t.finding)body+="<div class='muted'>finding: "+esc(t.finding)+"</div>";
    if(t.report)body+="<div class='muted'>report: "+esc(t.report)+"</div>";
    var trail=(t.events||[]).slice(-12).map(function(e){return "<span class='tchip' style='background:#2b3a55'>"+esc(e)+"</span>";}).join(" ");
    return "<div class='card'><div class='card-h'><span class='iid'>"+esc(t.task_id)+
      "</span>"+(term?("<span class='badge "+term+"'>"+state+"</span>"):"<span class='badge b-pink'>"+esc(state)+"</span>")+rp+
      "<span class='muted'>"+esc(t.ts||"")+"</span></div>"+
      "<div class='fsm' style='margin:6px 0'>"+fsmHtml+"</div>"+
      "<div style='margin:6px 0'>"+chips+"</div>"+gate+fr+body+
      "<div style='margin-top:6px'>"+trail+"</div></div>";
  }).join("");
}
```
(Khi autopilot phát quyết định, task đổi state → gate tự biến mất, events hiện
`approval_granted` — không cần code thêm.)

### 3.4 FE-4 Device grid (panel mới cạnh chart)
```js
function renderDevices(s){
  var el=document.getElementById("devices");
  var ds=(s.devices||[]); var thr=s.thresholds||{};
  if(!ds.length){el.innerHTML="<div class='empty'>chưa có telemetry</div>";return;}
  el.innerHTML=ds.map(function(d){
    var col=d.worst==="ok"?"var(--green)":(d.worst==="stale"?"var(--amber)":"var(--red)");
    var age=d.age_s!=null?d.age_s:0;
    var w=Math.min(100,(age/((thr.critical_seconds||120)*1.0))*100);
    var sigs=(d.signals||[]).map(function(g){
      return "<span class='chip'>"+esc(g.signal.split("_").pop())+" "+fmt(g.value)+
        " <b style='color:"+(g.quality==="ok"?"var(--green)":"var(--red)")+"'>●</b></span>";
    }).join(" ");
    return "<div class='card' style='border-color:"+(d.worst==="ok"?"var(--line)":col)+"'>"+
      "<div class='card-h'><span class='iid'>"+esc(d.device)+"</span>"+
      "<span class='badge' style='background:"+col+";color:#0b101b'>"+esc(d.worst)+"</span>"+
      "<span class='muted'>age "+esc(d.age_s)+"s (event-time)</span></div>"+
      "<div class='barwrap' style='width:100%'><i class='bar' style='width:"+w+"%;background:"+col+"'></i></div>"+
      "<div style='margin-top:6px'>"+sigs+"</div></div>";
  }).join("");
}
```
HTML: `<h2>Devices · quality + staleness (AD-10)</h2><div class="panel"><div class="grid3" id="devices"></div></div>`.

### 3.5 FE-5 Tool port audit (panel mới)
```js
function renderTools(s){
  var el=document.getElementById("tools");
  var ts=(s.tools?Object.keys(s.tools).map(function(k){return s.tools[k];}):[]);
  if(!ts.length){el.innerHTML="<div class='empty'>chưa có artifact nào qua Tool port</div>";return;}
  el.innerHTML=ts.map(function(t){
    var ev=(t.events||[]).slice(-6).map(function(e){
      var bad=e.event==="failed";
      return "<span class='chip' style='"+(bad?"border-color:var(--red);color:var(--red)":"")+"'>"+
        esc(e.event)+(e.reused?" ⟲ reused":"")+(bad?(" — "+esc(e.reason)):"")+"</span>";
    }).join(" ");
    return "<div class='card'><div class='card-h'><span class='iid'>"+esc(t.kind)+"</span>"+
      "<span class='badge "+(t.last==="failed"?"b-red":"b-green")+"'>"+esc(t.last)+"</span>"+
      "<span class='muted'>id "+esc(t.id)+"</span></div><div>"+ev+"</div></div>";
  }).join("");
}
```
HTML: `<h2>Tool port audit · validate → create → read-back (AD-6)</h2><div class="panel" id="tools"></div>`.

### 3.6 FE-8 Footer Track C (thay `renderFooter`)
```js
function renderFooter(){
  var arch=["MQTT wss","tele/*","history","detector","supervisor task/*","tool port tool/*","dashboard view-only"];
  document.getElementById("arch").innerHTML=arch.map(function(a,i){
    return "<span class='a'>"+a+"</span>"+(i<arch.length-1?"<span class='ar'>→</span>":"");
  }).join("")+"<span class='ar'>·</span><span class='a' style='color:var(--gold)'>approval policy → approval/<id> (autopilot route)</span>";
  var ads=["AD-1 single-queue supervisor","AD-2 task FSM","AD-3 deterministic playbooks","AD-4 agents=LLM contexts",
    "AD-5 sole publishers","AD-6 tool port read-back","AD-8 approval TTL","AD-9 broker quarantined",
    "AD-10 event-time freshness","AD-11 registry identity","AD-12 no cmd from task layer","AD-13 replay=render"];
  document.getElementById("adr").innerHTML=ads.map(function(a){return "<span class='adb'>"+a+"</span>";}).join("");
}
```
(sửa chuỗi `<id>` thành `&lt;id&gt;` khi viết HTML — hoặc dùng "approval/&lt;id&gt;".)

### 3.7 `tick()` — thêm các section mới
```js
    if(lastSections.feed!==JSON.stringify(s.auto_incidents||[]))renderFeed(s);
    if(lastSections.devices!==JSON.stringify([s.devices,s.thresholds]))renderDevices(s);
    if(lastSections.tools!==JSON.stringify(s.tools||{}))renderTools(s);
    if(lastSections.header!==JSON.stringify([s.variant,s.counters,s.elapsed_s,s.bridge,s.clocks,s.policy]))renderHeader(s);
```
(và gán lại `lastSections.*` tương ứng sau khi render — theo pattern hiện có.)

### 3.8 Guard view-only (quan trọng)
Cuối cùng rà: `app.html` phải **không chứa** `<button`, `method:`, `api/request`,
`api/decision`. Test 8 dưới chặn hồi quy. POST endpoints giữ ở backend cho
rehearsal/dev (không phá AD-5), UI không gọi bao giờ.

## 4. Demo script 8 phút — KHÔNG BÀN PHÍM, KHÔNG CHUỘT

| Phút | Trên màn hình (tự xảy ra) | Câu thoại |
|---|---|---|
| 0–1 | Chip `MQTT ● LIVE 2Hz`; device grid 6 thiết bị nhảy; lag chip `broker epoch −245s` | "Dữ liệu thật từ broker BTC. Từ giây này tôi không đụng gì nữa" |
| 1–2 | (BTC đổi stream / rehearsal: sim `status=offline`) → Incident feed sáng `▲ inc-line_xx high→URGENT` → task card tự mở RECEIVED | "Bảng đóng severity→priority; spawn idempotent — trigger lại chỉ nâng priority" |
| 2–3 | Stage chips `observe·observer → plan·maintenance…` tự chạy; finding hiện | "Playbook deterministic — mỗi agent một context, không ai tự chọn bước" |
| 3–4 | Approval gate hiện TTL đếm ngược; ~4s sau events hiện `approval_granted`; EXECUTING→VERIFYING→REPORTED; tool audit sáng `wo ⟲/✓ read-back` | "Bước critical có cổng + TTL + audit; policy demo tự duyệt — production đặt human, không đổi code" |
| 4–6 | Kịch bản act-fail (env `CMMS_FLAKY=1` hoặc `--approval-policy deny-first` cho PARTIAL approval_denied): tool panel `failed` đỏ ⇄ badge `↺ act→plan 1/2` → hết cap → PARTIAL + reason | "Từ chối là thật: tự re-plan trong cap, không success ảo" |
| 6–7 | Chỉ device grid + lag chip: 3 lớp staleness; device offline viền đỏ giữ | "Event-time vs arrival vs ingest-truth — clock broker trễ cũng không lỏi" |
| 7–8 | Mở trail `<details>`; footer 13 AD | "Mọi diễn biến là JSONL — replay là render. Hệ tự trị, người chỉ xem và phân tích" |

Chạy tập: `PYTHONUTF8=1 uv run python harness_loop.py --trackc-live --approval-policy auto --approval-delay 4 --serve-ui 8765`. Tab phụ `?json=1` trả lời câu "có state ẩn không" — không có, tất cả là bus events. Fallback nếu BTC không kích sự cố: replay JSONL ghi từ rehearsal vào dashboard (AD-13 render-only — vẫn là xem).

## 5. TESTS (thêm vào `tests/test_trackc_orchestration.py` / `test_trackc_dashboard.py`)

1. **`test_auto_loop_detector_spawns_idempotent`** — fabric registry 1 device 2 signal;
   `hist.write(..., quality="offline")`; `d.scan()` 2 lần cùng window (patch
   `time.time` cố định hoặc gọi trong cùng 1s) → store có đúng 1 task live, call 2
   `minted=False`; severity `high` → priority `URGENT`; bus có 2 frame
   `ops/incident_feed`.
2. **`test_auto_loop_detector_new_window_mints_new_task`** — window_s=0.1, sleep qua
   window → task thứ 2 minted=True (chứng minh cửa sổ + không trùng lặp vĩnh viễn).
3. **`test_autopilot_approves_via_sole_publisher`** — dựng fabric như
   `test_request_driven_path_end_to_end_runs_unchanged` (dashboard + set_publisher(bus));
   autopilot delay=0; `poll()` → task tới REPORTED; **bus.messages có frame
   `approval/<task_id>`** (chứng minh đi qua publisher, không phải gọi supervisor).
4. **`test_autopilot_policy_human_waits`** — policy `human`, drive task tới
   AWAITING_APPROVAL, `poll()` vài lần → vẫn AWAITING_APPROVAL, `decisions==[]`.
5. **`test_autopilot_deny_first_partials`** — policy `deny-first`, delay=0 → task
   PARTIAL, fail_reason `approval_denied`, bus có frame DENIED.
6. **`test_snapshot_exposes_tools_bridge_devices_policy`** — publish tele (quality
   `offline`) + tool frame + bridge frame + `set_policy("auto", 4)` → snapshot có
   `tools[...].events`, `bridge.connected`, `devices[].worst=="offline"`,
   `policy.mode=="auto"`, `thresholds`, `clocks.epoch_lag_s` số.
7. **`test_bridge_heartbeat_publishes_on_disconnect`** — TrackCBridge với fake paho
   client (stub `_client` có attribute gọi được) hoặc gọi thẳng
   `_on_disconnect(...)` sau `start(bus)` → bus có `bridge/heartbeat connected=False`.
8. **`test_spa_has_no_operator_controls`** —
   ```python
   def test_spa_has_no_operator_controls():
       html = Path("ui/app.html").read_text(encoding="utf-8")
       assert "<button" not in html.lower()
       assert "api/decision" not in html and "api/request" not in html
       assert "method:" not in html  # không POST gì từ UI
   ```

Acceptance: `PYTHONUTF8=1 uv run pytest tests/ -q` xanh; loop nhanh
`PYTHONUTF8=1 uv run pytest tests/test_trackc_*.py -q` (~1s). Thủ công:
`--trackc-live` để yên → thấy task tự spawn → tự approval → REPORTED; tắt mạng 10s →
chip DISCONNECTED rồi tự LIVE lại.

## 6. Rủi ro & chú ý

- **Ai tạo sự cố lúc chấm?** BTC điều khiển stream. Nếu BTC không kích trong phiên →
  detector im lặng (đúng thiết kế, không spam) → tập trước fallback replay JSONL.
- **Detector spam**: cửa sổ 300s/device + idempotent spawn đã chặn; KHÔNG hạ
  `window_s` dưới 60s trong demo.
- **`hist` vs bus**: bridge phát `tele/*` trên bus nhưng detector đọc `hist` — bắt
  buộc có subscriber ghi hist (mục 2.4) nếu không detector mù. Kiểm tra bằng log
  `hist.count()` lúc dựng.
- **Autopilot chạy dựa trên `dash.snapshot()`**: nếu dashboard chết thì không ai
  duyệt — chấp nhận được (dashboard là thành phần demo), nhưng nhớ `snapshot()` rẻ
  (in-memory) — poll 1s ok.
- **`_ingest_task` state từ `approval_granted`**: sau granted, state trong store là
  EXECUTING nhưng task dict không nằm trong payload của event đó — nếu card còn
  hiển thị AWAITING_APPROVAL quá 1 tick thì thêm nhánh: `ev=="approval_granted"` →
  `rec["state"]="EXECUTING"` (đoạn 1 dòng trong `_ingest_task`).
- **paho reconnect**: heartbeat dựa on_disconnect; muốn nhanh set keepalive=10.
- **Chart D/Z SEAL cũ** giữ nguyên — kể chuyện nền HAI nếu BTC hỏi.
- **`Dashboard()` không thresholds** = TTL/bar sai số mặc định — nhớ truyền (2.4).

## 7. Thứ tự làm + commit

1. `orchestration/auto_loop.py` + tests 1–5 (hệ thống tự chạy — việc quan trọng nhất)
2. `dashboard` phần còn thiếu (2.3) + test 6 — nửa buổi
3. `harness_loop --trackc-live` + bridge heartbeat (2.2/2.4) + test 7 — dựng được
   control room live, tập kịch bản
4. FE panels (3.1–3.7) + test 8
5. Tập demo theo mục 4 hai lần, bấm giờ

Commit gợi ý (tách 2 commit cho gọn):
- `feat(orchestration): auto-loop — telemetry detector → idempotent incident spawn + approval policy autopilot qua sole publisher (AD-1/AD-5/AD-8)`
- `feat(ui): view-only Track C control room — incident feed, approval gate read-only + TTL, task FSM/replan viz, device staleness grid, tool audit, bridge health + epoch clocks; SPA zero controls`

(+ trailer `Co-Authored-By: Claude <noreply@anthropic.com>` như cũ.)
