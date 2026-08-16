# PLAN-TRACKC-FIXES — Kế hoạch sửa lỗi sau review + live MQTT (2026-08-16)

> Nguồn: review code Epic 0–6 (~3.500 dòng) + dò live broker (`docs/MQTT-DATA-TRACKC.md`).
> Mục tiêu: **trước khi demo/chấm**, hệ thống phải chạy được 3 kịch bản theo đúng spine
> (AD-1/3/6/8/9/10). Tất cả mã dưới đây là gợi ý hoàn chỉnh — copy được, nhưng đọc hiểu
> trước khi dán (đây là bài của mình, không phải của Claude).

## Tổng quan — 3 P0, 2 P1, 6 P2

| # | Mức | Vấn đề | Hệ quả khi demo | File |
|---|-----|--------|-----------------|------|
| 1 | **P1** | `settings_from_env` map cả full topic `MQTT_TOPIC=.../test/telemetry` vào `topic_prefix` | subscribe/publish sai topic | `adapters/trackc.py` |
| 2 | **P1** | `TrackCBridge.start()` gán `on_message = lambda *a: None` | **đường live chết hoàn toàn** — nhận message mà không ingest | `adapters/trackc.py` |
| 3 | **P0** | Không có gì gọi `advance()` cho task từ request/incident | task đứng yên ở RECEIVED, dashboard "không chạy" | `orchestration/supervisor.py` |
| 4 | **P0** | `_apply_action` vứt kết quả `ToolResult` — act không bao giờ fail | replan/PARTIAL không bao giờ xảy ra, giao diện "success ảo" | `orchestration/supervisor.py` |
| 5 | **P0** | Observer không thấy `status=offline/error` và không thấy stream DỪNG (event-time vẫn "fresh") | Kịch bản 3 (device timeout) không detect | `orchestration/agents/observer.py` + `history/buffer.py` |
| 6 | P2 | dashboard `_ingest_task` ghi đè `state` bằng **agent name** | card task hiện sai state FSM | `ui/dashboard.py` |
| 7 | P2 | action agent lấy `device=None` cho playbook generic | work_order bị port từ chối (device không trong registry) | `orchestration/agents/action.py` |
| 8 | P2 | `port.list_by_key` loop 5 kind lặp cùng 1 lookup | semantics sai (AD-6 list-before-retry) | `tools/port.py` |
| 9 | P2 | Dead code: `STAGE_FOR_AGENT`, `build_generic_playbook` | rối người đọc | supervisor + playbooks |
| 10 | P2 | `ingest_request`: playbook luôn đè priority của request | request URGENT từ UI bị hạ thành ROUTINE | supervisor |
| 11 | **P2-bảo-mật** | `docs/MQTT-GUIDE.md` chứa **password + tk_ key thật** (đã push GitHub) | rò rỉ credential | docs |

**Trình tự làm (dependency):** 1,2 → (buffer arrival) → 5 → 7 → 4 → 3 → các P2 còn lại → test.
Lý do: test "generic end-to-end" (mục 4+3) chỉ pass khi observer đã có default devices (5)
và action đã có device fallback (7).

---

## PHẦN 1 — P1: Bridge live-path (`adapters/trackc.py`)

### 1.1 Helper chuẩn hóa topic → team prefix

Thêm vào sau `_normalize_metric`:

```python
def _team_prefix(topic: str) -> str:
    """Normalize whatever ``MQTT_TOPIC`` holds to the team topic PREFIX. People
    naturally paste the full topic from the invite (``.../test/telemetry``); the
    code needs ``hackathon/<team>/`` because subscribe uses ``<prefix>+/telemetry``
    and publish builds ``<prefix><env>/telemetry``."""
    t = str(topic or "").strip().strip("/")
    for suffix in ("/test/telemetry", "/judge/telemetry", "/telemetry"):
        if t.lower().endswith(suffix):
            t = t[: -len(suffix)]
            break
    return f"{t}/" if t else ""
```

Trong `settings_from_env`, dòng gán `topic_prefix` hiện tại:

```python
    cfg["topic_prefix"] = os.environ.get("MQTT_TOPIC") or vals.get("MQTT_TOPIC", cfg["topic_prefix"])
```

đổi thành:

```python
    cfg["topic_prefix"] = _team_prefix(
        os.environ.get("MQTT_TOPIC") or vals.get("MQTT_TOPIC", cfg["topic_prefix"]))
```

→ `mqtt.env` giữ nguyên `MQTT_TOPIC=hackathon/underrated/test/telemetry` cũng chạy đúng.

### 1.2 Wire `on_message` thật trong `start()`

Thay toàn bộ method `start()` (đang chứa `self._client.on_message = lambda *a: None`)
bằng:

```python
    def start(self, bus) -> None:
        """Start the internal-bus re-publisher. ``bus`` is a ``BusClient`` (or the
        InMemoryBus-driven client). Subscribes to the team's test+judge telemetry
        topics; each incoming contract payload is normalized and re-published on
        ``tele/*``. Offline/test harnesses drive :meth:`ingest_payload` directly."""
        self._bus = bus
        if self.connected:
            self._client.on_message = self._on_message
            self._client.on_connect = self._on_connect
            self._client.subscribe(f"{self.settings['topic_prefix']}+/telemetry")

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        # paho drops subscriptions across reconnects — re-arm on every (re)connect.
        client.subscribe(f"{self.settings['topic_prefix']}+/telemetry")

    def _on_message(self, client, userdata, msg) -> None:
        """paho callback: one broker frame -> canonical envelopes on the internal
        bus. A malformed frame is skipped, never raised — the broker loop must
        survive bad input (AD-9 quarantine)."""
        try:
            raw = json.loads(msg.payload.decode("utf-8", "replace"))
        except ValueError:
            return
        if not isinstance(raw, dict):
            return
        try:
            self.ingest_payload(raw)
        except Exception:  # noqa: BLE001
            pass
```

Chú ý thứ tự khi gọi trong app thật: `connect()` **trước** `start(bus)` (start chỉ
subscribe khi `self.connected`). `on_connect` lo việc re-subscribe khi mạng rớt.

### 1.3 Timestamp fallback phải ra UTC, không phải giờ máy

Trong `parse_payload`, nhánh fallback `timestamp` (khi không có `epoch`) hiện đang là:

```python
                from datetime import datetime

                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts = dt.astimezone().__str__().replace("+00:00", "Z")
```

`astimezone()` không tham số = convert sang **múi giờ máy** (máy VN → +07:00). Sửa:

```python
                from datetime import datetime, timezone

                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:  # naive string -> read as UTC, never local
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
```

### 1.4 `build_payload` nhận `scenario` (optional)

Stream thật có field `scenario` ("NORMAL"), spec thầy gửi thì không. Giữ frame 5 field
khi không cần, thêm khi có — sửa chữ ký (chỉ phần thay đổi):

```python
def build_payload(device_values: dict[str, dict[str, float]], environment: str,
                  team_code: str, epoch: float, status: dict[str, str] | None = None,
                  scenario: str | None = None) -> dict:
```

và cuối hàm, thay `return {...}` bằng:

```python
    frame: dict[str, Any] = {
        "timestamp": _iso_utc(epoch),
        "epoch": int(epoch),
        "environment": environment,
        "teamCode": team_code,
        "devices": devices,
    }
    if scenario:
        frame["scenario"] = scenario
    return frame
```

(`parse_payload` đã nhét `scenario` vào `res.meta` — không phải sửa.)

---

## PHẦN 2 — Groundwork cho P0-3: arrival tracking (`history/buffer.py`)

AD-10: event-time là đồng hồ freshness duy nhất — **nhưng** event-time không thấy được
stream **DỪNG** (thiết bị treo, ts mẫu cuối vẫn "mới" mãi). Cho phép **một ngoại lệ
wall-clock**: ghi lại thời điểm ARRIVAL (lúc nhận mẫu) từng signal, so arrival-vs-now.
Cùng họ với TTL phê duyệt của AD-8. Ghi memlog (mục 5.5).

3 chỉnh trong `HistoryBuffer`:

**(a)** `import time` đầu file (cạnh `import threading`).

**(b)** Trong `__init__`, sau các `CREATE TABLE`:

```python
        # Local wall-clock arrival per signal (seconds). Event-time alone cannot see a
        # STOPPED stream (AD-10); arrival-vs-now is the admitted wall-clock exception,
        # same family as the approval TTL (AD-8). In-memory: a silence detector is a
        # runtime concern, not history.
        self._arrivals: dict[str, float] = {}
```

**(c)** Thêm property + đổi `write()` + thêm `last_arrival()`:

```python
    @property
    def arrivals(self) -> dict[str, float]:
        with _LOCK:
            return dict(self._arrivals)

    # ---- write (ingest consumer only) ----
    def write(self, signal_id: str, ts_epoch_ms: int, value: float, quality: str = "ok",
              arrival: float | None = None) -> None:
        """Append a sample. Caller is responsible for row ordering per signal.
        ``arrival`` overrides the recorded wall-clock arrival (tests simulate silence)."""
        with _LOCK:
            self._conn.execute(
                "INSERT OR REPLACE INTO telemetry(signal_id, ts_epoch_ms, value, quality) "
                "VALUES(?,?,?,?)",
                (signal_id, ts_epoch_ms, value, quality),
            )
            self._arrivals[signal_id] = arrival if arrival is not None else time.time()
            self._trim()

    def last_arrival(self, signal_ids: list[str] | None = None) -> dict[str, float]:
        """Wall-clock arrival seconds for the given signals (all when omitted)."""
        with _LOCK:
            if signal_ids is None:
                return dict(self._arrivals)
            return {s: self._arrivals[s] for s in signal_ids if s in self._arrivals}
```

Ai gọi `write()`? Ingest consumer — tìm chỗ subscribe `tele/` rồi `history.write(...)`
(thực tế không cần truyền gì thêm, `arrival` mặc định = now là đủ; chỉ test mới truyền
`arrival` cũ để giả lập stream dừng).

---

## PHẦN 3 — P0-2: act fail khi Tool port từ chối (`orchestration/supervisor.py`)

Hiện tại trong vòng lặp `advance()`:

```python
            if stage.name == "act":
                self._apply_action(task_id, t, out)
```

`_apply_action` trả `None`, mọi `ToolResult` bị vứt → port từ chối (backend chết,
validation fail) cũng coi như thành công. Sửa 2 chỗ:

**(a)** Thay đoạn trên bằng:

```python
            if stage.name == "act":
                applied = self._apply_action(task_id, t, out)
                if applied.get("failed"):
                    return self.replan_or_partial(
                        task_id, "act", applied.get("reason") or "action_failed")
```

(Lưu ý: `set_cursor(stage_idx+1)` đã chạy trước đó — không sao, `replan_or_partial`
ghi đè cursor về target back-edge `act → plan`.)

**(b)** `_apply_action` trả dict, check từng `ToolResult.ok`:

```python
    def _apply_action(self, task_id: str, task, out: dict) -> dict:
        """After an approved act intent, the Tool port creates the informational
        artifacts (work order + record + notification). Never ``cmd/*`` (AD-12).
        Returns {"ok": True, ...} or {"failed": True, "reason": ...} — a port
        rejection FAILS the act stage so replan/PARTIAL can run (AD-3/AD-6)."""
        if self.port is None:
            return {"ok": True, "skipped": "no_port"}
        if out.get("type") == "work_order":
            r = self.port.create("work_order", dict(out), task_id=task_id, stage_name="act",
                                 agent="action", priority=getattr(task, "priority", "ROUTINE"))
            if not r.ok:
                return {"failed": True, "reason": f"work_order:{r.error}"}
        if getattr(task, "source_incident_id", None):
            r = self.port.create("incident_record", {
                "source_incident_id": task.source_incident_id, "note": out.get("summary", ""),
            }, task_id=task_id, stage_name="act", agent="action")
            if not r.ok:
                return {"failed": True, "reason": f"incident_record:{r.error}"}
        r = self.port.create("notification", {
            "recipient": "maintenance_manager",
            "message": out.get("summary", "maintenance notification"),
        }, task_id=task_id, stage_name="act", agent="action")
        if not r.ok:
            return {"failed": True, "reason": f"notification:{r.error}"}
        return {"ok": True}
```

Chain kết quả: port từ chối → act fail → `replan_or_partial("act", ...)` → back-edge
`act → plan` (cap 2) → hết cap → **PARTIAL**. Đây chính là kịch bản "tool từ chối,
hệ thống tự re-plan rồi báo partial" mà đề bài chấm.

---

## PHẦN 4 — P0-3: Observer thấy offline/error + stream dừng (`orchestration/agents/observer.py`)

### 4.1 `import time` đầu file.

### 4.2 Generic playbook: không khai báo devices → quan sát cả registry

Đầu `deterministic()`:

```python
        device_ids = list(ctx.device_ids())
        if not device_ids:
            # generic playbook declares no devices -> observe the whole closed
            # registry (AD-11) instead of seeing nothing
            device_ids = list(ctx.registry.devices)
```

### 4.3 `_observe_signal`: quality offline/error + arrival-age

Thay method hiện tại bằng:

```python
    def _observe_signal(self, ctx: AgentContext, signal_id: str,
                        ref_ts: int) -> Observation:
        rows = ctx.history.recent([signal_id], limit=3) if ctx.history is not None else []
        unit = ctx.registry.signal_unit(signal_id)
        dev = ctx.registry.device_for_signal(signal_id)  # registry-owned identity (AD-11)
        dev_id = dev.device_id if dev else signal_id.rsplit("_", 1)[0]
        if not rows:
            return Observation(device_id=dev_id, signal_id=signal_id, value=None,
                               event_ts="", age_seconds=None, staleness="offline",
                               quality="offline", unit=unit)
        ts, value, quality = rows[0][1], rows[0][2], rows[0][3]
        if quality == "missing_ts" or ts <= 0:
            return Observation(device_id=dev_id, signal_id=signal_id, value=None,
                               event_ts="", age_seconds=None, staleness="missing_ts",
                               quality="missing_ts", unit=unit)
        from datetime import datetime, timezone

        event_ts = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z")
        age = None if ref_ts <= 0 else max(0, (ref_ts - ts) / 1000.0)
        staleness = _staleness(age, ctx.runtime)
        if quality in ("offline", "error"):
            # ingest truth (AD-10): the source SAID the device is offline/error —
            # no age computation may downgrade that
            staleness = quality
        elif hasattr(ctx.history, "last_arrival"):
            # stopped-stream: event-time stays fresh forever when a device hangs;
            # arrival-vs-now is the admitted wall-clock exception (see buffer.py)
            arrivals = ctx.history.last_arrival([signal_id])
            if signal_id in arrivals:
                arrival_age = max(0.0, time.time() - arrivals[signal_id])
                staleness = _worst(staleness, _staleness(arrival_age, ctx.runtime))
        return Observation(
            device_id=dev_id, signal_id=signal_id,
            value=value, event_ts=event_ts, age_seconds=age, staleness=staleness,
            quality=quality, unit=unit,
        )
```

### 4.4 Helper `_worst` + bảng thứ tự

Thêm module-level (cạnh `_staleness`):

```python
_STALE_ORDER = {"fresh": 0, "stale": 1, "critical_stale": 2, "missing_age": 2,
                "error": 3, "offline": 3, "missing_ts": 3}


def _worst(a: str, b: str) -> str:
    return a if _STALE_ORDER.get(a, 0) >= _STALE_ORDER.get(b, 0) else b
```

### 4.5 `_finding` tính cả `error`

```python
def _finding(obs: list[Observation]) -> str:
    stale = [o for o in obs if o.staleness in
             ("stale", "critical_stale", "offline", "error", "missing_ts")]
    if not obs:
        return "no device telemetry available for inspection"
    if not stale:
        return f"observed {len(obs)} signal(s); all fresh"
    names = ", ".join(o.signal_id for o in stale[:5])
    return (f"{len(stale)} stale/offline/error signal(s): {names} — "
            f"plan must assume degraded telemetry")
```

### 4.6 Ghi memlog (ngoại lệ wall-clock)

```bash
uv run _bmad/scripts/memlog.py append \
  --workspace _bmad-output/planning-artifacts/architecture/architecture-seal-iot-2026-08-16 \
  --type decision \
  --text "AD-10 exception: observer may compare arrival-vs-now (wall clock) to detect a STOPPED stream; same family as the AD-8 approval TTL. Event-time remains the only freshness clock otherwise."
```

---

## PHẦN 5 — P0-1: Advance driver (`orchestration/supervisor.py`)

Hiện tượng: `ingest_request`/`spawn_from_incident` chỉ mint task rồi bỏ đó; chỉ
`handle_approval` gọi `advance()` đúng 1 lần (đủ vì advance có while-loop, nhưng act-fail
replan thì dừng giữa chừng). Sửa:

### 5.1 `import threading` + lock trong `__init__`

```python
        self._subscribed = False
        self._drive_lock = threading.RLock()   # 2 driver không đan xen trên 1 task
```

### 5.2 `_drive` + `_spawn_drive`

```python
    # -- advance driver (AD-1) -------------------------------------------
    def _drive(self, task_id: str) -> dict:
        """Run advance() repeatedly while the task keeps making progress: a replan
        returns mid-playbook (PLANNING) and must be driven again; approval waits and
        terminal states exit. Guard: max 24 rounds + no-progress detection, so a
        pathological task can never spin forever."""
        with self._drive_lock:
            out: dict = {}
            for _ in range(24):
                t0 = self.store.get(task_id)
                snap = (t0.state, t0.stage_cursor) if t0 else None
                out = self.advance(task_id)
                t1 = self.store.get(task_id)
                if out.get("state") in ("waiting", "REPORTED", "FAILED", "PARTIAL",
                                        "CANCELLED", "unknown"):
                    return out
                snap1 = (t1.state, t1.stage_cursor) if t1 else None
                if snap1 == snap:
                    return out  # không tiến triển -> thoát chống treo
            return out

    def _spawn_drive(self, task_id: str) -> None:
        threading.Thread(target=self._drive, args=(task_id,), daemon=True,
                         name=f"drive-{task_id}").start()
```

(Cẩn thận precedence khi so snapshot: tính `snap1` ra biến riêng rồi mới `==` —
viết `{expr} if cond else None == snap` một dòng sẽ so sai do `==` ăn phần `None`.)

### 5.3 `ingest_request` — thêm `drive`, sửa priority (luôn ở mục 10)

```python
    def ingest_request(self, text: str, playbook_id: str = "generic",
                       source_incident_id: str | None = None,
                       priority: str | None = None, origin: str = "human",
                       drive: bool = False) -> dict:
        ...
        task = self.store.mint(playbook_id, text, origin=origin,
                               source_incident_id=source_incident_id,
                               priority=priority or pb.priority or "ROUTINE", created=created)
        self._task_event(task.task_id, "opened",
                         {"task": task.to_dict(), "playbook": playbook_id})
        if drive:
            self._spawn_drive(task.task_id)
        return {"task_id": task.task_id, "state": task.state}
```

- `priority` đổi default `"ROUTINE"` → `None` để phân biệt "không nói gì" (lúc đó lấy
  priority của playbook — conflict_assessment được URGENT). Request UI nói URGENT thì
  URGENT thắng.
- **`drive=False` mặc định** để 23 test cũ (gọi `advance()` tay) không đổi hành vi.

### 5.4 `_on_request` bật drive

```python
        self.ingest_request(text, pb, priority=priority, drive=True)
```

### 5.5 `spawn_from_incident` — drive mặc định (cửa auto-loop AD-1)

Thêm param `drive: bool = True`; cuối **cả hai** nhánh (re-spawn lẫn mint mới) thêm:

```python
        if drive:
            self._spawn_drive(task_id)
```

(_drive tự no-op khi task đã terminal/đang chờ approval — gọi thêm không hại.)

### 5.6 `handle_approval` drive tiếp sau approve

```python
            self.store.transition(task_id, "approve")
            self._task_event(task_id, "approval_granted", {"approval_id": approval_id})
            self._drive(task_id)   # thay cho self.advance(task_id)
            return {"accepted": True, "decision": decision}
```

Sau approve, nếu act bị port từ chối → replan → `_drive` gọi advance tiếp tới khi
hết cap → PARTIAL, hoặc chờ approval tiếp. Trước đây advance đơn lẻ để task kẹt giữa.

---

## PHẦN 6 — P2 quick fixes

### 6.1 Dashboard `_ingest_task` (`ui/dashboard.py` ~dòng 148)

Hiện tại:

```python
        st = inner.get("payload") if isinstance(inner.get("payload"), str) else None
        rec["state"] = inner.get("agent") or rec["state"]
```

`st` là dead var; dòng dưới gán **tên agent** vào `state` (card FSM sai hoàn toàn).
Thay bằng:

```python
        p = inner.get("payload") if isinstance(inner.get("payload"), dict) else {}
        task_info = p.get("task") if isinstance(p.get("task"), dict) else {}
        ev = inner.get("event") or event
        # state FSM thật: task dict có trong opened/handoff; closed mang {"state": ...}
        if task_info.get("state"):
            rec["state"] = task_info["state"]
        elif ev == "closed" and p.get("state"):
            rec["state"] = p["state"]
        if inner.get("ts") and ev in ("opened", "closed"):
            rec["ts"] = inner["ts"]
```

### 6.2 Action agent: device fallback (`orchestration/agents/action.py` dòng 15)

```python
        device = (ctx.device_ids()
                  or [o.device_id for o in ctx.observations[:1]]
                  or [None])[0]
```

Generic playbook (stage không khai báo device) → lấy từ observation đầu tiên
(observer giờ quan sát cả registry — Phần 4.2). Không có observation nào → vẫn None →
port từ chối → act fail → replan (đúng hành vi, không crash).

### 6.3 `port.list_by_key` (`tools/port.py` dòng 230)

Hiện loop 5 kind làm cùng 1 lookup → trả 5 bản sao. Sửa:

```python
    def list_by_key(self, key: str) -> list[dict]:
        """AD-6 list-before-retry: find the recorded artifact(s) for a key."""
        row = self._conn.execute(
            "SELECT kind, backend_id FROM port_keys WHERE key=?", (key,)).fetchone()
        if not row:
            return []
        kind, backend_id = row[0], row[1]
        return [{"kind": kind, "backend_id": backend_id,
                 "artifact": self._read_back(kind, backend_id)}]
```

### 6.4 Xóa dead code

- `orchestration/supervisor.py` dòng 19–22: block `STAGE_FOR_AGENT = {...}` (không ai
  dùng — `agent_for_stage` trong playbooks.py mới là bản tốt). Xác nhận trước khi xóa:
  `grep -rn STAGE_FOR_AGENT` chỉ thấy chỗ định nghĩa.
- `orchestration/playbooks.py` dòng 163–167: `build_generic_playbook()` (không caller).
  `grep -rn build_generic_playbook` xác nhận.

### 6.5 Đã gộp vào 5.3 (priority + drive cùng một method).

### 6.6 Scrub secret `docs/MQTT-GUIDE.md` ⚠️ làm ngay

File đã **push lên GitHub** chứa password `mq_...` (3 chỗ: ~dòng 17, 27, 46) và key
`tk_...` (~dòng 29). Sửa từng chỗ thành:

```
MQTT_PASS=(xem mqtt.env — không đưa secret vào docs/git)
```

- **Scrub chỉ sửa HEAD** — git history vẫn còn. Password test ngắn hạn rủi ro thấp,
  nhưng nên nhắn BTC xin **rotate** (câu hỏi mở mục 9). Key `tk_` hỏi xem dùng lại
  được không.
- Kiểm tra không còn sót: `git grep -nE "mq_[A-Za-z0-9]+|tk_[A-Za-z0-9-]+" -- docs README.md`
  (chỉ được phép thấy trong `mqtt.env` — file này gitignored).

---

## PHẦN 7 — Test mới (viết trước khi code cũng được — TDD)

Vị trí: bridge → `tests/test_trackc_ingress.py`; supervisor/observer →
`tests/test_trackc_orchestration.py`; dashboard → `tests/test_trackc_dashboard.py`.
Dùng sẵn `build_fabric()` (trả 7 tuple: `_, bus, store, cmms, port, _, sup`).

1. **`test_team_prefix_normalizes_full_topic`** — viết `mqtt.env` tạm (tmp_path) với
   `MQTT_TOPIC=hackathon/underrated/test/telemetry` → `settings_from_env(path)`
   → `cfg["topic_prefix"] == "hackathon/underrated/"`. Thử cả `.../judge/telemetry`
   và prefix thuần.

2. **`test_bridge_on_message_ingests_to_bus`** — `TrackCBridge(registry)`; gán
   `b._bus = FakeBus()` (record `publish_telemetry`); dựng `types.SimpleNamespace(
   payload=json.dumps(frame).encode())` với frame 1 thiết bị MOTOR_01; gọi
   `b._on_message(None, None, msg)` → fake bus nhận `motor_01_current` quality "ok".
   Thêm 1 case payload rác `b"not-json"` → không raise.

3. **`test_act_port_failure_replans_then_partial`** — `monkeypatch.setattr(
   cmms, "create_work_order", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("cmms down")))`
   ; `ingest_request("prepare inspection", "prepare_inspection")`; approve adjudicate;
   approve act → **PARTIAL**; bus có event `replan` (failed_stage "act") và
   `closed` state PARTIAL; `cmms.lookup("work_orders") == []`.

4. **`test_generic_request_end_to_end_reported`** — publish `request/in` lên bus
   (text không match playbook nào → generic); poll ≤5s tới `AWAITING_APPROVAL`;
   approve (act approval); poll tới `REPORTED`; `cmms.lookup("work_orders")` ≥ 1.
   *Cần Phần 4.2 + 6.2 xong trước thì mới xanh.*

5. **`test_observer_offline_quality_marks_staleness`** — `hist.write("motor_01_current",
   now_ms, 23.0, quality="offline")` (ts mới, không stale theo event-time); chạy
   observe (advance 1 lần rồi đọc event `stage_done` observe, hoặc gọi
   `ObserverAgent().deterministic(ctx)` trực tiếp) → observation có
   `staleness == "offline"`, finding chứa "offline".

6. **`test_observer_stopped_stream_by_arrival`** — `hist.write(..., arrival=
   time.time()-999)` (event ts vẫn mới) → staleness `"critical_stale"` (arrival-age
   999s ≥ critical_seconds), finding nhắc degraded.

7. **`test_spawn_from_incident_autodrives`** — `sup.spawn_from_incident("inc-1",
   "high", "prepare_inspection")` (drive mặc định True) → poll ≤5s:
   `store.get(tid).state == "AWAITING_APPROVAL"`.

8. **REWRITE `test_request_driven_path_end_to_end_runs_unchanged`**
   (`tests/test_trackc_dashboard.py`) — assertion `state == "RECEIVED"` ngay sau
   publish giờ **sai** (task được drive nền). Đổi thành poll deadline 5s chờ
   `AWAITING_APPROVAL`, sau đó tiếp tục assert như cũ:

   ```python
   deadline = time.time() + 5
   while time.time() < deadline:
       tasks = dash.snapshot()["tasks"]  # hoặc GET /?json=1
       if tasks and tasks[0]["state"] == "AWAITING_APPROVAL":
           break
       time.sleep(0.05)
   else:
       pytest.fail("task không tới AWAITING_APPROVAL sau 5s")
   ```

Poll helper dùng chung (deadline + sleep 0.05) nên rút thành 1 hàm nhỏ cuối file test.

---

## PHẦN 8 — Chạy & acceptance

```bash
# toàn bộ (Windows console cp1252 → bắt buộc PYTHONUTF8=1)
PYTHONUTF8=1 make test          # hoặc: PYTHONUTF8=1 uv run pytest -q

# smoke đường live (cần mqtt.env): connect + 10s đếm tele/*
PYTHONUTF8=1 uv run python - <<'PY'
import json, time
from config import load_mapping
from adapters import TrackCBridge
from bus.client import InMemoryBus
bus = InMemoryBus()
seen = []
bus.subscribe("tele/", lambda t, p: seen.append(t))
b = TrackCBridge(load_mapping().trackc)
assert b.connect(), "connect fail"
b.start(bus)
time.sleep(10)
print("tele frames:", len(seen), "| signals:", len(set(seen)))
b.close()
PY
```

Kỳ vọng: **114 test pass** (106 cũ + 8 mới, không test cũ break ngoài test rewrite);
smoke live thu ≥ 10 signal `tele/*` trong 10s (~2Hz × 6 thiết bị).

## PHẦN 9 — Commit & câu hỏi mở

Commit gợi ý (1 commit/lớp cũng được, miễn chạy xanh trước khi commit):

```
Fix P1 bridge live-path + P0 act-fail/observer-staleness/advance-driver + P2 (dashboard state, generic devices, port list_by_key, scrub secrets); 114 tests pass

Co-Authored-By: Claude <noreply@anthropic.com>
```

- **Tuyệt đối không** commit: `mqtt.env`, `tai lieu/`, `knowledge/runbooks/` (đang
  untracked — giữ nguyên).
- Câu hỏi còn mở với BTC: (1) topic `judge` bao giờ bật; (2) các giá trị `scenario`
  ngoài NORMAL; (3) thiết bị offline thì rời mảng `devices` hay nằm lại với
  status đổi; (4) **xin rotate password + tk_ key** (đã lộ trong git history).
