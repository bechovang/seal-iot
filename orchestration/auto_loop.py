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
            if dev.is_aggregate:
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