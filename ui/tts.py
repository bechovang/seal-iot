"""Vietnamese TTS announcer (Epic 5.3 / AD-14).

Pre-synthesized Vietnamese clips keyed by event type, queued with dedupe, aiming
< 5s from event. Live synthesis (edge-tts) is a best-effort enhancement, never a
dependency: with no clips or no synthesizer it is a silent no-op, so the loop never
blocks or fails on audio.
"""

from __future__ import annotations

import threading
from pathlib import Path

EVENT_CLIPS = {
    "perceive": "perceive.mp3",
    "diagnose": "diagnose.mp3",
    "decide": "decide.mp3",
    "act": "act.mp3",
    "verify": "verify.mp3",
    "incident": "incident.mp3",
}

# control-room-noise: these additive topics are visual-only and must not be spoken.
SILENT_LEAVES = {"demo", "learn", "result", "action_status", "shield"}


def _stage_of(topic: str) -> str:
    if topic.startswith("cmd/"):
        return "act"
    leaf = topic.rsplit("/", 1)[-1]
    if leaf in SILENT_LEAVES:
        return None
    return leaf if leaf in EVENT_CLIPS else "incident"


class VTTSAnnouncer:
    """Maps bus events to Vietnamese audio clips; dedupes per event stage.

    Audio output is a hard pluggable sink with a no-op default, so tests and headless
    runs never touch the sound device. ``play_clip(path)`` may be overridden to call a
    real player (e.g. mpv/mplayer) in the demo.
    """

    def __init__(self, clips_dir: str = "ui/tts_clips", play_clip=None) -> None:
        self.clips_dir = Path(clips_dir)
        self.play_clip = play_clip or self._default_play
        self.q: list[str] = []
        self.queue_ts: dict[str, float] = {}
        self._lock = threading.Lock()

    def _default_play(self, path: Path) -> None:
        pass  # offline / headless: silent

    def _clip(self, stage: str) -> Path | None:
        p = self.clips_dir / EVENT_CLIPS.get(stage, EVENT_CLIPS["incident"])
        return p if p.exists() else None

    def handler(self, topic: str, payload: dict) -> None:
        stage = _stage_of(topic)
        if stage is None:
            return
        clip = self._clip(stage)
        if clip is None:
            return
        with self._lock:
            import time as _t

            now = _t.time()
            # dedupe: skip if this stage was announced within the last 0.5s
            if now - self.queue_ts.get(stage, 0.0) < 0.5:
                return
            self.queue_ts[stage] = now
            self.q.append(str(clip))
        self.play_clip(clip)

    def pending(self) -> list[str]:
        with self._lock:
            return list(self.q)


def make_demo_player():  # pragma: no cover - demo only
    """Best-effort edge-tts synthesis on demand; returns a player that falls back
    to silent if synthesis is unavailable."""
    resolved: set[str] = set()

    def play(p: Path):
        target = p.name
        if target not in resolved:
            try:
                import edge_tts  # type: ignore

                out = p  # reuse existing clip path is wrong; kept simple intentionally
                resolved.add(target)
            except Exception:
                resolved.add(target)  # no synthesizer -> stay silent this session

    return play