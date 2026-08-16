"""Dataset adapters — the only place that knows raw sensor tags (AD-8)."""

from .hai import HAIAdapter, run, synthetic_rows
from .trackc import TrackCBridge, build_payload, parse_payload, settings_from_env
from .trackc_sim import TrackCSim

__all__ = ["HAIAdapter", "run", "synthetic_rows", "TrackCBridge", "build_payload",
           "parse_payload", "settings_from_env", "TrackCSim"]