"""Dataset adapters — the only place that knows raw sensor tags (AD-8)."""

from .hai import HAIAdapter, run, synthetic_rows

__all__ = ["HAIAdapter", "run", "synthetic_rows"]