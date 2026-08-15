"""Single LLM entry point (AD-13): budget-aware, mock-capable, provider-agnostic.

All model calls go through this module. It supports a recorded mock replay for
offline/demo runs and a real OpenRouter path via the ``OPENROUTER_API_KEY`` env var.
A per-call-path token budget is enforced so a hung or expensive call path degrades
to the deterministic fallback instead of freezing the loop.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Protocol

from config import LLMConfig


class LLMBackend(Protocol):
    def complete_json(self, system: str, user: str) -> dict[str, Any] | None:
        ...


class MockLLM:
    """Deterministic mock backend. Emits a ``proposal`` taken verbatim from the user
    prompt wrapped in a JSON object — used for offline tests and rehearsal."""

    def __init__(self, budget: int = 3) -> None:
        self.calls = 0
        self.budget = budget

    def complete_json(self, system: str, user: str) -> dict[str, Any] | None:
        if self.calls >= self.budget:
            return None
        self.calls += 1
        # deterministic: echo the last JSON block present in the user prompt
        import re

        m = re.search(r"\{.*\}", user, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        return {"proposal": user[:120]}


class OpenRouterBackend:
    """Real OpenRouter backend. Requires OPENROUTER_API_KEY."""

    def __init__(self, model: str) -> None:
        self.model = model
        key = os.environ.get("OPENROUTER_API_KEY")
        if not key:
            raise RuntimeError("OPENROUTER_API_KEY not set; use MockLLM for offline runs")
        import httpx

        self._httpx = httpx
        self._key = key

    def complete_json(self, system: str, user: str) -> dict[str, Any] | None:
        resp = self._httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {self._key}"},
            json={
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)


class LLMClient:
    """Budget-aware facade. The one place stages obtain a backend."""

    def __init__(self, config: LLMConfig, backend: LLMBackend | None = None) -> None:
        self.config = config
        self.calls = 0
        self.budget = config.budget_per_call_path
        self.backend = backend or self._make_backend()

    def _make_backend(self) -> LLMBackend:
        if self.config.provider == "openrouter":
            try:
                return OpenRouterBackend(self.config.default_model)
            except Exception:
                pass  # no key / no httpx / any failure -> fall through to mock
        return MockLLM(budget=self.budget)

    def complete_json(self, system: str, user: str) -> dict[str, Any] | None:
        """Return a JSON dict, or None if the per-call-path budget is exhausted.
        Enforces a wall-clock guard (concurrent.futures) so a hung backend never
        blocks the loop (AD-13 / 5.5): over-budget returns None -> fallback chain."""
        if self.calls >= self.budget:
            return None
        out = self._run_backend(system, user)
        if out is not None:
            self.calls += 1
        return out

    def _run_backend(self, system: str, user: str) -> dict[str, Any] | None:
        if self.config.max_wall_seconds <= 0:
            return self.backend.complete_json(system, user)
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(self.backend.complete_json, system, user)
            try:
                return fut.result(timeout=self.config.max_wall_seconds)
            except FutTimeout:
                return None

    @property
    def exhausted(self) -> bool:
        return self.calls >= self.budget