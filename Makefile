.PHONY: install lint test e2e demo ui check

install:
	uv sync

lint:
	uv run python -m compileall -q config.py harness_loop.py adapters act bus decide diagnose history incident knowledge learn llm perceive plant_model score verify bus ui
	@echo "lint ok"

test:
	uv run pytest -q

# Epic 5.1: record a synthetic incident end-to-end to JSONL, then replay it and
# pass a smoke assertion. The recorded log doubles as the failure-recovery fixture.
e2e:
	PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl
	PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e.jsonl --replay

# Same loop under a red-team / degraded variant, recorded to a separate log.
demo:
	PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e-redteam.jsonl --variant red-team
	PYTHONUTF8=1 uv run python harness_loop.py --log demo/e2e-redteam.jsonl --replay

# Epic 5.2 / AD-14: control-room SPA. Replays the recorded demo into a fresh loop
# that serves ui/app.html at http://127.0.0.1:8765 (watch mode, additive emits).
ui:
	PYTHONUTF8=1 uv run python harness_loop.py --log demo/controlroom.jsonl --watch

check: lint test e2e