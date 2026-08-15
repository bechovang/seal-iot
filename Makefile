.PHONY: install lint test e2e

install:
	uv sync

lint:
	uv run python -m compileall -q config.py adapters act bus decide diagnose history incident knowledge learn llm perceive plant_model score verify
	@echo "lint ok"

test:
	uv run pytest -q

e2e:
	@echo "deferred: add JSONL recorder + full-loop replay (Epic 5)"