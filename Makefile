.PHONY: install lint test

install:
	uv sync

lint:
	uv run ruff check --select I --fix
	uv run ruff format anyio_utils/ tests/
	uv run ruff check anyio_utils/ tests/
	uv run pyright anyio_utils/ tests/

test:
	uv run pytest -v tests/
