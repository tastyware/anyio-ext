.PHONY: install lint test

install:
	uv sync

lint:
	uv run ruff check --select I --fix
	uv run ruff format anyio_ext/ tests/
	uv run ruff check anyio_ext/ tests/
	uv run pyright anyio_ext/ tests/

test:
	uv run pytest -v tests/
