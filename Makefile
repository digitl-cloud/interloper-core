.PHONY: build

check:
	uv run ruff check
	uv run ty check
	uv run pytest

docker-build:
	docker build -f dockerfile -t interloper .

