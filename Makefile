.PHONY: build

setup:
	pre-commit install
	uv sync --all-packages --all-extras

check:
	uv run ruff check
	uv run pyright
	uv run pytest

claude-commit:
	claude --dangerously-skip-permissions --model haiku -p "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push."

codex-commit:
	codex exec --dangerous-skip-permissions --model gpt-5-codex-mini "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push."

docker-build:
	docker build . -t interloper -t europe-docker.pkg.dev/dc-int-connectors-prd/docker/interloper

docker-build-linux:
	docker build . -t interloper -t europe-docker.pkg.dev/dc-int-connectors-prd/docker/interloper --platform linux/amd64

docker-push:
	docker push europe-docker.pkg.dev/dc-int-connectors-prd/docker/interloper

docker-build-push:
	make docker-build-linux
	make docker-push