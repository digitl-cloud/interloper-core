.PHONY: build

check:
	uv run ruff check
	uv run pyright
	uv run pytest

docker-build:
	docker build -f dockerfile -t interloper .

claude-commit:
	claude --model haiku -p "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push." --dangerously-skip-permissions