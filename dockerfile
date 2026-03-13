FROM ghcr.io/astral-sh/uv:python3.12-alpine AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-workspace \
    --package interloper-core \
    --package interloper-assets \
    --package interloper-docker

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable \
    --package interloper-core \
    --package interloper-assets \
    --package interloper-docker

FROM python:3.12-alpine

COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["interloper"]
