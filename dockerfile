FROM ghcr.io/astral-sh/uv:python3.10-alpine AS builder

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-workspace \
    --package interloper \
    --package interloper-assets \
    --package interloper-docker \
    --extra cli

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable \
    --package interloper \
    --package interloper-assets \
    --package interloper-docker \
    --extra cli

FROM python:3.10-alpine

COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["interloper"]

