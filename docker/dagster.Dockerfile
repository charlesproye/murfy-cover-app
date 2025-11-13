FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --locked --no-dev --extra orchestration --extra transform

COPY src/ /app/src/
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra orchestration --extra transform

FROM python:3.11-slim-bookworm

RUN groupadd --gid 1001 app && \
    useradd --uid 1001 --gid app --shell /bin/bash --create-home app

RUN mkdir -p /opt/dagster/dagster_home
RUN chown -R app:app /opt/dagster/dagster_home

COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app
USER app

# Run the Dagster gRPC server
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "bib_dagster.definitions"]
