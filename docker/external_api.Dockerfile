FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev --extra external_api --extra reports

COPY src /app/src

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --extra external_api --extra reports

FROM python:3.11-slim-bookworm

RUN groupadd --gid 1001 app && \
    useradd --uid 1001 --gid app --shell /bin/bash --create-home app

COPY --from=builder --chown=app:app /app /app

RUN mkdir -p /home/app/.postgresql
COPY --chown=app:app ./certs/bib-prod-rdb-data-ev.pem /home/app/.postgresql/root.crt

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app
USER 1001

# Run FastAPI with Uvicorn
# The --proxy-headers --forwarded-allow-ips=* are used so that FastAPI sees requests as HTTPS and not HTTP
CMD ["uvicorn", "src.external_api.app:app", "--proxy-headers", "--forwarded-allow-ips=*", "--host", "0.0.0.0", "--port", "4000"]
