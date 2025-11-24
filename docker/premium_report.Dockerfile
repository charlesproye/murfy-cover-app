FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev --extra scraping_playwright

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra scraping_playwright

FROM python:3.11-slim-bookworm

RUN groupadd --gid 1001 app && \
    useradd --uid 1001 --gid app --shell /bin/bash --create-home app

WORKDIR /app
USER 1001

COPY --from=builder --chown=app /app /app

RUN mkdir -p /home/app/.postgresql
COPY --chown=app:app ./certs/bib-prod-rdb-data-ev.pem /home/app/.postgresql/root.crt

ENV PATH="/app/.venv/bin:$PATH"
ENV PLAYWRIGHT_BROWSERS_PATH=0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
    libgtk-3-0 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    libxss1 libxshmfence1 libasound2 fonts-liberation ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

USER 1001
RUN /app/.venv/bin/python -m playwright install chromium && \
    rm -rf /home/app/.cache/ms-playwright

WORKDIR /app
