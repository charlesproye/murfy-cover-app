FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev --extra scraping --extra activation

COPY src/ /app/src/
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra scraping --extra activation

FROM python:3.11-slim-bookworm

# Install Chromium and ChromeDriver from Debian repositories
# This supports both amd64 and arm64 architectures
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 \
    libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 \
    libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 xdg-utils \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Create symlinks for common Chrome/ChromeDriver names
RUN ln -sf /usr/bin/chromium /usr/bin/google-chrome && \
    ln -sf /usr/bin/chromedriver /usr/local/bin/chromedriver

# Check versions
RUN chromium --version && chromedriver --version

RUN groupadd --gid 1001 app && \
    useradd --uid 1001 --gid app --shell /bin/bash --create-home app

COPY --from=builder --chown=app:app /app /app

RUN mkdir -p /home/app/.postgresql
COPY --chown=app:app ./certs/bib-prod-rdb-data-ev.pem /home/app/.postgresql/root.crt

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

USER 1001

CMD ["python", "src/ingestion/soh_scraping/main.py", "aramis", "autospherre", "ev_market", "spoticar"]
