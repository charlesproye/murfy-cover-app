FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# TODO - Find a way to remove the aws-java-sdk-bundle dependency which is too heavy (590MB)
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar & \
    curl -L -o /opt/spark/jars/bundle-2.28.29.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.29/bundle-2.28.29.jar & \
    curl -L -o /opt/spark/jars/spark-hadoop-cloud_2.13-4.0.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.13/4.0.1/spark-hadoop-cloud_2.13-4.0.1.jar & \
    wait

WORKDIR /app

# Install dependencies first (for better caching)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev --extra transform

# Copy ONLY the essential source files - exclude EDA and other bloat
COPY src/core/ ./src/core/
COPY src/transform/ ./src/transform/
COPY src/__init__.py ./src/__init__.py

# Install the project with minimal source
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra transform

# Use official Spark base image that includes spark-submit and proper entrypoints
FROM apache/spark:4.0.1-java21-python3

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/usr \
    UV_PYTHON=/usr/bin/python3

# Install UV in the Spark image (as root to avoid permission issues)
USER root
RUN pip install uv

COPY --from=builder /opt/spark/jars/hadoop-aws-3.4.1.jar ${SPARK_HOME}/jars/
COPY --from=builder /opt/spark/jars/bundle-2.28.29.jar ${SPARK_HOME}/jars/
COPY --from=builder /opt/spark/jars/spark-hadoop-cloud_2.13-4.0.1.jar ${SPARK_HOME}/jars/

# Copy project files needed for UV installation
COPY pyproject.toml uv.lock /app/
COPY src/core /app/src/core
COPY src/transform /app/src/transform
COPY src/__init__.py /app/src/__init__.py

# Allow spark user to read and write to the app directory
RUN chown -R spark:spark /app \
    && chmod -R u+rwX /app

# Install dependencies directly into the system Python using UV (as root)
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra transform
USER spark

