# Repository Guidelines

## Project Structure & Module Organization
EValue is a monorepo that hosts every EV data workflow. Shared utilities, Spark helpers, and caching live under `src/core/`, while OEM-specific ingestion services sit in `src/ingestion/` and nightly PySpark transformations live in `src/transform/`. `src/db_models/` contains SQLAlchemy models plus the Alembic config, `src/external_api/` exposes the FastAPI interface, and Dagster assets live in `src/bib_dagster/`. Exploratory notebooks stay inside `src/EDA/`, and regression tests mirror the module tree inside `tests/`. Supporting infrastructure resides in `docker/`, `k8s/`, `scripts/`, and `dagster_home/`.

## Build, Test & Development Commands
Install dependencies with `uv sync --locked --all-extras`; scope installs by module via `uv sync --locked --extra transform` or `--extra ingestion`. Run unit and integration suites with `uv run pytest tests/` or target files such as `uv run pytest tests/transform/test_energy_segments.py -k vin_edge_case`. Generate coverage with `uv run pytest --cov=src`. Format/lint using `uv run ruff format` and `uv run ruff check --fix`, then `pre-commit run --all-files` before pushing. When explicitly asked to check and fix typing, use `uv run ty check ...`. Manage schema changes through `uv run alembic -c src/db_models/alembic.ini upgrade head` and `... revision --autogenerate -m "message"`. Frontend work happens in `frontend/` with `pnpm install` and `pnpm dev`. Run the external API via `uv run uvicorn src.external_api.app:app --reload --port 4000`, Dagster with `DAGSTER_HOME=./dagster_home dagster dev`, and brand-specific ingestion via the provided `start_*.sh` scripts.

## Coding Style & Naming Conventions
Python follows Ruff defaults: 4-space indentation, snake_case modules, and type-hinted public APIs. Keep docstrings on Dagster assets, CLI entrypoints, and FastAPI routes to document IO contracts. Strip notebook outputs with `uv run nbstripout notebook.ipynb` (pre-commit automates this). JavaScript/TypeScript code adheres to ESLint + Prettier, with kebab-case filenames for components and PascalCase for React exports.

## Testing Guidelines
Tests live beside their functional peers (`tests/transform`, `tests/ingestion`, etc.). Mirror the source path when naming files and include parametrized VIN, timezone, and OEM fixtures. Mark slow, network, or Spark-dependent tests so CI can gate them. Schema or contract changes must include Alembic autogenerate verification plus DB-backed tests. Favor high-level service checks (e.g., FastAPI responses, Dagster asset materializations) when touching shared data models.

## Architecture & Runtime Overview
The ingestion layer captures OEM data (Tesla fleet telemetry, High Mobility, Mobilisights) into S3 buffers, compresses daily to `compressed/YYYY/MM/DD/{VIN}.parquet`, and hands the results to `src/transform/` Spark jobs. These jobs cache intermediate DataFrames with `core.caching_utils.cache_result`, enrich with fleet metadata, and emit aggregated SoH metrics to databases served by `src/external_api/`. Dagster orchestrates Spark applications on Kubernetes (`k8s/spark_application/`), while Dockerfiles in `docker/` produce deployable images. Understanding this flow is key when modifying schema, S3 layouts, or Kafka ingestion topics.

## Commit & Pull Request Guidelines
Recent history mixes ticket-prefixed (`EVALUE-110`) and conventional (`Fix:`, `Build(deps):`) summaries. Follow that pattern: start with the ticket or scope, keep messages imperative, and split unrelated work. Every PR should describe the problem, outline the solution, list commands/tests executed, and link Jira/GitHub issues. Attach logs, API responses, or UI screenshots when changing contracts or user flows, and request reviewers from every affected domain (data, backend, frontend).

## Security & Configuration Tips
Copy `.env.dist` to `.env` and inject provider credentials, AWS keys, Redis, and Postgres settings locally—never commit secrets. The `start_*.sh` scripts expect those variables to be exported before launch. Keep Docker Compose services and the Kubernetes manifests in sync when altering ports or topics, and prefer referencing secrets from your orchestrator rather than embedding them in code or configs.
