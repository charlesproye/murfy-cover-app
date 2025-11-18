# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EValue is a data analytics service for electric vehicle (EV) data processing. The pipeline handles data extraction from multiple OEM APIs (Tesla, BMW, High Mobility, Mobilisights), transformation using PySpark, and valorization of battery health metrics. The project is deployed on Kubernetes and uses Dagster for orchestration.

## Development Environment

### Package Management
This project uses **uv** for dependency management. Install it from https://github.com/astral-sh/uv

```bash
# Install all dependencies
uv sync --locked --all-extras

# Install specific module dependencies
uv sync --locked --extra transform
uv sync --locked --extra ingestion
uv sync --locked --extra orchestration
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/path/to/test_file.py

# Run tests with coverage
uv run pytest --cov=src
```

Tests are located in the `tests/` directory mirroring the `src/` structure. Use VS Code's test explorer or `.vscode/launch.json` debug configurations.

### Code Quality
```bash
# Format code
uv run ruff format

# Lint and fix issues
uv run ruff check --fix

# Run pre-commit hooks manually
pre-commit run --all-files
```

Pre-commit hooks are configured to run ruff, trailing whitespace checks, and nbstripout for notebooks.

### Database Migrations (Alembic)
```bash
# Check current migration status
uv run alembic -c src/db_models/alembic.ini current

# Run pending migrations
uv run alembic -c src/db_models/alembic.ini upgrade head

# Generate new migration
uv run alembic -c src/db_models/alembic.ini revision --autogenerate -m "description"
```

Database models are defined in `src/db_models/` using SQLAlchemy.

### Running Services Locally

```bash
# External API (port 4000)
uv run uvicorn src.external_api.app:app --reload --port 4000

# Dagster orchestration (dev mode)
dagster dev
# Set DAGSTER_HOME environment variable to ./dagster_home

# Individual ingestion services (see start_*.sh scripts)
./start_tesla_fleet_telemetry.sh
./start_hm.sh
./start_bmw.sh
```

Check `.vscode/launch.json` for debug configurations for FastAPI services and other modules.

### Notebook Development
Before committing notebooks, strip outputs to keep the repo clean:
```bash
# Strip outputs from all notebooks
find . -name '*.ipynb' -exec uv run nbstripout {} +
```

If pre-commit is enabled (`pre-commit install`), nbstripout runs automatically.

## Architecture

### Module Structure
```
src/
├── core/              # Shared utilities (S3, caching, logging, pandas helpers)
├── db_models/         # SQLAlchemy database models and migrations
├── ingestion/         # Data ingestion from OEM APIs (Tesla, BMW, HM, Mobilisights)
├── transform/         # ETL pipeline using PySpark (runs on Kubernetes)
├── activation/        # Vehicle activation and fleet management
├── external_api/      # FastAPI service for external data access
├── bib_dagster/       # Dagster orchestration definitions
├── results/           # Battery health analysis and reporting
├── visualization/     # Streamlit dashboards
└── eda/               # Exploratory data analysis notebooks
```

### Core Module (`src/core/`)

The core module provides utilities used across the entire codebase:

- **caching_utils.py**: `@cache_result` decorator and `CachedETL` class for S3/local caching of ETL results
- **s3_utils.py**: `S3_Bucket` wrapper around boto3 for simplified S3 operations
- **spark_utils.py**: PySpark session management and DataFrame utilities
- **sql_utils.py**: Database connection and pandas-to-SQL utilities
- **console_utils.py**: `@main_decorator` and `single_dataframe_script_main` for CLI scripts
- **pandas_utils.py**: Helper functions extending pandas API
- **logging_utils.py**: Structured logging configuration

Import the default S3 bucket instance: `from core.singleton_s3_bucket import bucket`

### Transform Pipeline (`src/transform/`)

The transform module is the heart of the ETL pipeline. It processes data through these stages:

**Data Flow:**
```
JSON API responses (S3) + Static data (DB)
    ↓
Raw Time Series (raw_tss/*.parquet on S3)
    ↓ + fleet_info DataFrame
Processed Time Series (processed_tss/*.parquet on S3)
    ↓
Raw Results (raw_results/*.parquet on S3) - Battery health metrics with noise
    ↓
Processed Results (vehicle_data table) - Cleaned, aggregated weekly metrics
```

**Key ETL Stages:**
1. **raw_tss**: Parse JSON responses into tabular DataFrames per brand (BMW, Tesla, HM, Mobilisights)
2. **fleet_info**: Join static vehicle data from multiple DB tables
3. **processed_tss**: Normalize column names/units, add segmentation (in_charge, in_discharge), join fleet_info
4. **raw_results**: Compute battery State of Health (SoH) and charging level metrics
5. **processed_results**: Aggregate to weekly frequency, remove outliers, enforce monotonic SoH decrease

Each ETL stage uses the `@cache_result` decorator pattern from `core.caching_utils`.

### Ingestion Architecture (`src/ingestion/`)

Handles real-time and batch data ingestion from multiple sources:

- **tesla_fleet_telemetry**: FastAPI server receiving streaming telemetry via Kafka
- **high_mobility**: Polls High Mobility API for BMW, Ford, Mercedes, Renault, Volvo data
- **mobilisights**: Ingests data from Mobilisights API
- **ev_database**: Scrapes EV database for model specifications

Data flows: OEM API → S3 temp buffer → Daily compression to Parquet → Transform pipeline

Storage structure:
```
s3://bucket/temp/{VIN}/data.json           # Daily buffer
s3://bucket/compressed/YYYY/MM/DD/{VIN}.parquet  # Compressed archives
```

### Orchestration (`src/bib_dagster/`)

Dagster orchestrates the entire pipeline:
- Asset definitions in `defs/`
- Spark job integrations via `pipes/` (Spark Operator on K8s)
- Schedules in `defs/schedules.py`
- Resources (S3, DB connections) in `defs/resources.py`

Run locally: `dagster dev` (set DAGSTER_HOME to ./dagster_home)

### External API (`src/external_api/`)

FastAPI service providing authenticated access to vehicle data:
- JWT-based authentication
- Usage-based billing (per request/VIN)
- Rate limiting via Redis
- Endpoints for static/dynamic vehicle data, warranties, battery health

Swagger docs: http://localhost:4000/api/v1/docs

## Deployment

### Kubernetes Resources
Located in `k8s/`:
- `deployment/`: Long-running services (ingestion APIs, visualization)
- `job/`: One-off jobs
- `spark_application/`: SparkApplication CRDs for transform jobs
- `kafka-connector/`: Kafka Connect configurations

### Docker Images
Dockerfiles in `docker/`:
- `dagster.Dockerfile`: Orchestration service
- `spark.Dockerfile`: PySpark jobs (transform pipeline)
- `tesla.Dockerfile`, `hm.Dockerfile`, etc.: Ingestion services
- `external_api.Dockerfile`: External API service

Build with `docker-compose` or deploy to Scaleway Container Registry (`rg.fr-par.scw.cloud/data-engineering/`).

## Important Patterns

### S3 Caching Pattern
Use `@cache_result` for expensive ETL operations:
```python
from core.caching_utils import cache_result
from core.singleton_s3_bucket import bucket

S3_KEY = "processed/{brand}/data.parquet"

@cache_result(S3_KEY, on="s3", path_params=["brand"])
def get_processed_data(brand: str, force_update: bool = False):
    # Computation here
    return dataframe
```

### CLI Script Pattern
Use `single_dataframe_script_main` for ETL scripts:
```python
from core.console_utils import single_dataframe_script_main, main_decorator

@main_decorator
def main():
    single_dataframe_script_main(get_processed_data, brand="tesla", force_update=True)

if __name__ == "__main__":
    main()
```

### PySpark on Kubernetes
Transform jobs run as SparkApplications on K8s. Use `core.spark_utils` for session management:
```python
from core.spark_utils import get_spark_session

spark = get_spark_session(app_name="my_job")
df = spark.read.parquet("s3a://bucket/path/")
```

### Database Access
SQLAlchemy models in `src/db_models/`. Use `core.sql_utils` for pandas integration:
```python
from core.sql_utils import con  # Default DB connection
import pandas as pd

df = pd.read_sql("SELECT * FROM vehicle", con)
```

## Data Storage Strategy

### Tesla Telemetry Buffering
1. Real-time data arrives via Kafka → stored in S3 temp/{VIN}/
2. Buffer size: 1000 messages/vehicle, flush interval: 30 seconds
3. Daily compression at midnight UTC → compressed/YYYY/MM/DD/{VIN}.parquet
4. Transform pipeline reads from compressed archives

### Parquet Format
All intermediate and final results use Parquet for:
- Efficient columnar storage
- Schema enforcement
- Better query performance
- Compression ratio

## Common Issues

### isort Import Organization
The project uses custom first-party packages in `pyproject.toml`:
```
known-first-party = ["core", "activation", "transform", "eda", "load", "web", "results", "external_api", "visualization"]
```

### Python Version
Requires Python >=3.11, <3.14. Use the `.venv` created by `uv sync`.

### Environment Variables
Copy `.env.dist` to `.env` and configure:
- S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET)
- Database credentials (POSTGRES_*)
- OEM API credentials (varies per provider)
- Redis (for external API)

### Spark on K8s
When debugging Spark jobs, check:
- SparkApplication logs: `kubectl logs -f <spark-driver-pod>`
- Executor logs in K8s dashboard
- S3 connectivity from pods (use s3a:// protocol)
