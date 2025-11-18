# Test Suite

Test suite for EValue, covering core utilities, ingestion, transformation, external API, and database models.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── core/                    # Core utility tests
├── ingestion/              # Data ingestion tests (marked @pytest.mark.integration)
├── transform/              # ETL pipeline tests (marked @pytest.mark.integration)
├── test_external_api/      # External API endpoint tests
└── test_db_models/         # Database model tests
```

**Note**: Test directories use `test_` prefix to avoid import conflicts with source packages.

## Quick Start

### 1. Install Dependencies
```bash
uv sync --locked --all-extras
```

### 2. Start PostgreSQL
```bash
docker-compose up -d postgres
```

Database runs on `localhost:55432` (user/password/db: `evalue`).

### 3. Configure Environment
Copy `.env.dist` to `.env` and set required variables:
```bash
# Database (Docker PostgreSQL)
DB_DATA_EV_HOST=localhost
DB_DATA_EV_PORT=55432
DB_DATA_EV_NAME=evalue
DB_DATA_EV_USER=evalue
DB_DATA_EV_PASSWORD=evalue

# Auth (required for external API tests)
SECRET_KEY=your-secret-key-min-32-chars
ALGORITHM=HS256
ENCRYPT_KEY=your-fernet-key-44-chars-base64
COOKIE_SECURE=false
COOKIE_DOMAIN=localhost
```

### 4. Run Migrations
```bash
uv run alembic -c src/db_models/alembic.ini upgrade head
```

## Running Tests

### Run Unit Tests (default)
```bash
uv run pytest
```
Integration tests are skipped by default.

### Run All Tests (including integration)
```bash
uv run pytest -m ""
```

### Run Only Integration Tests
```bash
uv run pytest -m integration
```

### Run Specific File/Test
```bash
uv run pytest tests/test_external_api/test_auth.py
uv run pytest tests/core/test_stats_utils.py::test_function_name
```

### Run with Coverage
```bash
uv run pytest --cov=src --cov-report=html
```

## Test Markers

### Unit Tests (CI-compatible)
- Pure logic tests (no external dependencies)
- Database tests (using PostgreSQL in docker-compose)

### Integration Tests (`@pytest.mark.integration`)
Require external resources not available in CI:
- External APIs (Mobilisights, High Mobility, etc.)
- S3/Minio storage
- Examples: `tests/ingestion/*`, `tests/transform/*`, `tests/core/s3/*`

## Fixtures (conftest.py)

Key fixtures available in all tests:
- `db_session`: Async database session with automatic rollback
- `test_client`: FastAPI test client with ASGITransport
- `test_company`, `test_user`, `test_role`, `test_fleet`: Test entities
- `test_vehicle`: Complete vehicle with relationships
- `test_oem`, `test_make`, `test_battery`: Supporting data

## Writing Tests

Use fixtures from `conftest.py`. Tests automatically rollback database changes.

```python
def test_api_endpoint(test_client, test_user):
    response = test_client.get("/v1/endpoint")
    assert response.status_code == 200

@pytest.mark.integration
async def test_external_api(db_session):
    # Test requiring external API
    pass
```

## Troubleshooting

**Connection refused**: Ensure PostgreSQL is running (`docker-compose up -d postgres`)

**Schema errors** (`relation "..." does not exist`): Run migrations
```bash
uv run alembic -c src/db_models/alembic.ini upgrade head
```

**Import errors**: Ensure dependencies are installed (`uv sync --locked --all-extras`)

## CI/CD

Tests run in CI with PostgreSQL sidecar. Integration tests are automatically skipped.

CI requirements:
1. PostgreSQL service/sidecar
2. Environment variables (DB credentials, auth secrets)
3. Dependencies via `uv sync --locked --all-extras`
4. Migrations via `uv run alembic -c src/db_models/alembic.ini upgrade head`

See `.github/workflows/test.yml` for complete workflow configuration.
