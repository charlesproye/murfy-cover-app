# EValue Test Suite

Test suite for EValue, covering core utilities, ingestion, transformation, external API, and database models.

## Philosophy: Explicit is Better Than Implicit

This test suite uses **polyfactory** for test data generation instead of implicit pytest fixtures:

- **Explicitness**: Factory calls are explicit - you see exactly what data is being created
- **Flexibility**: Easy to customize test data without modifying shared fixtures
- **Type Safety**: Full IDE autocomplete and type checking
- **Maintainability**: No hidden dependencies between tests
- **Clarity**: Test code reads like regular Python - no magic parameter injection

## Test Structure

```
tests/
├── conftest.py              # Infrastructure fixtures (db_session, app_client) and helpers
├── factories/               # Polyfactory test data factories
│   ├── __init__.py         # Public factory exports
│   ├── base.py             # Base factory classes with async persistence
│   ├── core.py             # Company, User, Fleet, Role factories
│   ├── vehicle.py          # Vehicle-related factories
│   ├── api.py              # API model factories
│   ├── report.py           # Report factories
│   └── tesla.py            # Tesla schema factories
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

## Writing Tests with Factories

### Basic Usage

```python
from tests.factories import CompanyFactory, UserFactory, RoleFactory

async def test_create_user(db_session):
    # Create dependencies explicitly
    company = await CompanyFactory.create_async(session=db_session)
    role = await RoleFactory.create_async(session=db_session)

    # Create user with explicit relationships
    user = await UserFactory.create_async(
        session=db_session,
        company_id=company.id,
        role_id=role.id,
        email="test@example.com"
    )

    assert user.id is not None
    assert user.email == "test@example.com"
```

### Complex Relationships

```python
from tests.factories import (
    OemFactory, MakeFactory, BatteryFactory,
    VehicleModelFactory, VehicleFactory, FleetFactory
)

async def test_vehicle_creation(db_session):
    # Build dependency chain explicitly
    oem = await OemFactory.create_async(session=db_session)
    make = await MakeFactory.create_async(session=db_session, oem_id=oem.id)
    battery = await BatteryFactory.create_async(session=db_session)
    vehicle_model = await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
    )

    company = await CompanyFactory.create_async(session=db_session)
    fleet = await FleetFactory.create_async(session=db_session, company_id=company.id)

    vehicle = await VehicleFactory.create_async(
        session=db_session,
        fleet_id=fleet.id,
        vehicle_model_id=vehicle_model.id,
        vin="5YJ3E1EA1KF123456"
    )
```

### User Authentication Testing

```python
async def test_login(db_session, app_client):
    company = await CompanyFactory.create_async(session=db_session)
    role = await RoleFactory.create_async(session=db_session)
    user = await UserFactory.create_async(
        session=db_session,
        company_id=company.id,
        role_id=role.id,
    )

    # UserFactory auto-attaches plain_password (defaults to "testpassword123")
    response = await app_client.post(
        "/v1/auth/login",
        json={"email": user.email, "password": user.plain_password}
    )
    assert response.status_code == 200
```

### Batch Creation

```python
async def test_multiple_users(db_session):
    company = await CompanyFactory.create_async(session=db_session)
    role = await RoleFactory.create_async(session=db_session)

    # Create 10 users at once
    users = await UserFactory.create_batch_async(
        session=db_session,
        size=10,
        company_id=company.id,
        role_id=role.id,
    )
    assert len(users) == 10
```

## Available Factories

### Core Business Models
- `CompanyFactory` - Companies
- `RoleFactory` - User roles
- `UserFactory` - Users (with password hashing)
- `FleetFactory` - Vehicle fleets
- `UserFleetFactory` - User-Fleet relationships
- `OemFactory` - OEMs (defaults to Tesla)
- `MakeFactory` - Makes (defaults to Tesla)

### Vehicle Models
- `RegionFactory` - Regions (defaults to France)
- `BatteryFactory` - Batteries (defaults to LFP 75kWh)
- `VehicleModelFactory` - Vehicle models (with trendlines)
- `VehicleFactory` - Vehicles (auto-generates VIN)
- `VehicleDataFactory` - Time-series data
- `VehicleStatusFactory` - Status tracking

### API Models
- `ApiUserFactory` - API users
- `ApiPricingPlanFactory` - Pricing plans
- `ApiUserPricingFactory` - User pricing
- `ApiCallLogFactory` - Call logs
- `ApiBillingFactory` - Billing records

### Reports
- `FlashReportCombinationFactory` - Flash reports
- `PremiumReportFactory` - Premium reports

### Tesla Schema
- `UserTeslaFactory` - Tesla users
- `UserTokenFactory` - Tesla tokens

See `tests/factories/` for complete implementations.

## Infrastructure Fixtures (conftest.py)

These fixtures are available in all tests:

- **`db_session`**: AsyncSession with automatic rollback after each test
- **`app_client`**: httpx.AsyncClient for testing FastAPI endpoints
- **`engine`**: AsyncEngine for database connection

Helper functions:
- `get_auth_headers(token)`: Create Authorization header
- `get_cookie_dict(response)`: Extract cookies from response

## Migration from Fixtures to Factories

### Old Way (Implicit Fixtures)
```python
async def test_old(db_session, foo_company, admin_role, foo_user):
    # What values does foo_company have? Unknown!
    # Hidden dependencies between fixtures
    assert foo_user.company_id == foo_company.id
```

### New Way (Explicit Factories)
```python
async def test_new(db_session):
    # Explicit - you see exactly what's created
    company = await CompanyFactory.create_async(session=db_session)
    role = await RoleFactory.create_async(session=db_session)
    user = await UserFactory.create_async(
        session=db_session,
        company_id=company.id,
        role_id=role.id,
    )
    assert user.company_id == company.id
```

## Best Practices

1. **Create only what you need** - Don't create unnecessary dependencies
2. **Be explicit** - Always show relationships clearly in test code
3. **Reuse within tests** - Create objects once, reuse for multiple operations
4. **Use custom values** - Override defaults when testing edge cases
5. **Keep tests isolated** - Each test creates its own data

## Troubleshooting

**Connection refused**: Ensure PostgreSQL is running (`docker-compose up -d postgres`)

**Schema errors** (`relation "..." does not exist`): Run migrations
```bash
uv run alembic -c src/db_models/alembic.ini upgrade head
```

**Import errors**: Ensure dependencies are installed (`uv sync --locked --all-extras`)

**Factory errors**: Check that all required foreign keys are provided:
```python
# BAD - missing required company_id
user = await UserFactory.create_async(session=db_session)

# GOOD - all FKs provided
company = await CompanyFactory.create_async(session=db_session)
role = await RoleFactory.create_async(session=db_session)
user = await UserFactory.create_async(
    session=db_session,
    company_id=company.id,
    role_id=role.id
)
```

## CI/CD

Tests run in CI with PostgreSQL sidecar. Integration tests are automatically skipped.

CI requirements:
1. PostgreSQL service/sidecar
2. Environment variables (DB credentials, auth secrets)
3. Dependencies via `uv sync --locked --all-extras`
4. Migrations via `uv run alembic -c src/db_models/alembic.ini upgrade head`

See `.github/workflows/test.yml` for complete workflow configuration.

## For More Details

See individual factory files in `tests/factories/` for:
- Available factory fields and defaults
- Special behavior (like `UserFactory.plain_password`)
- Custom factory methods

Example test file with factories: `tests/test_db_models/test_vehicle_models.py`
