# External API BIB Batteries

API providing access to vehicle data with authentication system and billing per request/VIN.

## 🚀 Features

- **Secure Authentication**: JWT Tokens System
- **Vehicle Data Access**:
  - Static Data: model, version, capacity, warranty
  - Dynamic Data: state of health (SOH), mileage, additional data
- **Pricing System**:
  - Different pricing plans (Basic, Premium, Enterprise)
  - Usage-based billing (per request/VIN)
  - Configurable rate limits per plan
- **Advanced Logging**: Structured logs and complete traceability
- **Data Simulator**: Generation of mock data for testing

## 🛠️ Technologies

- **FastAPI**: High-performance API Framework
- **SQLModel/SQLAlchemy**: ORM for database management
- **PostgreSQL**: Relational database
- **Redis**: Cache and rate limiter manager
- **Pydantic**: Data validation and serialization
- **Docker/Docker Compose**: Containerization and orchestration
- **UV**: Python environment management
- **Structlog**: Structured logging

## 📋 Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/)

## 🚀 Installation

### With Docker (recommended)

```bash
# Clone the repository
git clone <repo-url>
cd api_externe

# Configure environment variables
cp .env.example .env
# Edit the .env file with your own values

# Start containers
docker-compose up -d external_api
```

### Local Development with UV

```bash
# Start application in development mode
uv run uvicorn src.external_api.app:app --reload --port 4000
```

## 📝 Configuration

The same /.env file is used by data analyze and external_api.
Copy the `.env.example` file to `.env` and adjust values according to your environment.

# Redis

REDIS_HOST=localhost
REDIS_PORT=6379

# Security

SECRET_KEY=your-secret-key
ACCESS_TOKEN_EXPIRE_MINUTES=60

```

## 🔍 API Documentation

Once the API is started, interactive documentation is available at:

- Swagger UI Documentation: http://localhost:4000/api/v1/docs
- ReDoc Documentation: http://localhost:4000/api/v1/redoc

## 🔰 Usage

### Authentication

1. Create a user account with `POST /api/v1/auth/register`
2. Get a JWT token with `POST /api/v1/auth/login`
3. Use this token in the Authorization header for subsequent requests

### Model Data Access

- Trendline Data: `GET /api/v1/vehicle/static/{vin}`
- Warranty Data: `GET /api/v1/vehicle/dynamic/{vin}`

### Vehicle Data Access

- Static Data: `GET /api/v1/vehicle/static/{vin}`
- Dynamic Data: `GET /api/v1/vehicle/dynamic/{vin}`

### Billing

- Check your usage: `GET /api/v1/billing/usage`
- Mark calls as billed: `POST /api/v1/billing/pay`

## 📊 Monitoring and Maintenance

### Logs

Logs are emitted in structured JSON format, facilitating their integration with systems like ELK or Grafana.

### Monitoring

The API exposes a `/health` endpoint for health checks, compatible with Kubernetes and other monitoring systems.

## 📈 Pricing Plans

| Plan       | Requests/day | Price per request |
| ---------- | ------------ | ----------------- |
| Basic      | 100          | €0.10             |
| Premium    | 1000         | €0.05             |
| Enterprise | Unlimited    | €0.03             |

## 📝 License

This project is under MIT license. See the `LICENSE` file for more details.

## 📮 Contact

For any questions or support, please contact the BIB Batteries team.
```
