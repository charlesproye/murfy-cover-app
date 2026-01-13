import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware.base import BaseHTTPMiddleware

from ingestion.kafka_producer import stop_kafka_producer

from .bmw_router import bmw_router
from .kia_router import kia_router
from .volkswagen_router import volkswagen_router

LOGGER = logging.getLogger(__name__)
__VERSION__ = "1.0.0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestionnaire de cycle de vie de l'application.
    Exécuté au démarrage et à l'arrêt.
    """
    # Code exécuté au démarrage
    LOGGER.info(f"Démarrage de l'API. Version: {__VERSION__}")
    instrumentator.expose(app)

    yield

    # Code exécuté à l'arrêt
    LOGGER.info("Arrêt de l'API")
    # Stop Kafka producer if it was initialized
    await stop_kafka_producer()


app = FastAPI(
    title="Vehicle Data Receiver",
    version=__VERSION__,
    openapi_prefix="/to_be_defined_by_customer",
    lifespan=lifespan,
)
logging.basicConfig(level=logging.INFO)


# Middleware for testing
class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logging.info(f"Request Headers: {dict(request.headers)}")
        response = await call_next(request)
        return response


class MetricsProtectionMiddleware(BaseHTTPMiddleware):
    """Block /metrics access from external requests (via ingress)"""

    async def dispatch(self, request: Request, call_next):
        # If request is for /metrics and comes through ingress (has X-Forwarded-For),
        # block it. Internal cluster requests won't have this header.
        if request.url.path == "/metrics" and request.headers.get("X-Forwarded-For"):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
        return await call_next(request)


# Add middleware to FastAPI app
app.add_middleware(LogRequestMiddleware)  # type: ignore[arg-type]
app.add_middleware(MetricsProtectionMiddleware)  # type: ignore[arg-type]


app.include_router(volkswagen_router)
app.include_router(bmw_router)
app.include_router(kia_router)


instrumentator: Instrumentator = Instrumentator(
    should_group_status_codes=False,
    excluded_handlers=["/redoc", "/docs", "/openapi.json", "/metrics"],
).instrument(
    app,
    metric_namespace="ing_api_push",
)
