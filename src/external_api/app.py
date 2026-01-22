import logging
from contextlib import asynccontextmanager

import appsignal
from fastapi import FastAPI, Request
from fastapi.exception_handlers import (
    http_exception_handler,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi_pagination import add_pagination
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from external_api import __VERSION__
from external_api.api.v1 import api_router
from external_api.core.config import settings
from external_api.core.http_client import HTTP_CLIENT

# BACKEND_CORS_ORIGINS=[
#     "http://localhost:3000"
# ]


logger = logging.getLogger(__name__)


class MetricsProtectionMiddleware(BaseHTTPMiddleware):
    """Block /metrics access from external requests (via ingress)"""

    async def dispatch(self, request: Request, call_next):
        # If request is for /metrics and comes through ingress (has X-Forwarded-For),
        # block it. Internal cluster requests won't have this header.
        if request.url.path == "/metrics" and request.headers.get("X-Forwarded-For"):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager of the application.
    Executed at startup and shutdown.
    """
    # Code executed at startup
    logger.info(f"API startup. Version: {__VERSION__}")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    HTTP_CLIENT.start()
    appsignal.start()
    instrumentator.expose(app)

    for route in app.routes:
        if (
            hasattr(route, "path")
            and route.path == "/metrics"
            and isinstance(route, APIRoute)
        ):
            route.include_in_schema = False
            break

    yield

    logger.info("API shutdown")


# Prefix to use for OpenAPI URLs
root_path = "/api" if settings.ENVIRONMENT == "proxy" else ""

app = FastAPI(
    title="BIB Batteries API",
    description="BIB Batteries API enables to access SoH, SoH estimated, dynamic and static data on your vehicles.",
    version=__VERSION__,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    # Uncomment the following line to completely disable OpenAPI documentation:
    # openapi_url=None,
    docs_url=f"{settings.API_V1_STR}/docs",
    redoc_url=f"{settings.API_V1_STR}/redoc",
    lifespan=lifespan,
    root_path=root_path,  # "/api" if not is_local else "",
    openapi_tags=[
        # {"name": "Authentication", "description": "Authentication operations"},
        # # {"name": "Vehicle Model", "description": "Statistical data on vehicle models"},
        # {"name": "Individual Vehicles", "description": "Data on individual vehicles"},
        # {"name": "Billing", "description": "Billing operations and usage tracking"},
    ],
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request, exc):
    if exc.status_code == 500:
        logger.exception("HTTP exception", exc_info=exc)
    else:
        logger.warning(f"HTTP exception: {exc.status_code} {exc.detail}")
    return await http_exception_handler(request, exc)


# Add Routers
app.include_router(api_router, prefix=settings.API_V1_STR)
add_pagination(app)

# # Ajout du middleware de journalisation
app.add_middleware(MetricsProtectionMiddleware)  # type: ignore[arg-type]

instrumentator: Instrumentator = Instrumentator(
    should_group_status_codes=False,
    excluded_handlers=["/redoc", "/docs", "/openapi.json", "/metrics"],
).instrument(
    app,
    metric_namespace="evalue_back",
)
FastAPIInstrumentor().instrument_app(app)


@app.get("/", include_in_schema=False)
async def root():
    """
    Root entry point of the API.
    """
    # Determine the documentation URL based on environment
    docs_path = f"{settings.API_V1_STR}/docs"
    docs_url = docs_path  # if is_local else f"/api{docs_path}"

    return {
        "message": f"Welcome to the {settings.PROJECT_NAME} API",
        "version": __VERSION__,
        "documentation": docs_url,
    }


@app.get("/health", include_in_schema=False)
async def health_check():
    """
    Health check endpoint.
    """
    return {"status": "healthy"}
