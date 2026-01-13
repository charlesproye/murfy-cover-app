import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi_pagination import add_pagination
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware.base import BaseHTTPMiddleware

from external_api import __VERSION__
from external_api.api.v1 import api_router
from external_api.core.config import settings
from external_api.core.http_client import HTTP_CLIENT

# BACKEND_CORS_ORIGINS=[
#     "http://localhost:3000"
# ]

# Structured logging configuration
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()


class MetricsProtectionMiddleware(BaseHTTPMiddleware):
    """Block /metrics access from external requests (via ingress)"""

    async def dispatch(self, request: Request, call_next):
        # If request is for /metrics and comes through ingress (has X-Forwarded-For),
        # block it. Internal cluster requests won't have this header.
        if request.url.path == "/metrics" and request.headers.get("X-Forwarded-For"):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
        return await call_next(request)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log requests and responses"""

    async def dispatch(self, request: Request, call_next):
        """
        Log incoming and outgoing requests.
        """
        # Log incoming request
        logger.info(
            "Incoming request",
            method=request.method,
            url=str(request.url),
            client=request.client.host if request.client else None,
        )

        # Process request
        try:
            response = await call_next(request)
            # Log response
            logger.info(
                "Response",
                status_code=response.status_code,
                method=request.method,
                url=str(request.url),
            )
            return response
        except Exception as e:
            # If error, log and return an error response
            logger.error(
                "Error processing request",
                error=str(e),
                method=request.method,
                url=str(request.url),
                exc_info=True,
            )
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Internal server error"},
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager of the application.
    Executed at startup and shutdown.
    """
    # Code executed at startup
    logger.info("API startup", version=__VERSION__)
    logger.info("Environment", environment=settings.ENVIRONMENT)
    HTTP_CLIENT.start()
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

# Add Routers
app.include_router(api_router, prefix=settings.API_V1_STR)
add_pagination(app)

# # Ajout du middleware de journalisation
app.add_middleware(RequestLoggingMiddleware)  # type: ignore[arg-type]
app.add_middleware(MetricsProtectionMiddleware)  # type: ignore[arg-type]

instrumentator: Instrumentator = Instrumentator(
    should_group_status_codes=False,
    excluded_handlers=["/redoc", "/docs", "/openapi.json", "/metrics"],
).instrument(
    app,
    metric_namespace="evalue_back",
)


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
