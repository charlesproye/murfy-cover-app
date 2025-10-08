import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware

from external_api.api.v1 import api_router  # as api_router_v1
from external_api.core.config import settings

# BACKEND_CORS_ORIGINS=[
#     "http://localhost:3000"
# ]

# Configuration du logging structuré
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware pour journaliser les requêtes et réponses"""

    async def dispatch(self, request: Request, call_next):
        """
        Journalise les requêtes entrantes et sortantes.
        """
        # Journalisation de la requête entrante
        logger.info(
            "Requête entrante",
            method=request.method,
            url=str(request.url),
            client=request.client.host if request.client else None,
        )

        # Traitement de la requête
        try:
            response = await call_next(request)
            # Journalisation de la réponse
            logger.info(
                "Réponse",
                status_code=response.status_code,
                method=request.method,
                url=str(request.url),
            )
            return response
        except Exception as e:
            # En cas d'erreur, journaliser et retourner une réponse d'erreur
            logger.error(
                "Erreur lors du traitement de la requête",
                error=str(e),
                method=request.method,
                url=str(request.url),
                exc_info=True,
            )
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Erreur interne du serveur"},
            )


# class ProxyHeadersMiddleware(BaseHTTPMiddleware):
#     """Middleware to handle proxy headers like X-Forwarded-Prefix"""

#     async def dispatch(self, request: Request, call_next):
#         """
#         Adjust path info based on X-Forwarded-Prefix header
#         """
#         forwarded_prefix = request.headers.get("X-Forwarded-Prefix", "")

#         # Log the prefix for debugging
#         if forwarded_prefix:
#             logger.info("X-Forwarded-Prefix detected", prefix=forwarded_prefix)
#             # We're in production mode if we get this header
#             os.environ["ENVIRONMENT"] = "production"
#         else:
#             # Local development
#             if is_local:
#                 logger.info("Running in local development mode")

#         response = await call_next(request)
#         return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestionnaire de cycle de vie de l'application.
    Exécuté au démarrage et à l'arrêt.
    """
    # Code exécuté au démarrage
    logger.info("Démarrage de l'API", version="1.1.0")
    logger.info("Environnement", environment=settings.ENVIRONMENT)
    # Fourniture de l'application
    yield

    # Code exécuté à l'arrêt
    logger.info("Arrêt de l'API")


# # Fonction pour déterminer si on est en environnement local
# def is_local_environment() -> bool:
#     """
#     Détecte si l'application tourne en environnement local, développement ou en production.
#     En production, on s'attend à ce que l'en-tête X-Forwarded-Prefix soit présent.
#     """
#     return os.getenv("ENVIRONMENT", "development") == "development"


# Détermine si nous sommes en environnement local
# is_local = is_local_environment()

# Préfixe à utiliser pour les URL OpenAPI
root_path = "/api" if settings.ENVIRONMENT == "proxy" else ""

app = FastAPI(
    title=f"{settings.PROJECT_NAME} API",
    description="External API for accessing vehicle data with a pricing system.",
    version="1.1.0",
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
    CORSMiddleware,
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Routers
app.include_router(api_router, prefix=settings.API_V1_STR)
add_pagination(app)

# # Ajout du middleware de journalisation
app.add_middleware(RequestLoggingMiddleware)

# # Ajout du middleware pour les en-têtes de proxy
# app.add_middleware(ProxyHeadersMiddleware)


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
        "version": "1.2.0",
        "documentation": docs_url,
    }


# @app.get("/")
# async def root():
#     return {"message": f"Welcome to {settings.PROJECT_NAME} {settings.API_V1_STR}"}


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    """
    return {"status": "healthy"}

