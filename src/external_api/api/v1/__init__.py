from fastapi import APIRouter

from external_api.api.v1.endpoints import (
    admin,
    auth,
    billing,
    dashboard,
    flash_report,
    in_life_data,
    individual,
    model,
    passport,
    premium_report,
    static_data,
    tesla,
)
from external_api.core.config import settings

# Création du routeur API principal
api_router = APIRouter()

# Route for the API
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
api_router.include_router(model.router, prefix="/model", tags=["Vehicle Model"])
api_router.include_router(
    static_data.router, prefix="/static_data", tags=["Static Data"]
)
api_router.include_router(
    in_life_data.router, prefix="/in_life_data", tags=["In life Data"]
)
api_router.include_router(billing.router, prefix="/billing", tags=["Billing"])
api_router.include_router(individual.router, prefix="/individual", tags=["Individual"])
api_router.include_router(
    premium_report.router, prefix="/premium_report", tags=["Reports"]
)

# Routes cachées de la documentation OpenAPI
hidden_router = APIRouter(include_in_schema=False)
# Passport routes (cachées)
hidden_router.include_router(passport.router, prefix="/passport", tags=["passport"])
# Les routes d'administration sont incluses mais ne sont pas exposées dans la documentation OpenAPI
hidden_router.include_router(admin.router, prefix="/admin", tags=["Administration"])
hidden_router.include_router(
    flash_report.flash_report_router, prefix="/flash_report", tags=["Flash Report"]
)
hidden_router.include_router(tesla.tesla_router, prefix="/tesla", tags=["Tesla"])
api_router.include_router(hidden_router)
