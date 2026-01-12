from fastapi import APIRouter

from external_api.api.v1.endpoints import (
    admin,
    auth,
    billing,
    dashboard,
    flash,
    flash_report,
    in_life_data,
    individual,
    passport,
    premium,
    static_data,
    tesla,
    vehicle_command,
)

# Create main API router
api_router = APIRouter()

# Route for the API
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
api_router.include_router(
    static_data.router, prefix="/static_data", tags=["Static Data"]
)
api_router.include_router(flash.router, prefix="/flash", tags=["Flash"])
api_router.include_router(premium.router, prefix="/premium", tags=["Premium"])
api_router.include_router(individual.router, prefix="/individual", tags=["Individual"])

api_router.include_router(
    vehicle_command.router, prefix="/vehicle-command", tags=["Vehicle command"]
)

# Hidden routes for OpenAPI documentation
hidden_router = APIRouter(include_in_schema=False)

hidden_router.include_router(
    in_life_data.router, prefix="/in_life_data", tags=["In life Data"]
)
# Passport routes (hidden)
hidden_router.include_router(passport.router, prefix="/passport", tags=["passport"])
# Admin routes are included but not exposed in OpenAPI documentation
hidden_router.include_router(admin.router, prefix="/admin", tags=["Administration"])
hidden_router.include_router(
    flash_report.flash_report_router, prefix="/flash_report", tags=["Flash Report"]
)
hidden_router.include_router(billing.router, prefix="/billing", tags=["Billing"])
hidden_router.include_router(tesla.tesla_router, prefix="/tesla", tags=["Tesla"])

api_router.include_router(hidden_router)
