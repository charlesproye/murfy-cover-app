"""
Factory exports for polyfactory-based test data generation.

Usage:
    from tests.factories import CompanyFactory, UserFactory

    async def test_something(db_session):
        company = await CompanyFactory.create_async(
            session=db_session,
            name="Custom Company"
        )
        user = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
        )
"""

from tests.factories.api import (
    ApiBillingFactory,
    ApiCallLogFactory,
    ApiPricingPlanFactory,
    ApiUserFactory,
    ApiUserPricingFactory,
)
from tests.factories.core import (
    CompanyFactory,
    FleetFactory,
    MakeFactory,
    OemFactory,
    RoleFactory,
    UserFactory,
    UserFleetFactory,
)
from tests.factories.report import (
    FlashReportCombinationFactory,
    PremiumReportFactory,
)
from tests.factories.tesla import (
    UserTeslaFactory,
    UserTokenFactory,
)
from tests.factories.vehicle import (
    BatteryFactory,
    RegionFactory,
    VehicleDataFactory,
    VehicleFactory,
    VehicleModelFactory,
    VehicleStatusFactory,
)

__all__ = [  # noqa: RUF022 - organized by domain for readability
    # Core business
    "CompanyFactory",
    "FleetFactory",
    "MakeFactory",
    "OemFactory",
    "RoleFactory",
    "UserFactory",
    "UserFleetFactory",
    # Vehicles
    "BatteryFactory",
    "RegionFactory",
    "VehicleDataFactory",
    "VehicleFactory",
    "VehicleModelFactory",
    "VehicleStatusFactory",
    # API
    "ApiBillingFactory",
    "ApiCallLogFactory",
    "ApiPricingPlanFactory",
    "ApiUserFactory",
    "ApiUserPricingFactory",
    # Reports
    "FlashReportCombinationFactory",
    "PremiumReportFactory",
    # Tesla
    "UserTeslaFactory",
    "UserTokenFactory",
]
