"""Factories for API-related models."""

from datetime import date
from decimal import Decimal
from uuid import uuid4

from polyfactory import Use

from db_models.api import (
    ApiBilling,
    ApiCallLog,
    ApiPricingPlan,
    ApiUser,
    ApiUserPricing,
)
from tests.factories.base import BaseAsyncFactory


class ApiUserFactory(BaseAsyncFactory[ApiUser]):
    __model__ = ApiUser

    # user_id must be provided
    api_key = Use(lambda: str(uuid4()))
    is_active = True
    last_access = None


class ApiPricingPlanFactory(BaseAsyncFactory[ApiPricingPlan]):
    __model__ = ApiPricingPlan

    name = Use(lambda: f"Plan-{uuid4().hex[:8]}")
    description = "Test pricing plan"
    requests_limit = 1000
    max_distinct_vins = 100
    price_per_request = Decimal("0.10")


class ApiUserPricingFactory(BaseAsyncFactory[ApiUserPricing]):
    __model__ = ApiUserPricing

    # user_id and pricing_plan_id must be provided
    custom_requests_limit = None
    custom_max_distinct_vins = None
    custom_price_per_request = None
    effective_date = Use(lambda: date.today())
    expiration_date = None


class ApiCallLogFactory(BaseAsyncFactory[ApiCallLog]):
    __model__ = ApiCallLog

    # user_id must be provided
    vin = Use(lambda: f"5YJ3E1EA1KF{uuid4().hex[:6].upper()}")
    endpoint = "/vehicle/static"
    response_time = 125.5
    status_code = 200
    is_billed = False
    billed_at = None


class ApiBillingFactory(BaseAsyncFactory[ApiBilling]):
    __model__ = ApiBilling

    # user_id must be provided
    period_start = Use(lambda: date.today())
    period_end = Use(lambda: date.today())
    total_requests = 0
    distinct_vins = 0
    total_amount = Decimal("0.00")
    invoice_number = None
    paid = False
    payment_date = None


__all__ = [
    "ApiBillingFactory",
    "ApiCallLogFactory",
    "ApiPricingPlanFactory",
    "ApiUserFactory",
    "ApiUserPricingFactory",
]
