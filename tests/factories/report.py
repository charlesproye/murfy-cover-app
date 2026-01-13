"""Factories for report-related models."""

from uuid import uuid4

from polyfactory import Use

from db_models.enums import LanguageEnum
from db_models.report import FlashReportCombination, Report, ReportType
from tests.factories.base import BaseAsyncFactory


class FlashReportCombinationFactory(BaseAsyncFactory[FlashReportCombination]):
    __model__ = FlashReportCombination

    vin = Use(lambda: f"5YJ3E1EA1KF{uuid4().hex[:6].upper()}")
    make = "tesla"
    model = "model 3"
    type = "long range awd"
    version = "2021"
    odometer = 50000
    token = Use(lambda: str(uuid4()))
    language = LanguageEnum.EN


class PremiumReportFactory(BaseAsyncFactory[Report]):
    __model__ = Report

    # vehicle_id must be provided
    report_url = Use(lambda: f"https://s3.example.com/reports/{uuid4()}.pdf")
    task_id = Use(lambda: str(uuid4()))
    report_type = ReportType.premium


__all__ = [
    "FlashReportCombinationFactory",
    "PremiumReportFactory",
]
