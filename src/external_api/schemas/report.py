from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from db_models.report import ReportType


class ReportGeneration(BaseModel):
    job_id: str
    message: str
    estimated_duration: str


class ReportPDFUrl(BaseModel):
    job_id: str | None = None
    vin: str
    message: str | None = None
    url: str | None = None
    expires_at: datetime | None = None
    error: str | None = None
    retry_info: str | None = None


class ReportSync(BaseModel):
    vin: str
    url: str
    message: str


# === Comprehensive Report Data Models ===


class VehicleReportInfo(BaseModel):
    """Vehicle information for premium report."""

    model_config = ConfigDict(from_attributes=True)

    vin: str = Field(description="Vehicle Identification Number")
    oem: str = Field(description="Vehicle OEM name")
    model: str = Field(description="Vehicle model name")
    registration: str | None = Field(None, description="License plate number")
    mileage: str = Field(description="Current odometer reading in km")
    type: str | None = Field(
        None,
        description="Vehicle variant/trim (e.g., '73 kwh awd', 'long range', 'performance')",
    )
    charging_port: str | None = Field(
        None, description="Type of charging port (e.g. Type 2)"
    )
    fast_charging_port: str | None = Field(
        None, description="Type of fast charging port (e.g. CCS)"
    )
    image_url: str | None = Field(None, description="URL to vehicle image")


class BatteryReportInfo(BaseModel):
    """Battery information for premium report."""

    model_config = ConfigDict(from_attributes=True)

    manufacturer: str | None = Field(None, description="Battery manufacturer name")
    chemistry: str | None = Field(
        None, description="Battery chemistry type (e.g., NMC, LFP)"
    )
    capacity: float | None = Field(description="Battery capacity in kWh")
    net_capacity: float | None = Field(description="Battery net capacity in kWh")
    wltp_range: str = Field(description="WLTP certified range in km")
    consumption: int | None = Field(
        None, description="Average consumption in kWh/100km"
    )
    warranty_years: int = Field(description="Battery warranty duration in years")
    warranty_km: int = Field(description="Battery warranty mileage limit in km")


class SohChartData(BaseModel):
    """State of Health (SoH) chart data with trendline values."""

    model_config = ConfigDict(from_attributes=True)

    soh_bib: float | None = Field(
        None, description="Current BIB SoH percentage (0-100)"
    )
    soh_readout: float | None = Field(
        None, description="Readout SoH percentage (0-100)"
    )
    grade: str | None = Field(None, description="BIB battery health grade (A-E)")
    odometer_points: list[int] = Field(
        description="Mileage points for chart x-axis in km"
    )
    current_km: int = Field(description="Current vehicle mileage in km")
    soh_trendline: list[float] = Field(
        description="SoH trendline values corresponding to labels"
    )
    soh_min: list[float] = Field(
        description="Lower confidence bound for SoH prediction"
    )
    soh_max: list[float] = Field(
        description="Upper confidence bound for SoH prediction"
    )

    readout_only: bool = Field(description="Whether the report is a readout report")


class ChargingInfo(BaseModel):
    """Charging information for a specific charger type (20% to 80% charge)."""

    model_config = ConfigDict(from_attributes=True)

    type: str = Field(description="Charger speed category (Slow/Medium/Fast/Fastest)")
    duration: str = Field(description="Formatted charging duration (e.g., '7h44')")
    duration_hours: float = Field(description="Charging duration in hours")
    power: int = Field(description="Charging power in kW")
    typical: str = Field(
        description="Typical charging location (e.g., 'Wall box', 'Supercharger')"
    )
    percentage: float = Field(
        description="Relative duration compared to slowest charger (for visualization)"
    )


class AutonomyInfo(BaseModel):
    """Estimated range for different driving conditions."""

    model_config = ConfigDict(from_attributes=True)

    cycle: str = Field(
        description="Driving cycle type (Urbain/Autoroute/Mixte - Urban/Highway/Mixed)"
    )
    summer: str = Field(description="Estimated range in summer conditions")
    winter: str = Field(description="Estimated range in winter conditions")


class WarrantyInfo(BaseModel):
    """Battery warranty coverage information with remaining limits."""

    model_config = ConfigDict(from_attributes=True)

    remaining_km: int = Field(description="Remaining warranty mileage in km")
    km_percentage: float = Field(
        description="Percentage of warranty mileage remaining (0-100)"
    )
    remaining_time: str | None = Field(
        None, description="Remaining warranty time (e.g., '5 ans et 3 mois')"
    )
    time_percentage: float | None = Field(
        None, description="Percentage of warranty time remaining (0-100)"
    )
    warranty_years: int = Field(description="Total warranty duration in years")
    warranty_km: int = Field(description="Total warranty mileage limit in km")


class ReportMetadata(BaseModel):
    """Report generation metadata."""

    model_config = ConfigDict(from_attributes=True)

    uuid: UUID | None = Field(
        None,
        description="Report UUID. Only used if the report is generated for a PDF and inserted in the database.",
    )
    date: str = Field(description="Report generation date (dd/mm/YYYY)")
    delivered_by: str = Field(description="Report issuer organization")
    report_type: ReportType = Field(description="Report type")
    qr_code_url: str | None = Field(None, description="QR code as base64 PNG data URL")
    report_verification_url: str | None = Field(
        None, description="Report verification URL"
    )

    @model_validator(mode="after")
    def if_uuid_check(self) -> "ReportMetadata":
        if self.uuid is not None and not self.report_verification_url:
            raise ValueError(
                "report_verification_url must not be None if uuid is provided"
            )

        if self.uuid is not None and not self.qr_code_url:
            raise ValueError("qr_code_url must not be None if uuid is provided")

        return self


class ReportData(BaseModel):
    """
    Complete premium battery report data.

    Includes vehicle information, battery specifications, health metrics,
    charging estimates, range predictions, and warranty coverage.
    """

    model_config = ConfigDict(from_attributes=True)

    vehicle: VehicleReportInfo = Field(
        description="Vehicle identification and specifications"
    )
    battery: BatteryReportInfo = Field(
        description="Battery specifications and warranty"
    )
    soh_data: SohChartData = Field(
        description="State of Health analysis with historical and predicted values"
    )
    charging_data: list[ChargingInfo] = Field(
        description="Charging time and cost estimates for different charger types"
    )
    autonomy_data: list[AutonomyInfo] = Field(
        description="Estimated range for different driving conditions"
    )
    warranty_data: WarrantyInfo = Field(description="Battery warranty coverage status")
    report: ReportMetadata = Field(description="Report generation information")


# === Report Verification Models ===


class ReportVerificationRequest(BaseModel):
    """Request model for report verification."""

    vin_last_4: str = Field(
        ...,
        min_length=4,
        max_length=4,
        pattern=r"^\d{4}$",
        description="Last 4 digits of the VIN (always numeric)",
    )


class ReportVerificationResponse(BaseModel):
    """Response model for report verification."""

    verified: bool = Field(description="Whether the VIN matches the report")
    pdf_url: str | None = Field(None, description="Presigned URL to the PDF report")
    report_date: str | None = Field(None, description="Date the report was generated")
    report_type: ReportType | None = Field(None, description="Type of the report")
