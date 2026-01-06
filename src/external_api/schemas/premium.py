from pydantic import BaseModel, ConfigDict, Field


class PremiumReportGeneration(BaseModel):
    job_id: str
    message: str
    estimated_duration: str


class PremiumReportPDFUrl(BaseModel):
    job_id: str
    vin: str
    message: str
    url: str | None
    error: str | None
    retry_info: str | None


class PremiumReportSync(BaseModel):
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
        None, description="Type of charging port (e.g., CCS, Type 2)"
    )
    image_url: str | None = Field(None, description="URL to vehicle image")


class BatteryReportInfo(BaseModel):
    """Battery information for premium report."""

    model_config = ConfigDict(from_attributes=True)

    manufacturer: str | None = Field(None, description="Battery manufacturer name")
    chemistry: str | None = Field(
        None, description="Battery chemistry type (e.g., NMC, LFP)"
    )
    type: str | None = Field(None, description="Battery type (e.g., LITHIUM-ION)")
    capacity: float = Field(description="Battery capacity in kWh")
    wltp_range: str = Field(description="WLTP certified range in km")
    consumption: int | None = Field(
        None, description="Average consumption in kWh/100km"
    )
    warranty_years: int = Field(description="Battery warranty duration in years")
    warranty_km: int = Field(description="Battery warranty mileage limit in km")


class SohChartData(BaseModel):
    """State of Health (SoH) chart data with trendline values."""

    model_config = ConfigDict(from_attributes=True)

    value: float = Field(description="Current SoH percentage (0-100)")
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

    uuid: str | None = Field(None, description="Report UUID")
    date: str = Field(description="Report generation date (dd/mm/YYYY)")
    delivered_by: str = Field(description="Report issuer organization")


class PremiumReportData(BaseModel):
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
