import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TypeInfo(BaseModel):
    model_type: str = Field(..., description="Model type")
    versions: list[str] | None = Field(None, description="Versions")
    has_trendline_bib: bool = Field(..., description="Has trendline BIB")
    has_trendline_oem: bool = Field(..., description="Has trendline OEM")


class ModelInfo(BaseModel):
    model_name: str = Field(..., description="Model name")
    types: list[TypeInfo] = Field(..., description="Types")


class MakeInfo(BaseModel):
    make_name: str = Field(..., description="Make name")
    models: list[ModelInfo] = Field(..., description="Models")


class AllMakesModelsInfo(BaseModel):
    makes: list[MakeInfo] = Field(..., description="Makes")


class ModelType(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    make: str = Field(..., description="Make (company that made the car)")
    model_name: str = Field(..., description="Model name")
    model_type: str | None = Field(None, description="Model type")
    commissioning_date: datetime | None = Field(..., description="Commissioning date")
    end_of_life_date: datetime | None = Field(..., description="End of life date")
    has_soh_estimation_bib: bool = Field(..., description="Has SoH estimation BIB")
    has_soh_estimation_oem: bool = Field(..., description="Has SoH estimation OEM")


class VehicleModelData(BaseModel):
    commissioning_date: datetime | None = Field(None, description="Commissioning date")
    end_of_life_date: datetime | None = Field(None, description="End of life date")
    autonomy: int | None = Field(None, description="Autonomy (km)")
    autonomy_city_winter: int | None = Field(
        None, description="Autonomy city winter (km)"
    )
    autonomy_city_summer: int | None = Field(
        None, description="Autonomy city summer (km)"
    )
    autonomy_highway_winter: int | None = Field(
        None, description="Autonomy highway winter (km)"
    )
    autonomy_highway_summer: int | None = Field(
        None, description="Autonomy highway summer (km)"
    )
    autonomy_combined_winter: int | None = Field(
        None, description="Autonomy combined winter (km)"
    )
    autonomy_combined_summer: int | None = Field(
        None, description="Autonomy combined summer (km)"
    )
    warranty_date: int | None = Field(None, description="Warranty date (years)")
    warranty_km: float | None = Field(None, description="Warranty km")
    maximum_speed: int | None = Field(None, description="Maximum speed (km/h)")
    charge_plug_location: str | None = Field(None, description="Charge plug location")
    charge_plug_type: str | None = Field(None, description="Charge plug type")
    expected_consumption: int | None = Field(
        None, description="Expected consumption (kWh)"
    )
    fast_charge_max_power: int | None = Field(
        None, description="Fast charge max power (kW)"
    )
    fast_charge_duration: int | None = Field(
        None, description="Fast charge duration (minutes)"
    )
    standard_charge_duration: int | None = Field(
        None, description="Standard charge duration (minutes)"
    )
    ac_charge_duration: int | None = Field(
        None, description="AC charge duration (minutes)"
    )

    model_config = ConfigDict(from_attributes=True)


class BatteryModelData(BaseModel):
    battery_chemistry: str | None = Field(None, description="Battery chemistry")
    battery_oem: str | None = Field(None, description="Battery OEM")
    battery_capacity: float | None = Field(
        None, description="Battery capacity (kWh)", alias="capacity"
    )
    battery_net_capacity: float | None = Field(
        None, description="Battery net capacity (kWh)", alias="net_capacity"
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ModelData(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    make: str = Field(..., description="Make (company that made the car)")
    model_name: str = Field(..., description="Model name")
    model_type: str | None = Field(None, description="Model type")
    version: str | None = Field(None, description="Version")
    vehicle: VehicleModelData = Field(..., description="Vehicle model data")
    battery: BatteryModelData = Field(..., description="Battery model data")


class ModelTrendline(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    model_name: str = Field(..., description="Model name")
    model_type: str | None = Field(None, description="Model type")
    version: str | None = Field(None, description="Version")
    trendline_bib_mean: str | None = Field(
        None, description="Trendline BIB equation", nullable=True
    )
    trendline_bib_min: str | None = Field(
        None, description="Trendline BIB min", nullable=True
    )
    trendline_bib_max: str | None = Field(
        None, description="Trendline BIB max", nullable=True
    )
    trendline_oem: str | None = Field(None, description="Trendline OEM", nullable=True)
    trendline_oem_min: str | None = Field(
        None, description="Trendline OEM min", nullable=True
    )
    trendline_oem_max: str | None = Field(
        None, description="Trendline OEM max", nullable=True
    )
    comment: str | None = Field(..., description="Comment")
