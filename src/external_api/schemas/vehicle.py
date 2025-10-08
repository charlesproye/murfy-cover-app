"""Schemas for vehicle related data"""

from datetime import date, datetime

from pydantic import UUID4, BaseModel, Field


class BatteryData(BaseModel):
    """Battery information"""

    id: UUID4 = Field(..., description="ID unique de la batterie")
    battery_name: str = Field(..., description="Nom de la batterie")
    battery_chemistry: str | None = Field(None, description="Chimie de la batterie")
    battery_oem: str | None = Field(None, description="Fabricant de la batterie")
    capacity: float | None = Field(None, description="Capacité totale en kWh")
    net_capacity: float | None = Field(None, description="Capacité nette en kWh")

    class Config:
        from_attributes = True


class OemData(BaseModel):
    """OEM information"""

    id: UUID4 = Field(..., description="ID unique du constructeur")
    oem_name: str = Field(..., description="Nom du constructeur")
    description: str | None = Field(None, description="Description du constructeur")

    class Config:
        from_attributes = True


class MakeData(BaseModel):
    """Make information"""

    id: UUID4 = Field(..., description="ID unique de la marque")
    make_name: str = Field(..., description="Nom de la marque")
    description: str | None = Field(None, description="Description de la marque")

    class Config:
        from_attributes = True


class VehicleModelData(BaseModel):
    """Vehicle model information"""

    id: UUID4 = Field(..., description="ID unique du modèle")
    model_name: str = Field(..., description="Nom du modèle")
    type: str | None = Field(None, description="Type de véhicule")
    version: str | None = Field(None, description="Version du modèle")
    autonomy: int | None = Field(None, description="Autonomie en km")
    url_image: str | None = Field(None, description="URL de l'image du véhicule")
    warranty_date: int | None = Field(None, description="Garantie en années")
    warranty_km: float | None = Field(None, description="Garantie en km")
    source: str | None = Field(None, description="Source des données")
    battery: BatteryData | None = None
    oem: OemData | None = None
    make: MakeData | None = None

    class Config:
        from_attributes = True


class VehicleBase(BaseModel):
    """Base vehicle information"""

    fleet_id: UUID4
    region_id: UUID4
    vehicle_model_id: UUID4
    vin: str
    activation_status: bool | None = None
    is_displayed: bool | None = None
    start_date: date | None = None
    licence_plate: str | None = None
    end_of_contract_date: date | None = None
    last_date_data: date | None = None

    class Config:
        from_attributes = True


class VehicleRead(VehicleBase):
    """Vehicle read model"""

    id: UUID4

    class Config:
        from_attributes = True


class StaticVehicleData(BaseModel):
    """Static vehicle information returned by the API"""

    vin: str = Field(..., description="Numéro d'identification du véhicule (VIN)")
    start_date: date | None = Field(None, description="Date de début de contrat")
    make_name: str | None = Field(None, description="Nom de la marque")
    model_name: str | None = Field(None, description="Nom du modèle")
    version: str | None = Field(None, description="Version du modèle")
    type: str | None = Field(None, description="Type de véhicule")
    battery_name: str | None = Field(None, description="Nom de la batterie")
    battery_chemistry: str | None = Field(None, description="Chimie de la batterie")
    capacity: float | None = Field(None, description="Capacité totale en kWh")
    net_capacity: float | None = Field(None, description="Capacité nette en kWh")
    autonomy: int | None = Field(None, description="Autonomie en km")
    warranty_date: int | None = Field(None, description="Garantie en années")
    warranty_km: float | None = Field(None, description="Garantie en km")
    source: str | None = Field(None, description="Source des données")

    class Config:
        from_attributes = True


class DynamicVehicleData(BaseModel):
    """Dynamic vehicle information returned by the API"""

    vin: str = Field(..., description="Numéro d'identification du véhicule (VIN)")
    odometer: float | None = Field(None, description="Odomètre en km")
    region: str | None = Field(None, description="Région du véhicule")
    speed: float | None = Field(None, description="Vitesse en km/h")
    location: str | None = Field(None, description="Localisation du véhicule")
    soh: float | None = Field(None, description="State of Health de la batterie en %")
    cycles: float | None = Field(None, description="Nombre de cycles de la batterie")
    consumption: float | None = Field(
        None, description="Consommation moyenne en kWh/100km"
    )
    soh_comparison: float | None = Field(
        None, description="Comparaison du SOH avec d'autres véhicules"
    )
    timestamp: datetime | None = Field(None, description="Horodatage des données")
    level_1: float | None = Field(
        None, description="Niveau 1 de charge (1.4-1.9 kW, 120V, AC, 12-16 Ah)"
    )
    level_2: float | None = Field(
        None, description="Niveau 2 de charge (1.9.3-19.2 kW, 208V, AC, 32-64 Ah)"
    )
    level_3: float | None = Field(None, description="Niveau 3 de charge (> 50kW, DC)")
    soh_oem: float | None = Field(None, description="SOH selon le constructeur en %")

    class Config:
        from_attributes = True


class VehicleActivationRequest(BaseModel):
    """Request model for vehicle activation"""

    vins: list[str] = Field(
        ...,
        description="Liste des VINs à activer. Pour les véhicules Tesla, un seul VIN doit être soumis à la fois. Les VINs doivent tous être de la même marque.",
        min_items=1,
    )
    make: str | None = Field(
        None, description="Marque des véhicules (une seule marque par requête)"
    )

    class Config:
        from_attributes = True


class VehicleActivationResponse(BaseModel):
    """Response model for vehicle activation"""

    vins: list[str] = Field(..., description="List of processed VINs")
    success: bool = Field(..., description="Global operation status")
    message: str = Field(..., description="Message describing the operation result")
    errors: dict[str, str] = Field(
        default_factory=dict, description="Error details by VIN"
    )

    class Config:
        from_attributes = True


class VehicleEligibilityResponse(BaseModel):
    """Response model for vehicle eligibility check"""

    vin: str = Field(..., description="VIN of the checked vehicle")
    exists: bool = Field(
        ..., description="Indicates if the vehicle exists in the database"
    )
    is_eligible: bool = Field(
        ..., description="Indicates if the vehicle is eligible for activation"
    )
    is_activated: bool = Field(
        ..., description="Indicates if the vehicle is already activated"
    )
    message: str = Field(
        ..., description="Explanatory message about vehicle eligibility"
    )

    class Config:
        from_attributes = True

