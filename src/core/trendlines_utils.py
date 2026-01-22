from core.models.data_source import DataSource
from core.models.trendlines import Trendlines
from db_models import VehicleModel


def get_flash_trendlines(vehicle_model: VehicleModel) -> Trendlines:
    return get_trendlines_with_priority(vehicle_model, priority=DataSource.OEM)


def get_readout_trendlines(vehicle_model: VehicleModel) -> Trendlines:
    return get_trendlines_with_priority(vehicle_model, priority=DataSource.BIB)


def get_premium_trendlines(vehicle_model: VehicleModel) -> Trendlines:
    return get_trendlines_with_priority(vehicle_model, priority=DataSource.BIB)


def get_trendlines_with_priority(
    vehicle_model: VehicleModel, priority: DataSource
) -> Trendlines:
    trendlines = None

    if vehicle_model.trendline_oem:
        if (
            vehicle_model.trendline_oem_min is None
            or vehicle_model.trendline_oem_max is None
        ):
            raise ValueError(
                "Trendlines min/max must be provided if using OEM trendline"
            )

        trendlines = Trendlines(
            main=vehicle_model.trendline_oem,
            minimum=vehicle_model.trendline_oem_min,
            maximum=vehicle_model.trendline_oem_max,
            source=DataSource.OEM,
        )

        if priority == DataSource.OEM:
            return trendlines

    if vehicle_model.trendline_bib:
        if (
            vehicle_model.trendline_bib_min is None
            or vehicle_model.trendline_bib_max is None
        ):
            raise ValueError(
                "Trendlines min/max must be provided if using BIB trendline"
            )

        trendlines = Trendlines(
            main=vehicle_model.trendline_bib,
            minimum=vehicle_model.trendline_bib_min,
            maximum=vehicle_model.trendline_bib_max,
            source=DataSource.BIB,
        )

        if priority == DataSource.BIB:
            return trendlines

    if trendlines:
        return trendlines

    raise ValueError(f"No trendlines found for vehicle model {vehicle_model.id}")
