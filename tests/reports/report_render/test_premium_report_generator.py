"""Tests for PremiumReportGenerator class and report rendering."""

import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from db_models.company import Oem
from db_models.vehicle import Battery, Vehicle, VehicleData, VehicleModel
from external_api.core.config import settings
from reports.report_render.premium_report_generator import PremiumReportGenerator


def _create_test_models() -> tuple[
    Vehicle, VehicleModel, Battery, Oem, VehicleData, str
]:
    """Create simple test DB model instances for premium report generation."""
    # Create OEM
    oem = Oem(
        id=uuid.uuid4(),
        oem_name="Tesla",
        description="Tesla Inc.",
    )

    # Create Battery
    battery = Battery(
        id=uuid.uuid4(),
        battery_type="LITHIUM-ION",
        battery_chemistry="NMC",
        battery_oem="SAMSUNG SDI",
        capacity=42.2,
        battery_warranty_period=8,
        battery_warranty_mileage=160_000,
    )

    # Create VehicleModel
    vehicle_model = VehicleModel(
        id=uuid.uuid4(),
        model_name="Model Y Standard",
        type="SUV 4H",
        oem_id=oem.id,
        make_id=uuid.uuid4(),
        battery_id=battery.id,
        autonomy=308,
        warranty_km=160_000,
        warranty_date=8,  # 8 years warranty
        source="test",
        trendline={"trendline": "1 - 0.03 * np.log1p(x/10000)"},
        trendline_min={"trendline": "0.99 - 0.04 * np.log1p(x/10000)"},
        trendline_max={"trendline": "1.01 - 0.02 * np.log1p(x/10000)"},
        expected_consumption=16.1,
        maximum_speed=150,
        charge_plug_type="CCS COMBO",
        # Autonomy data for different driving cycles
        autonomy_city_summer=380,
        autonomy_city_winter=320,
        autonomy_highway_summer=290,
        autonomy_highway_winter=240,
        autonomy_combined_summer=335,
        autonomy_combined_winter=280,
    )

    # Create Vehicle (started 3 years ago for warranty calculation)
    vehicle = Vehicle(
        id=uuid.uuid4(),
        fleet_id=uuid.uuid4(),
        region_id=uuid.uuid4(),
        vehicle_model_id=vehicle_model.id,
        vin="LRWAE7EKXRC152510",
        bib_score="A",
        activation_status=True,
        is_eligible=True,
        licence_plate="VX-604-VP",
        start_date=datetime.now() - timedelta(days=365 * 7.5),
    )

    # Create VehicleData
    vehicle_data = VehicleData(
        id=uuid.uuid4(),
        vehicle_id=vehicle.id,
        odometer=20000.0,
        soh=0.95,
        consumption=16.1,
        timestamp=datetime.now(),
    )

    url_image = (
        "https://bib-batteries-assets.s3.fr-par.scw.cloud/car_images/model_y_256px.png"
    )

    return vehicle, vehicle_model, battery, oem, vehicle_data, url_image


async def test_generate_premium_report_html(tmp_path: Path) -> None:
    """Test HTML generation from simple DB models."""
    vehicle, vehicle_model, battery, oem, vehicle_data, url_image = (
        _create_test_models()
    )
    generator = PremiumReportGenerator()

    html_content = await generator.generate_premium_report_html(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        report_uuid=str(uuid.uuid4()),
        image_url=url_image,
    )

    output_path = tmp_path / "example_premium_report.html"
    output_path.write_text(html_content, encoding="utf-8")

    assert html_content
    assert vehicle.vin and vehicle.vin in html_content
    assert "<title>Rapport Premium" in html_content
    assert "Tesla Model Y Standard" in html_content
    assert "SAMSUNG SDI" in html_content

    # Verify autonomy data is present (dynamic from VehicleModel)
    assert "380 km" in html_content  # autonomy_city_summer
    assert "320 km" in html_content  # autonomy_city_winter
    assert "290 km" in html_content  # autonomy_highway_summer
    assert "240 km" in html_content  # autonomy_highway_winter
    assert "335 km" in html_content  # autonomy_combined_summer
    assert "280 km" in html_content  # autonomy_combined_winter

    # Verify warranty data is calculated and present
    assert "140 000" in html_content  # Remaining km (160000 - 20000)
    assert "6 mois" in html_content  # 8 - 7.5 years

    # Verify warranty info text uses dynamic values
    assert "8 ans" in html_content  # warranty_years
    assert "160" in html_content  # warranty_km (160,000)


@pytest.mark.gotenberg
@pytest.mark.integration
async def test_generate_premium_report_pdf_gotenberg() -> None:
    """Test PDF generation with Gotenberg (without S3 upload)."""
    generator = PremiumReportGenerator(gotenberg_url=settings.GOTENBERG_URL)
    output_path = Path("/tmp") / "bib_reports" / "example_premium_report.pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    vehicle, vehicle_model, battery, oem, vehicle_data, url_image = (
        _create_test_models()
    )

    html_content = await generator.generate_premium_report_html(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        report_uuid=str(uuid.uuid4()),
        image_url=url_image,
    )
    html_path = output_path.parent / "example_premium_report.html"
    html_path.write_text(html_content, encoding="utf-8")

    pdf_bytes = await generator.generate_pdf(
        html_content=html_content, output_path=output_path
    )

    assert output_path.exists()
    assert output_path.stat().st_size == len(pdf_bytes)
    assert pdf_bytes.startswith(b"%PDF")

    print(f"Saved PDF to {output_path}")
    print(f"Saved HTML to {html_path}")


async def test_generate_premium_report_html_with_fallback_autonomy() -> None:
    """Test HTML generation when vehicle_model lacks autonomy data but vehicle_data has it."""
    vehicle, vehicle_model, battery, oem, vehicle_data, url_image = (
        _create_test_models()
    )
    generator = PremiumReportGenerator()

    # Remove autonomy values from vehicle_model to test fallback
    vehicle_model.autonomy_city_summer = None
    vehicle_model.autonomy_city_winter = None
    vehicle_model.autonomy_highway_summer = None
    vehicle_model.autonomy_highway_winter = None
    vehicle_model.autonomy_combined_summer = None
    vehicle_model.autonomy_combined_winter = None

    # Set real_autonomy in vehicle_data (308 km WLTP)
    vehicle_data.real_autonomy = 308

    html_content = await generator.generate_premium_report_html(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        image_url=url_image,
    )

    assert html_content
    assert vehicle.vin and vehicle.vin in html_content

    # Verify computed autonomy data is present (computed from base_autonomy=308)
    # Urban summer: 308 / 1 = 308 km
    assert "308 km" in html_content
    # Urban winter: 308 / 1.5 = 205 km
    assert "205 km" in html_content
    # Motorway summer: 308 / 1.55 = 198 km
    assert "198 km" in html_content
    # Motorway winter: 308 / 2 = 154 km
    assert "154 km" in html_content
    # Mixed summer: 308 / 1.25 = 246 km
    assert "246 km" in html_content
    # Mixed winter: 308 / 1.75 = 176 km
    assert "176 km" in html_content
