"""Premium report generator using Figma template and Gotenberg."""

import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader

from core.numpy_utils import numpy_safe_eval
from db_models.company import Oem
from db_models.report import ReportType
from db_models.vehicle import Battery, Vehicle, VehicleData, VehicleModel
from external_api.schemas.report import (
    AutonomyInfo,
    BatteryReportInfo,
    ChargingInfo,
    ReportData,
    ReportMetadata,
    SohChartData,
    VehicleReportInfo,
    WarrantyInfo,
)
from reports.exceptions import MissingBIBSoH, MissingOEMSoH
from reports.report_config import GOTENBERG_URL
from reports.report_render.asset_utils import AssetEmbedder
from reports.report_render.gotenberg_client import GotenbergClient
from reports.reports_utils import generate_report_qr_code_data_url

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates PDF reports from templates."""

    def __init__(
        self,
        gotenberg_url: str = GOTENBERG_URL,
    ):
        """
        Initialize the premium report generator.

        Args:
            template_dir: Directory containing Jinja2 templates
            assets_dir: Directory containing SVG and PNG assets
            gotenberg_url: URL of the Gotenberg service
        """
        # Set default paths relative to this file
        template_dir = Path(__file__).parent / "templates"
        assets_dir = Path(__file__).parent / "assets"

        self.template_dir = Path(template_dir)
        self.assets_dir = Path(assets_dir)

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=True,
        )

        self.jinja_env.filters["number_format"] = self._number_format
        self.jinja_env.filters["default_value"] = self._default_value
        self.jinja_env.globals["read_svg"] = self._read_svg

        # Initialize components
        self.asset_embedder = AssetEmbedder(self.assets_dir)
        self.gotenberg_client = GotenbergClient(base_url=gotenberg_url)

        # Consumption coefficients for calculating ranges from base autonomy
        self.URBAN_SUMMER_COEF = 1
        self.URBAN_WINTER_COEF = 1.5
        self.MOTORWAY_SUMMER_COEF = 1.55
        self.MOTORWAY_WINTER_COEF = 2
        self.MIXED_SUMMER_COEF = 1.25
        self.MIXED_WINTER_COEF = 1.75

    @staticmethod
    def _format_date_french(dt: datetime) -> str:
        """Format a date in French format (dd/mm/YYYY)."""
        return dt.strftime("%d/%m/%Y")

    def _prepare_autonomy_data(
        self, vehicle_model: VehicleModel, vehicle_data: VehicleData
    ) -> list[AutonomyInfo]:
        """
        Prepare autonomy data from VehicleModel or calculated from VehicleData for template rendering.

        For each range value, uses vehicle_model values if available. If not available and
        vehicle_data.real_autonomy exists, calculates using consumption coefficients. Otherwise uses "N/A".

        Returns a list of AutonomyInfo objects with cycle type and summer/winter autonomy values.
        """
        base_autonomy = vehicle_data.real_autonomy or vehicle_model.autonomy

        # Helper function to get value with fallback to computed value
        def get_autonomy_value(model_value: int | None, coef: float) -> str:
            if model_value:
                return f"{model_value} km"
            elif base_autonomy is not None:
                return f"{int(base_autonomy / coef)} km"
            else:
                return "N/A"

        return [
            AutonomyInfo(
                cycle="Urbain",
                summer=get_autonomy_value(
                    vehicle_model.autonomy_city_summer, self.URBAN_SUMMER_COEF
                ),
                winter=get_autonomy_value(
                    vehicle_model.autonomy_city_winter, self.URBAN_WINTER_COEF
                ),
            ),
            AutonomyInfo(
                cycle="Autoroute",
                summer=get_autonomy_value(
                    vehicle_model.autonomy_highway_summer, self.MOTORWAY_SUMMER_COEF
                ),
                winter=get_autonomy_value(
                    vehicle_model.autonomy_highway_winter, self.MOTORWAY_WINTER_COEF
                ),
            ),
            AutonomyInfo(
                cycle="Mixte",
                summer=get_autonomy_value(
                    vehicle_model.autonomy_combined_summer, self.MIXED_SUMMER_COEF
                ),
                winter=get_autonomy_value(
                    vehicle_model.autonomy_combined_winter, self.MIXED_WINTER_COEF
                ),
            ),
        ]

    @staticmethod
    def _calculate_warranty_remaining(
        vehicle: Vehicle,
        vehicle_model: VehicleModel,
        current_km: int,
    ) -> WarrantyInfo:
        """
        Calculate remaining warranty based on mileage and time.

        Returns WarrantyInfo with remaining km, remaining time, and progress percentages.
        """
        warranty_km = int(vehicle_model.warranty_km or 160000)
        warranty_years = int(vehicle_model.warranty_date or 8)

        # Calculate remaining mileage
        remaining_km = int(max(0, warranty_km - current_km))
        km_percentage = (
            min(100, (current_km / warranty_km) * 100) if warranty_km > 0 else 0
        )

        # Calculate remaining time
        remaining_years = 0
        remaining_months = 0
        time_percentage = None
        time_str = None  # Default when start_date is missing

        if vehicle.start_date:
            # Convert Date to datetime for calculations
            start_dt = datetime.combine(vehicle.start_date, datetime.min.time())
            # Use relativedelta for accurate date arithmetic (handles leap years and edge cases)
            warranty_end_date = start_dt + relativedelta(years=warranty_years)
            current_date = datetime.now()

            if current_date < warranty_end_date:
                # Calculate actual remaining time using relativedelta
                remaining_delta = relativedelta(warranty_end_date, current_date)
                remaining_years = remaining_delta.years
                remaining_months = remaining_delta.months

                # Use actual days for percentage calculation
                total_days = (warranty_end_date - start_dt).days
                elapsed_days = (current_date - start_dt).days
                time_percentage = (
                    min(100, (elapsed_days / total_days) * 100) if total_days > 0 else 0
                )
            else:
                # Warranty has expired
                time_percentage = 100

            # Format remaining time string (only when we have a start_date)
            if remaining_years > 0 and remaining_months > 0:
                time_str = f"{remaining_years} ans et {remaining_months} mois"
            elif remaining_years > 0:
                time_str = f"{remaining_years} ans"
            elif remaining_months > 0:
                time_str = f"{remaining_months} mois"
            elif time_percentage and time_percentage >= 100:
                time_str = "Expiré"
            else:
                # Edge case: warranty active but less than 1 month remaining
                time_str = "Moins d'un mois"

        return WarrantyInfo(
            remaining_km=remaining_km,
            km_percentage=100 - km_percentage,  # Inverted for progress bar (remaining)
            remaining_time=time_str,
            time_percentage=(
                100 - time_percentage
            )  # Inverted for progress bar (remaining)
            if time_percentage is not None
            else None,
            warranty_years=warranty_years,
            warranty_km=int(warranty_km),
        )

    @staticmethod
    def _calculate_charging_data(capacity_kwh: float | None) -> list[ChargingInfo]:
        """Calculate charging durations and costs for 20% to 80% charge."""
        if not capacity_kwh:
            return []

        # Power in kW for each type
        powers = {
            "Slow": 7,
            "Medium": 22,
            "Fast": 50,
            "Fastest": 150,
        }

        # Typical outlet names
        typical_names = {
            "Slow": "Normal outlet",
            "Medium": "Wall box",
            "Fast": "Public outlet",
            "Fastest": "Supercharger",
        }

        # We charge from 10% to 80% = 70% of capacity
        energy_to_charge = float(capacity_kwh) * 0.7

        results = []
        durations = []

        for type_name in ["Slow", "Medium", "Fast", "Fastest"]:
            power = powers[type_name]

            duration_hours = energy_to_charge / power
            durations.append(duration_hours)

            # Format duration (e.g., 7h44)
            hours = int(duration_hours)
            minutes = int((duration_hours - hours) * 60)
            duration_str = f"{hours}h{minutes:02d}"

            results.append(
                ChargingInfo(
                    type=type_name,
                    duration=duration_str,
                    duration_hours=duration_hours,
                    power=power,
                    typical=typical_names[type_name],
                    percentage=0.0,  # Will be set below
                )
            )

        # Calculate percentages for the progress bars (proportional to duration)
        max_duration = max(durations) if durations else 1
        for charging_info in results:
            charging_info.percentage = (
                charging_info.duration_hours / max_duration
            ) * 100

        return results

    async def generate_report_data(
        self,
        vehicle: Vehicle,
        vehicle_model: VehicleModel,
        battery: Battery,
        oem: Oem,
        vehicle_data: VehicleData,
        image_url: str | None,
        report_type: ReportType,
        report_uuid: str | None = None,
        verify_report_base_url: str | None = None,
    ) -> ReportData:
        """
        Generate structured data for a premium report.

        This method extracts and computes all data needed for the premium report
        and returns it as a structured Pydantic model.

        Args:
            vehicle: Vehicle DB model
            vehicle_model: VehicleModel DB model
            battery: Battery DB model
            oem: Oem DB model
            vehicle_data: Latest VehicleData DB model
            image_url: URL of the vehicle image
            report_type: Report type
            report_uuid: UUID of the report. Needed if data will be used to generate a PDF.
            verify_report_base_url: Base URL of the frontend, used to generate the report verification URL (optional)

        Returns:
            PremiumReportData: Structured data for the premium report

        Raises:
            ValueError: If required data is missing
        """
        vin = vehicle.vin
        if vin is None:
            raise ValueError("VIN is required")

        logger.info(f"Generating report data for VIN: {vin}")

        if vehicle_data.soh_bib is None and vehicle_data.soh_oem is None:
            raise ValueError(f"SoH data not available for VIN: {vin}")

        # Select trendline data (vehicle-specific or OEM fallback)
        if report_type == ReportType.readout:
            # For readout, use OEM trendlines from vehicle_model or oem table
            trendline = vehicle_model.trendline_oem or oem.trendline
            trendline_min = vehicle_model.trendline_oem_min or oem.trendline_min
            trendline_max = vehicle_model.trendline_oem_max or oem.trendline_max
        else:
            # For premium, use BIB trendlines from vehicle_model
            trendline = vehicle_model.trendline_bib or oem.trendline
            trendline_min = vehicle_model.trendline_bib_min or oem.trendline_min
            trendline_max = vehicle_model.trendline_bib_max or oem.trendline_max

        if not trendline or not trendline_min or not trendline_max:
            raise ValueError(f"No trendline data available for VIN: {vin}")

        current_km = int(vehicle_data.odometer or 0)

        trendline_eq = trendline
        trendline_min_eq = trendline_min
        trendline_max_eq = trendline_max

        if not trendline_eq or not trendline_min_eq or not trendline_max_eq:
            raise ValueError(f"Invalid trendline equations for VIN: {vin}")

        soh_chart_data = self.generate_soh_data(
            current_km=current_km,
            max_km=100_000,
            trendline_eq=trendline_eq,
            trendline_min_eq=trendline_min_eq,
            trendline_max_eq=trendline_max_eq,
            bib_score=vehicle.bib_score,
            bib_soh=float(vehicle_data.soh_bib) * 100
            if vehicle_data.soh_bib is not None
            else None,
            readout_soh_value=float(vehicle_data.soh_oem) * 100
            if vehicle_data.soh_oem is not None
            else None,
            report_type=report_type,
        )

        charging_data = self._calculate_charging_data(battery.capacity)

        autonomy_data = self._prepare_autonomy_data(vehicle_model, vehicle_data)

        warranty_info = self._calculate_warranty_remaining(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            current_km=current_km,
        )

        vehicle_info = VehicleReportInfo(
            vin=vin,
            oem=oem.oem_name,
            model=vehicle_model.model_name,
            registration=vehicle.licence_plate,
            mileage=str(int(current_km)),
            type=vehicle_model.type,
            charging_port=vehicle_model.charge_plug_type,
            fast_charging_port=vehicle_model.fast_charge_plug_type,
            image_url=image_url,
        )

        battery_info = BatteryReportInfo(
            manufacturer=battery.battery_oem,
            chemistry=battery.battery_chemistry,
            capacity=battery.capacity,
            net_capacity=battery.net_capacity,
            wltp_range=str(vehicle_model.autonomy),
            consumption=int(vehicle_data.consumption)
            if vehicle_data.consumption
            else vehicle_model.expected_consumption,
            warranty_years=warranty_info.warranty_years,
            warranty_km=warranty_info.warranty_km,
        )

        report_url = (
            f"{verify_report_base_url.rstrip('/')}/{report_uuid}"
            if report_uuid and verify_report_base_url
            else None
        )
        report_date = self._format_date_french(datetime.now())
        report_metadata = ReportMetadata(
            date=report_date,
            delivered_by="BIB Batteries",
            uuid=report_uuid,
            report_type=report_type,
            qr_code_url=generate_report_qr_code_data_url(report_url)
            if report_url
            else None,
            report_verification_url=report_url,
        )

        return ReportData(
            vehicle=vehicle_info,
            battery=battery_info,
            soh_data=soh_chart_data,
            charging_data=charging_data,
            autonomy_data=autonomy_data,
            warranty_data=warranty_info,
            report=report_metadata,
        )

    async def generate_report_html(
        self,
        vehicle: Vehicle,
        vehicle_model: VehicleModel,
        battery: Battery,
        oem: Oem,
        vehicle_data: VehicleData,
        image_url: str | None,
        report_type: ReportType,
        report_uuid: str | None = None,
        verify_report_base_url: str | None = None,
    ) -> str:
        """
        Generate the HTML content for a premium report.

        Args:
            vehicle: Vehicle DB model
            vehicle_model: VehicleModel DB model
            battery: Battery DB model
            oem: Oem DB model
            vehicle_data: Latest VehicleData DB model
            image_url: URL of the vehicle image
            report_type: Report type
            report_uuid: UUID of the report. Needed if data will be used to generate a PDF.
            verify_report_base_url: Base URL of the frontend, used to generate the report verification URL (optional)

        Returns:
            Rendered HTML content as string

        Raises:
            ValueError: If required data is missing
        """
        logger.info(f"Generating HTML for premium report, VIN: {vehicle.vin}")

        report_data = await self.generate_report_data(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            report_uuid=report_uuid,
            image_url=image_url,
            report_type=report_type,
            verify_report_base_url=verify_report_base_url,
        )

        # Use the main template which includes both pages with proper page breaks
        html_content = self.render_template(
            data=report_data,
            embed_assets=True,
        )

        return html_content

    @staticmethod
    def _number_format(value: Any, separator: str = " ") -> str:
        """
        Format a number with thousands separator.

        Args:
            value: Number to format
            separator: Thousands separator (default: space)

        Returns:
            Formatted string
        """
        try:
            num = float(value)
            # Format with thousands separator
            return f"{num:,.0f}".replace(",", separator)
        except (ValueError, TypeError):
            return str(value)

    @staticmethod
    def _default_value(value: Any, default: str = "-") -> str:
        """
        Return a default value if the input is None or empty.

        Args:
            value: Value to check
            default: Default value to return (default: "-")

        Returns:
            Original value if not None/empty, otherwise the default
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return default
        return str(value)

    def _read_svg(
        self, relative_path: str, css_classes: list[str] | None = None
    ) -> str:
        """
        Read an SVG file and return its content.

        Args:
            relative_path: Path relative to assets folder
            css_classes: List of CSS classes to add to the SVG
        Returns:
            SVG content as string
        """
        svg_path = self.assets_dir / relative_path
        if svg_path.exists() and svg_path.suffix.lower() == ".svg":
            svg_content = svg_path.read_text(encoding="utf-8")
            if css_classes:
                return f'<span class="{" ".join(css_classes)}">{svg_content}</span>'
            return svg_content
        raise FileNotFoundError(f"SVG file not found: {svg_path}")

    def render_template(
        self,
        data: ReportData,
        embed_assets: bool = True,
    ) -> str:
        """
        Render a Jinja2 template with the provided data.

        Args:
            template_name: Name of the template file
            data: Dictionary or Pydantic model of data to pass to the template
            embed_assets: Whether to automatically embed all assets as data URLs

        Returns:
            Rendered HTML as string
        """
        template = self.jinja_env.get_template(name="premium_report.html")

        context = data.model_dump(mode="python")

        if embed_assets:
            context["assets"] = self.asset_embedder.get_all_assets_as_data_urls()

        html_content = template.render(**context)
        return html_content

    async def generate_pdf(
        self,
        html_content: str,
        output_path: Path | None = None,
    ) -> bytes:
        """
        Generate a PDF from pre-rendered HTML content.

        Args:
            html_content: Rendered HTML string with embedded images
            output_path: Optional path to save the PDF

        Returns:
            PDF content as bytes
        """
        # Generate PDF
        pdf_bytes = await self.gotenberg_client.generate_pdf_from_html(
            html_content,
            wait_delay="0s",
            paper_width=8.27,  # standard A4
            paper_height=11.69,  # standard A4
            margin_top=0,
            margin_bottom=0,
            margin_left=0,
            margin_right=0,
            print_background=True,
            prefer_css_page_size=True,
            scale=1,
        )

        # Save to file if requested
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(pdf_bytes)
            logger.info(f"PDF saved to {output_path}")

        return pdf_bytes

    def generate_soh_data(
        self,
        current_km: int,
        max_km: int,
        trendline_eq: str,
        trendline_min_eq: str,
        trendline_max_eq: str,
        bib_soh: float | None,
        bib_score: str | None,
        readout_soh_value: float | None,
        report_type: ReportType,
    ) -> SohChartData:
        """
        Generate complete SoH data including current value, grade, and chart data.

        Uses offset-adjusted trendlines to anchor predictions to the actual current SoH value,
        matching the frontend implementation.

        Args:
            current_km: Current vehicle mileage (km)
            max_km: Maximum mileage to predict (km)
            trendline_eq: Equation string for main trendline
            trendline_min_eq: Equation string for lower bound
            trendline_max_eq: Equation string for upper bound
            bib_score: BIB battery health grade
            bib_soh: Current BIB SoH percentage (0-100), used to offset the trendline
            readout_soh_value: Current Readout SoH percentage (0-100)
            report_type: Report type

        Returns:
            SohChartData with offset-adjusted trendline values and current SoH
        """
        max_km_candidate = max(max_km, math.ceil(current_km * 1.1))
        # Round up to nearest multiple of 9000 to ensure nice round numbers for the x-axis
        max_km = int(math.ceil(max_km_candidate / 9000.0) * 9000)

        # Generate exactly 10 points to keep the chart clean
        km_points = np.linspace(0, max_km, 15, dtype=int)

        soh_trendline = np.round(numpy_safe_eval(trendline_eq, x=km_points) * 100, 1)
        soh_min = np.round(numpy_safe_eval(trendline_min_eq, x=km_points) * 100, 1)
        soh_max = np.round(numpy_safe_eval(trendline_max_eq, x=km_points) * 100, 1)

        # Calculate offset to anchor trendline to actual current SoH (matches frontend approach)
        trendline_value_at_current = float(
            np.round(numpy_safe_eval(trendline_eq, x=current_km) * 100, 1)
        )
        if report_type == ReportType.readout:
            if readout_soh_value is not None:
                offset = round(readout_soh_value - trendline_value_at_current, 1)
            else:
                raise MissingOEMSoH(f"SoH value missing for report_type={report_type}")
        elif report_type == ReportType.premium:
            if bib_soh is not None:
                offset = round(bib_soh - trendline_value_at_current, 1)
            else:
                raise MissingBIBSoH(f"SoH value missing for report_type={report_type}")

        # TODO: Remove the correction once EVALUE-250 is fixed
        soh_trendline = soh_trendline + offset

        return SohChartData(
            soh_bib=bib_soh,
            soh_readout=readout_soh_value,
            readout_only=report_type == ReportType.readout,
            grade=bib_score,
            odometer_points=km_points.tolist(),
            current_km=int(current_km),
            soh_trendline=soh_trendline.tolist(),
            soh_min=soh_min.tolist(),
            soh_max=soh_max.tolist(),
        )
