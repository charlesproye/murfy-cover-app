import logging
import uuid
from datetime import UTC, datetime
from typing import cast

import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql import ColumnElement, select

from core.gdrive_utils import get_google_service
from core.gsheet_utils import get_google_client
from core.s3.async_s3 import S3ACL, AsyncS3
from core.sql_utils import get_async_session_maker
from db_models import Report, Vehicle, VehicleData, VehicleModel
from db_models.company import Oem
from db_models.report import ReportType
from db_models.vehicle import Battery
from external_api.core.exceptions import ExistingReportException
from reports import reports_utils
from reports.report_config import VERIFY_REPORT_BASE_URL
from reports.report_render.report_generator import ReportGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GSheetReportGenerator:
    """Enriches Google Sheet data with vehicle information and generates PDF reports."""

    def __init__(
        self,
        spreadsheet_id: str | None = None,
        worksheet_name: str = "API",
        s3_bucket: str = "bib-premium-reports",
    ):
        """
        Initialize the ReportEnricher.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            worksheet_name: Name of the worksheet to process
            s3_bucket: S3 bucket name for storing PDFs
        """
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.s3_bucket = s3_bucket
        self._gsheet_client = get_google_client()
        self._sheets_service = get_google_service(service_type="sheets", version="v4")
        self._worksheet = None
        self.s3_client = AsyncS3(bucket=self.s3_bucket)
        self._engine = None
        self._session_factory = None
        self._async_session_factory = None

    @property
    def worksheet(self):
        if self._worksheet is None:
            if self.spreadsheet_id is None:
                raise ValueError("Spreadsheet ID is required")
            spreadsheet = self._gsheet_client.open_by_key(self.spreadsheet_id)
            self._worksheet = spreadsheet.worksheet(self.worksheet_name)
        return self._worksheet

    @property
    def async_session_factory(self):
        if self._async_session_factory is None:
            self._async_session_factory = get_async_session_maker()
        return self._async_session_factory

    def load_gsheet_data(self) -> pd.DataFrame:
        """
        Load data from Google Sheet and extract actual hyperlinks in column 'LINK'.
        """
        if self.spreadsheet_id is None:
            raise ValueError("Spreadsheet ID is required")

        logger.info(f"Loading data from Google Sheet: {self.worksheet_name}")

        all_values = self.worksheet.get_all_values()
        headers = all_values[0]
        data = all_values[1:]

        df = pd.DataFrame(data, columns=headers)

        hyperlinks: list[str] = []

        if data:
            sheet_id = self.worksheet.spreadsheet.id
            sheet_name = self.worksheet.title

            range_name = f"{sheet_name}!E2:E{len(data) + 1}"
            result = (
                self._sheets_service.spreadsheets()
                .get(
                    spreadsheetId=sheet_id,
                    ranges=[range_name],
                    fields="sheets.data.rowData.values.hyperlink",
                )
                .execute()
            )

            row_data = result["sheets"][0]["data"][0].get("rowData", [])

            for idx in range(len(data)):
                row = row_data[idx] if idx < len(row_data) else {}
                values = row.get("values", [])
                if not values:
                    hyperlinks.append("")
                    continue

                cell = values[0] or {}
                val = cell.get("hyperlink") or cell.get("formattedValue") or ""
                hyperlinks.append(val)

        df["LINK"] = hyperlinks or ["" for _ in range(len(df))]

        return df

    async def enrich_with_vehicle_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with vehicle model data from database.
        """
        logger.info(f"Enriching {len(df)} vehicles with database data")
        vin_list = df["VIN"].astype(str).dropna().tolist()
        vehicle_vin_column = cast(ColumnElement[str], Vehicle.vin)

        async with self.async_session_factory() as session:
            # Vin and ability to compute SoH
            results: list[
                tuple[str, bool | None, bool | None, bool]
            ] = await session.execute(
                select(
                    Vehicle.vin,
                    VehicleModel.soh_data,
                    VehicleModel.soh_oem_data,
                    Vehicle.activation_status,
                )
                .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
                .filter(vehicle_vin_column.in_(vin_list))
            )

            # Vin and actually available SOH
            sohs = await session.execute(
                select(Vehicle.vin)
                .join(VehicleData, Vehicle.id == VehicleData.vehicle_id)
                .filter(vehicle_vin_column.in_(vin_list))
                .filter(VehicleData.soh_bib.isnot(None))
            )

        query_rows = [
            {
                "VIN": vin,
                "SOH_DATA": soh_data,
                "SOH_OEM": soh_oem,
                "ACTIVE": activation_status,
            }
            for vin, soh_data, soh_oem, activation_status in results
        ]

        query_df = pd.DataFrame(query_rows)
        query_df["SOH"] = query_df["SOH_DATA"] | query_df["SOH_OEM"]
        merged_df = df.merge(query_df, on="VIN", how="left")

        merged_df["ACTIVE"] = merged_df["ACTIVE_y"].combine_first(merged_df["SOH"])

        merged_df = merged_df.drop(columns=["ACTIVE_x", "ACTIVE_y"])

        # Is SOH theorically computable for the model
        merged_df["SOH_COMPUTABLE"] = ~merged_df["SOH"].isna() & merged_df["SOH"]

        # Is SOH of the VIN available
        soh_vins = [vin for (vin,) in sohs]
        merged_df["AVAILABLE_SOH"] = merged_df["VIN"].isin(soh_vins)

        # Drop intermediate columns
        merged_df = merged_df.drop(columns=["SOH_DATA", "SOH_OEM", "SOH"])
        merged_df["DATE_REPORT"] = (
            pd.to_datetime(merged_df["DATE_REPORT"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .fillna("")
        )

        merged_df = merged_df[
            [
                "VIN",
                "DATE_REPORT",
                "ACTIVE",
                "SOH_COMPUTABLE",
                "LINK",
                "Commentaire",
                "AVAILABLE_SOH",
            ]
        ]

        return merged_df

    async def _fetch_report_data(
        self, vin: str
    ) -> tuple[Vehicle, VehicleModel, Battery, Oem, VehicleData, str | None]:
        """
        Fetch all required data for report generation from the database.

        Args:
            vin: Vehicle VIN

        Returns:
            Tuple of (Vehicle, VehicleModel, Battery, Oem, VehicleData, image_url)

        Raises:
            ValueError: If required data is missing
        """
        async with self.async_session_factory() as session:
            existing_report = await reports_utils.get_db_report_by_date(
                vin,
                datetime.now(UTC).date(),
                session,
                report_type=ReportType.premium,
            )
            if existing_report:
                raise ExistingReportException(
                    f"Report already exists for VIN: {vin} and date: {datetime.now(UTC).date()}"
                )

            stmt = reports_utils.build_report_data_query(vin)

            result = await session.execute(stmt)
            row = result.first()

            if not row:
                raise ValueError(f"Vehicle not found for VIN: {vin}")

            # Validate required relationships exist
            if row.Battery is None:
                raise ValueError(f"Battery data not available for VIN: {vin}")

            if row.Oem is None:
                raise ValueError(f"OEM data not available for VIN: {vin}")

            if row.VehicleData is None or row.VehicleData.soh_bib is None:
                raise ValueError(
                    f"Vehicle activated but SoH is not available yet for VIN: {vin}"
                )

            image_url = reports_utils.get_image_public_url(
                row.ModelImage, row.MakeImage
            )

            return (
                row.Vehicle,
                row.VehicleModel,
                row.Battery,
                row.Oem,
                row.VehicleData,
                image_url,
            )

    async def refresh_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Refresh presigned URLs for existing PDF reports for given VINs and dates.

        Args:
            df: DataFrame containing VIN and DATE_REPORT columns

        Returns:
            DataFrame with updated LINK column containing new presigned URLs
        """
        valid_rows = df[["VIN", "DATE_REPORT"]].dropna()
        vin_date_pairs = [
            (row["VIN"], row["DATE_REPORT"]) for _, row in valid_rows.iterrows()
        ]

        logger.info(
            f"Refreshing presigned URLs for {len(vin_date_pairs)} VIN/date combinations"
        )

        # Query database for reports matching VIN/date combinations
        vin_to_report: dict[tuple[str, str], Report] = {}

        async with self.async_session_factory() as session:
            for vin, date_str in vin_date_pairs:
                try:
                    report_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except Exception as e:
                    logger.error(f"Error parsing date {date_str} for VIN {vin}: {e}")
                    continue

                report = await session.execute(
                    select(Report)
                    .join(Vehicle, Report.vehicle_id == Vehicle.id)
                    .filter(Vehicle.vin == vin)
                    .filter(func.date(Report.created_at) == report_date)
                    .filter(Report.report_type == ReportType.premium)
                    .limit(1)
                )
                report = report.scalar_one_or_none()

                if report:
                    vin_to_report[(vin, date_str)] = report
                else:
                    logger.debug(f"No report found for VIN {vin} on date {date_str}")

        for (vin, date_str), report in vin_to_report.items():
            presigned_url = await self.s3_client.get_presigned_url(
                s3_uri=report.report_url,
                expires_in=7 * 24 * 60 * 60,  # 7 days
            )
            mask = (df["VIN"] == vin) & (df["DATE_REPORT"] == date_str)
            df.loc[mask, "LINK"] = presigned_url

        logger.info(f"Successfully refreshed {len(vin_to_report)} presigned URLs")
        return df

    async def download_pdfs_for_vins(
        self, vins: list[str], report_base_url: str
    ) -> list[tuple[str, str, uuid.UUID]]:
        """
        Generate PDF reports for given VINs using PremiumReportGenerator and upload to S3.
        """

        if not vins:
            logger.info("No VINs to generate PDFs for")
            return []

        logger.info(f"Generating PDFs for {len(vins)} VINs")

        list_files: list[tuple[str, str, uuid.UUID]] = []
        generator = ReportGenerator()

        for vin in vins:
            try:
                # Fetch required data for report generation
                (
                    vehicle,
                    vehicle_model,
                    battery,
                    oem,
                    vehicle_data,
                    image_url,
                ) = await self._fetch_report_data(vin)

                report_uuid = uuid.uuid4()
                html_content = await generator.generate_report_html(
                    vehicle=vehicle,
                    vehicle_model=vehicle_model,
                    battery=battery,
                    oem=oem,
                    vehicle_data=vehicle_data,
                    image_url=image_url,
                    report_uuid=report_uuid,
                    report_type=ReportType.premium,
                    verify_report_base_url=report_base_url,
                )

                pdf_bytes = await generator.generate_pdf(html_content=html_content)

                if pdf_bytes[:4] != b"%PDF":
                    logger.error(f"Generated file for {vin} is not a PDF")
                    continue

                logger.info(
                    f"PDF generated successfully for VIN {vin}, size: {len(pdf_bytes) / 1024:.2f} KB"
                )

                s3_uri = f"s3://{self.s3_bucket}/{ReportType.premium.value}/{self.worksheet_name.upper()}/{datetime.now(UTC).strftime('%Y%m%d')}/{vin}/{vin}_{report_uuid}.pdf"
                await self.s3_client.upload_file_fast(
                    s3_uri=s3_uri, file=pdf_bytes, acl=S3ACL.PRIVATE
                )

                list_files.append((vin, s3_uri, report_uuid))
            except ExistingReportException as e:
                logger.warning(e)
                continue
            except Exception as e:
                logger.error(f"Error generating PDF for {vin}: {e}", exc_info=True)
                continue

        logger.info(f"Generated {len(list_files)} PDFs successfully")
        return list_files

    async def add_reports_to_db(
        self, list_files: list[tuple[str, str, uuid.UUID]], task_id: str | None = None
    ) -> None:
        """
        Load report S3 URIs to database.

        Stores S3 URIs (s3://bucket/path format) so presigned URLs can be generated on-demand.
        """
        async with self.async_session_factory() as session:
            for vin, s3_uri, report_uuid in list_files:
                vehicle_id = await session.execute(
                    select(Vehicle.id).where(Vehicle.vin == vin)
                )
                vehicle_id = vehicle_id.scalar_one_or_none()

                if vehicle_id is None:
                    logger.error(f"Vehicle with VIN {vin} not found in database")
                    continue

                premium_report = Report(
                    id=report_uuid,
                    vehicle_id=vehicle_id,
                    report_url=s3_uri,
                    task_id=task_id,
                    report_type=ReportType.premium,
                )
                session.add(premium_report)
            await session.commit()

    async def add_urls(
        self, df: pd.DataFrame, list_files: list[tuple[str, str, uuid.UUID]]
    ) -> pd.DataFrame:
        """
        Add presigned S3 URLs to DataFrame.
        """

        logger.info(f"Generating presigned URLs for {len(list_files)} files")

        for vin, s3_uri, _ in list_files:
            url = await self.s3_client.get_presigned_url(
                s3_uri=s3_uri,
                expires_in=7 * 24 * 60 * 60,  # 7 days
            )
            df.loc[df["VIN"] == vin, "LINK"] = url

        logger.info("Presigned URLs generated")
        return df

    def apply_report_links(self, worksheet, df):
        formulas = []
        for row_idx in range(len(df)):
            vin = df.loc[row_idx, "VIN"]

            url = df.loc[row_idx, "LINK"]

            label = f"Rapport - {vin}"

            safe_label = label.replace('"', '""')

            if not url or not url.startswith("http"):
                formula = ""
            else:
                formula = f'=HYPERLINK("{url}", "{safe_label}")'

            formulas.append([formula])

        end_row = len(df) + 1
        cell_range = f"E2:E{end_row}"

        worksheet.update(cell_range, formulas, raw=False)

    def update_gsheet(self, df: pd.DataFrame) -> None:
        """
        Update Google Sheet with enriched data.
        """

        logger.info(f"Updating Google Sheet with {len(df)} rows")

        # Clear data below header
        self.worksheet.batch_clear(["A2:ZZ10000"])

        # Replace NaN values with empty strings for JSON compatibility
        df = df.fillna("")

        # Append new data
        self.worksheet.append_rows(df.values.tolist())

        self.apply_report_links(self.worksheet, df)

        logger.info("Google Sheet updated successfully")

    async def run(self) -> pd.DataFrame:
        logger.info("Starting Premium report enrichment process")

        # Load data
        df = self.load_gsheet_data()

        # Enrich with database data
        merged_df = await self.enrich_with_vehicle_data(df)

        # Download PDFs if requested
        vins_to_download = merged_df[
            (merged_df["AVAILABLE_SOH"])
            & (merged_df["DATE_REPORT"] == datetime.now(UTC).strftime("%Y-%m-%d"))
        ]["VIN"].tolist()

        list_files = await self.download_pdfs_for_vins(
            vins_to_download, VERIFY_REPORT_BASE_URL
        )
        await self.add_reports_to_db(list_files)

        merged_df = await self.refresh_urls(merged_df)

        merged_df = merged_df.drop(columns=["AVAILABLE_SOH"])

        self.update_gsheet(merged_df)

        logger.info("Report enrichment process completed")
        return merged_df

    async def close(self) -> None:
        """
        Close all resources, including the S3 client.

        Call this method after using ReportGenerator to properly clean up resources.
        """
        await self.s3_client.close()
