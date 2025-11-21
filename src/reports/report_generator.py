import asyncio
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright
from sqlalchemy.orm import sessionmaker

from core.env_utils import get_env_var
from core.gsheet_utils import get_google_client
from core.s3.async_s3 import AsyncS3
from core.sql_utils import get_sqlalchemy_engine
from db_models.vehicle import Vehicle, VehicleData, VehicleModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PREMIUM_REPORT_MAIL = get_env_var("PREMIUM_REPORT_MAIL")
PREMIUM_REPORT_PWD = get_env_var("PREMIUM_REPORT_PWD")


class ReportGenerator:
    """Enriches Google Sheet data with vehicle information and generates PDF reports."""

    def __init__(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        s3_bucket: str = "bib-premium-reports",
    ):
        """
        Initialize the ReportEnricher.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            worksheet_name: Name of the worksheet to process
            s3_bucket: S3 bucket name for storing PDFs
            login_email: Email for login (optional, can be set later)
            login_password: Password for login (optional, can be set later)
        """
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.s3_bucket = s3_bucket
        self.login_email = PREMIUM_REPORT_MAIL
        self.login_password = PREMIUM_REPORT_PWD
        self._gsheet_client = get_google_client()

        self._worksheet = None
        self._s3_client = None
        self._engine = None
        self._session_factory = None

    @property
    def worksheet(self):
        if self._worksheet is None:
            spreadsheet = self._gsheet_client.open_by_key(self.spreadsheet_id)
            self._worksheet = spreadsheet.worksheet(self.worksheet_name)
        return self._worksheet

    @property
    def s3_client(self) -> AsyncS3:
        if self._s3_client is None:
            self._s3_client = AsyncS3(bucket=self.s3_bucket)
        return self._s3_client

    @property
    def session_factory(self):
        if self._session_factory is None:
            self._engine = get_sqlalchemy_engine()
            self._session_factory = sessionmaker(bind=self._engine)
        return self._session_factory

    def load_gsheet_data(self) -> pd.DataFrame:
        """
        Load data from client GSheet.
        """
        logger.info(f"Loading data from Google Sheet: {self.worksheet_name}")
        existing_data = self.worksheet.get_all_records()
        df = pd.DataFrame(existing_data)
        return df

    def enrich_with_vehicle_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with vehicle model data from database.
        """
        logger.info(f"Enriching {len(df)} vehicles with database data")

        with self.session_factory() as session:
            # Vin and ability to compute SoH
            results = (
                session.query(
                    Vehicle.vin,
                    VehicleModel.soh_data,
                    VehicleModel.soh_oem_data,
                )
                .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
                .filter(Vehicle.activation_status)
                .filter(Vehicle.vin.in_(df["VIN"].astype(str)))
                .all()
            )

            # Vin and actually available SOH
            sohs = (
                session.query(Vehicle.vin)
                .join(VehicleData, Vehicle.id == VehicleData.vehicle_id)
                .filter(Vehicle.vin.in_(df["VIN"].astype(str)))
                .filter(VehicleData.soh.isnot(None))
                .all()
            )

        query_df = pd.DataFrame(results, columns=["VIN", "SOH_DATA", "SOH_OEM"])
        query_df["SOH"] = query_df["SOH_DATA"] | query_df["SOH_OEM"]
        merged_df = df.merge(query_df, on="VIN", how="left")

        # Is vehicle active
        merged_df["ACTIVE"] = ~merged_df["SOH"].isna()

        # Is SOH theorically computable for the model
        merged_df["SOH_COMPUTABLE"] = ~merged_df["SOH"].isna() & merged_df["SOH"]

        # Is SOH of the VIN available
        soh_vins = [vin for (vin,) in sohs]
        merged_df["AVAILABLE_SOH"] = merged_df["VIN"].isin(soh_vins)

        # Drop intermediate columns
        merged_df = merged_df.drop(columns=["SOH_DATA", "SOH_OEM", "SOH"])

        return merged_df

    async def download_pdfs_for_vins(self, vins: list[str]) -> list[tuple[str, str]]:
        """
        Download PDF reports for given VINs and upload to S3.
        """

        if not vins:
            logger.info("No VINs to download PDFs for")
            return []

        logger.info(f"Downloading PDFs for {len(vins)} VINs")

        list_files = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Login
            await page.goto("https://get-evalue.com/auth/login")
            await page.fill("#email", self.login_email)
            await page.fill("#password", self.login_password)
            await page.click("text=Login")
            await page.wait_for_load_state("networkidle")

            for vin in vins:
                try:
                    await page.goto(f"https://get-evalue.com/dashboard/passport/{vin}")
                    await page.wait_for_selector(
                        "text=Download Report", state="visible"
                    )
                    await asyncio.sleep(0.5)

                    async with page.expect_download() as dl_info:
                        await page.click("text=Download Report")
                    download = await dl_info.value

                    temp_path = Path(await download.path())
                    pdf_bytes = temp_path.read_bytes()

                    if pdf_bytes[:4] != b"%PDF":
                        logger.error(f"Downloaded file for {vin} is not a PDF")
                        continue
                except Exception as e:
                    logger.error(f"Error downloading PDF for {vin}: {e}")
                    continue

                # Upload to S3
                s3_path = f"{self.worksheet_name.upper()}/{vin}_{datetime.now().strftime('%Y%m%d')}.pdf"
                await self.s3_client.upload_file(s3_path, pdf_bytes)

                list_files.append((vin, s3_path))

            await browser.close()

        logger.info(f"Downloaded {len(list_files)} PDFs successfully")
        return list_files

    async def add_presigned_urls(
        self, df: pd.DataFrame, list_files: list[tuple[str, str]]
    ) -> pd.DataFrame:
        """
        Add presigned S3 URLs to DataFrame.
        """

        logger.info(f"Generating presigned URLs for {len(list_files)} files")

        for vin, s3_path in list_files:
            url = await self.s3_client.get_presigned_url(s3_path)
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
        cell_range = f"D2:D{end_row}"

        worksheet.update(cell_range, formulas, raw=False)

    def update_gsheet(self, df: pd.DataFrame) -> None:
        """
        Update Google Sheet with enriched data.
        """

        logger.info(f"Updating Google Sheet with {len(df)} rows")

        # Clear data below header
        self.worksheet.batch_clear(["A2:ZZ10000"])
        # Append new data
        self.worksheet.append_rows(df.values.tolist())

        self.apply_report_links(self.worksheet, df)

        logger.info("Google Sheet updated successfully")

    async def run(self) -> pd.DataFrame:
        logger.info("Starting report enrichment process")

        # Load data
        df = self.load_gsheet_data()

        # Enrich with database data
        merged_df = self.enrich_with_vehicle_data(df)

        # Download PDFs if requested
        vins_to_download = merged_df[merged_df["AVAILABLE_SOH"]]["VIN"].tolist()

        list_files = await self.download_pdfs_for_vins(vins_to_download)

        # Add presigned URLs
        merged_df = await self.add_presigned_urls(merged_df, list_files)

        merged_df = merged_df.drop(columns=["AVAILABLE_SOH"])

        self.update_gsheet(merged_df)

        logger.info("Report enrichment process completed")
        return merged_df
