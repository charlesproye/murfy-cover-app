"""
Generate OEM field filter files from Google Sheets.

Reads a Google Sheet with one sheet per OEM, where each sheet has:
- DATA POINTS: field name
- TO_KEEP: TRUE/FALSE indicating whether to keep the field

Creates one text file per OEM in src/ingestion/data/ with one line per field to keep.
"""

import logging
from pathlib import Path

import click
import gspread

from core.gsheet_utils import get_google_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_worksheet(worksheet: gspread.Worksheet, output_dir: Path) -> None:
    """
    Process a single worksheet and create a text file with fields to keep.

    Args:
        worksheet: gspread worksheet for a single OEM
        output_dir: directory to write the output file
    """
    oem_name = worksheet.title.lower().replace(" ", "_")

    if oem_name == "docs":
        logger.info(f"Skipping worksheet: {worksheet.title}")
        return

    logger.info(f"Processing worksheet: {worksheet.title}")

    # Get all values as list of dicts
    records = worksheet.get_all_records()

    # Filter rows where TO_KEEP is TRUE
    fields_to_keep = []

    no_types_fields = []
    for record in records:
        data_point = str(record.get("DATA POINTS", "")).strip()
        to_keep = str(record.get("TO_KEEP", "")).strip().upper()

        if to_keep == "TRUE" and data_point:
            if record.get("TYPE") is None:
                no_types_fields.append(data_point)
            data_type = str(record.get("TYPE", "string")).strip().lower()
            fields_to_keep.append(f"{data_point},{data_type}")

    if no_types_fields:
        logger.warning(
            f"[{oem_name}] No types found for the following fields: {no_types_fields}"
        )

    # Write to file
    output_file = output_dir / f"{oem_name}.csv"
    with open(output_file, "w") as f:
        f.write("field_name,field_type\n")
        f.write("\n".join(fields_to_keep))
        if fields_to_keep:
            f.write("\n")  # Trailing newline

    logger.info(f"Wrote {len(fields_to_keep)} fields to {output_file}")


@click.command()
@click.argument("sheet_id", default="1J25R4M4tU40zOF2CUrylpvVSkjmJvHshWlX0fh03PpE")
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory (defaults to src/ingestion/data/)",
)
def generate_field_files(sheet_id: str, output_dir: Path | None) -> None:
    """
    Generate OEM field filter files from Google Sheets.

    SHEET_ID is the ID of the Google Sheet to read (from the URL).
    """
    # Determine output directory
    if output_dir is None:
        repo_root = Path(__file__).parent.parent.parent.parent
        output_dir = repo_root / "src" / "ingestion" / "data"

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Get the Google Sheet
    client = get_google_client()
    sheet = client.open_by_key(sheet_id)
    logger.info(f"Opened sheet: {sheet.title}")

    # Process each worksheet (each represents an OEM)
    for worksheet in sheet.worksheets():
        try:
            process_worksheet(worksheet, output_dir)
        except Exception as e:
            logger.error(f"Error processing worksheet {worksheet.title}: {e}")


if __name__ == "__main__":
    generate_field_files()
