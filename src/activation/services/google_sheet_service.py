import math
from datetime import datetime

import pandas as pd

from activation.config.credentials import SPREADSHEET_ID
from core.gsheet_utils import get_google_client


def sanitize_value(val):
    """Ensure value is JSON compliant (no NaN, inf)."""
    if val is None:
        return ""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return ""
    return val


async def update_vehicle_activation_data(df: pd.DataFrame) -> bool:
    """Update or insert vehicle activation data in Google Sheet.

    Args:
        df (pd.DataFrame): DataFrame containing vehicle data with columns:
            - vin: Vehicle identification number
            - Eligibility: Whether the vehicle is eligible for activation
            - Real_Activation: Current activation status
            - Activation_Error: Any error messages
            - Oem: (Optional) Vehicle manufacturer
            - Make: (Optional) Vehicle make/brand

    Returns:
        bool: True if all updates were successful, False otherwise
    """

    client = get_google_client()
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1

    # Get all existing data at once
    existing_data = sheet.get_all_records()
    existing_df = pd.DataFrame(existing_data)

    # Get headers
    headers = sheet.row_values(1)
    vin_col = headers.index("vin") + 1
    activation_col = headers.index("Activation") + 1
    real_activation_col = headers.index("Real Activation") + 1
    eligibility_col = headers.index("Eligibility") + 1
    error_col = headers.index("Activation Error") + 1
    api_detail = headers.index("API Detail") + 1
    oem_col = headers.index("Oem") + 1
    make_col = headers.index("Make") + 1
    fleet_col = headers.index("Fleet") + 1
    company_col = headers.index("Company") + 1
    # owner_col = headers.index("Owner") + 1
    country_col = headers.index("Country") + 1
    account_owner_col = headers.index("account_owner_tesla") + 1
    updates = []
    inserts = []

    # Create a set of existing VINs for faster lookup
    existing_vins = set(existing_df["vin"].astype(str))

    for _, row in df.iterrows():
        vin = str(row["vin"])

        # Check if account_owner_tesla exists in the DataFrame columns
        account_owner_value = (
            row.get("account_owner_tesla", "")
            if "account_owner_tesla" in df.columns
            else ""
        )

        if vin in existing_vins:
            # Find the row number in the existing data
            row_idx = (
                existing_df[existing_df["vin"] == vin].index[0] + 2
            )  # +2 because of 0-based index and header row

            # Update the activation status according to the activation/deactivation date
            deactivation_date_raw = existing_df.loc[
                existing_df["vin"] == vin, "Deactivation date"
            ].values[0]
            activation_date_raw = existing_df.loc[
                existing_df["vin"] == vin, "Activation date"
            ].values[0]

            # Parse dates with error handling for empty/null values
            deactivation_date_ts = pd.to_datetime(
                deactivation_date_raw, errors="coerce"
            )
            activation_date_ts = pd.to_datetime(activation_date_raw, errors="coerce")

            # Convert to date only if valid (not NaT)
            deactivation_date = (
                deactivation_date_ts.date() if pd.notna(deactivation_date_ts) else None
            )
            activation_date = (
                activation_date_ts.date() if pd.notna(activation_date_ts) else None
            )

            today = datetime.now().date()

            if activation_date is not None and activation_date <= today:
                updates.append(
                    {
                        "range": f"R{row_idx}C{activation_col}",
                        "values": [[True]],
                    }
                )

            if deactivation_date is not None and deactivation_date <= today:
                updates.append(
                    {
                        "range": f"R{row_idx}C{activation_col}",
                        "values": [[False]],
                    }
                )

            # Update existing row
            updates.append(
                {
                    "range": f"R{row_idx}C{real_activation_col}",
                    "values": [[sanitize_value(row["Real_Activation"])]],
                }
            )
            updates.append(
                {
                    "range": f"R{row_idx}C{eligibility_col}",
                    "values": [[sanitize_value(row["Eligibility"])]],
                }
            )
            updates.append(
                {
                    "range": f"R{row_idx}C{error_col}",
                    "values": [[sanitize_value(row["Activation_Error"])]],
                }
            )
            updates.append(
                {
                    "range": f"R{row_idx}C{api_detail}",
                    "values": [[sanitize_value(row["API_Detail"])]],
                }
            )
            updates.append(
                {
                    "range": f"R{row_idx}C{account_owner_col}",
                    "values": [[account_owner_value]],
                }
            )

        else:
            new_row = [""] * len(
                headers
            )  # Create empty row with same length as headers
            new_row[vin_col - 1] = vin
            new_row[activation_col - 1] = False
            new_row[real_activation_col - 1] = False
            new_row[eligibility_col - 1] = True
            new_row[error_col - 1] = row["Activation_Error"]
            # new_row[api_detail - 1] = row['API_Detail']
            new_row[oem_col - 1] = "TESLA"
            new_row[make_col - 1] = "TESLA"
            new_row[company_col - 1] = "Bib"
            new_row[fleet_col - 1] = "Bib"
            # new_row[owner_col - 1] = "Bib"
            new_row[country_col - 1] = "France"
            new_row[account_owner_col - 1] = account_owner_value
            inserts.append(new_row)

    # Execute batch updates
    if updates:
        sheet.batch_update(updates)

    # Insert new rows
    if inserts:
        sheet.append_rows(inserts)

    return True
