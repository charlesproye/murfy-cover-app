import asyncio

from core.env_utils import get_env_var
from core.gdrive_utils import get_google_service, list_spreadsheets_in_folder
from reports.gsheet_report_generator import GSheetReportGenerator

CLIENT_FOLDER_ID = get_env_var("CLIENT_FOLDER_ID")


async def main():
    drive_service = get_google_service(service_type="drive", version="v3")
    spreadsheets = list_spreadsheets_in_folder(drive_service, CLIENT_FOLDER_ID)

    for spreadsheet in spreadsheets:
        spreadsheet_id = spreadsheet["id"]
        spreadsheet_name = spreadsheet["name"]

        # if spreadsheet_name != "TestClient":
        #     continue

        report_generator = GSheetReportGenerator(spreadsheet_id, spreadsheet_name)
        try:
            await report_generator.run()
        finally:
            await report_generator.close()


if __name__ == "__main__":
    asyncio.run(main())
