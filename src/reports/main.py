import asyncio

from core.env_utils import get_env_var
from core.gdrive_utils import get_drive_service, list_spreadsheets_in_folder
from reports.report_generator import ReportGenerator

CLIENT_FOLDER_ID = get_env_var("CLIENT_FOLDER_ID")


async def main():
    drive_service = get_drive_service()
    spreadsheets = list_spreadsheets_in_folder(drive_service, CLIENT_FOLDER_ID)

    for spreadsheet in spreadsheets:
        spreadsheet_id = spreadsheet["id"]
        spreadsheet_name = spreadsheet["name"]

        report_generator = ReportGenerator(spreadsheet_id, spreadsheet_name)
        await report_generator.run()


if __name__ == "__main__":
    asyncio.run(main())
