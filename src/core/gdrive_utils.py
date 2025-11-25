import base64
import json
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


def get_google_service(service_type: str = "drive", version: str = "v3"):
    """Get authenticated Google Drive service."""

    base64_creds = os.getenv("GOOGLE_PRIVATE_KEY")
    creds_dict = json.loads(base64.b64decode(base64_creds))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    get_google_service = build(service_type, version, credentials=credentials)

    return get_google_service


def list_spreadsheets_in_folder(
    drive_service: build, folder_id: str
) -> list[dict[str, str]]:
    query = (
        f"'{folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.spreadsheet'"
    )

    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    return files
