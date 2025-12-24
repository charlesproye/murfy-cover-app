import os

import dotenv
import msgspec
import pytest

from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.schema import CarState

# Mark all tests in this module as integration tests (require real Mobilisights API)
pytestmark = pytest.mark.integration


@pytest.fixture
def api():
    dotenv.load_dotenv()
    MS_BASE_URL = os.getenv("MS_BASE_URL")
    MS_EMAIL = os.getenv("MS_EMAIL")
    MS_PASSWORD = os.getenv("MS_PASSWORD")
    MS_COMPANY = os.getenv("MS_COMPANY")

    assert MS_BASE_URL is not None, "MS_BASE_URL must be set in environment"
    assert MS_EMAIL is not None, "MS_EMAIL must be set in environment"
    assert MS_PASSWORD is not None, "MS_PASSWORD must be set in environment"
    assert MS_COMPANY is not None, "MS_COMPANY must be set in environment"

    ms_api = MSApi(
        base_url=MS_BASE_URL,
        email=MS_EMAIL,
        password=MS_PASSWORD,
        company=MS_COMPANY,
    )

    return ms_api


def test_export_car_info(api: MSApi):
    code, res = api.export_car_info()
    assert code == 200
    assert res is not None

    next(res)
    car_state = msgspec.json.decode(next(res), type=CarState)

    assert car_state.vin is not None
