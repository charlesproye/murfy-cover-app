import os

import dotenv
import fakeredis
import pytest

from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.ingester import MobilisightsIngester


@pytest.fixture
def api():
    dotenv.load_dotenv()
    MS_BASE_URL = os.getenv("MS_BASE_URL")
    MS_EMAIL = os.getenv("MS_EMAIL")
    MS_PASSWORD = os.getenv("MS_PASSWORD")
    MS_FLEET_ID = os.getenv("MS_FLEET_ID")
    MS_COMPANY = os.getenv("MS_COMPANY")

    ms_api = MSApi(
        base_url=MS_BASE_URL,
        email=MS_EMAIL,
        password=MS_PASSWORD,
        fleet_id=MS_FLEET_ID,
        company=MS_COMPANY,
    )

    return ms_api


@pytest.fixture
def fake_redis(monkeypatch):
    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("ingestion.ingestion_cache.Redis", lambda **kwargs: r)
    return r


@pytest.fixture
def ms_ingester(fake_redis):
    ingester = MobilisightsIngester()
    return ingester


def test_export_car_info(api: MSApi, ms_ingester: MobilisightsIngester, fake_redis):
    code, res = api.export_car_info()
    assert code == 200
    assert res is not None

    next(res)  # skip first [

    bytes_data = next(res)

    parsed_data = ms_ingester._parse_car_state_with_cache(bytes_data)
    assert parsed_data is not None

    parsed_data = ms_ingester._parse_car_state_with_cache(bytes_data)
    assert parsed_data is None


def test_export_car_info_two_calls(
    api: MSApi, ms_ingester: MobilisightsIngester, fake_redis
):
    _, res = api.export_car_info()
    next(res)  # skip first [
    bytes_data = next(res)
    parsed_data_first_call = ms_ingester._parse_car_state_with_cache(bytes_data)
    assert parsed_data_first_call is not None

    _, res = api.export_car_info()
    next(res)  # skip first [
    bytes_data = next(res)
    parsed_data_second_call = ms_ingester._parse_car_state_with_cache(bytes_data)
    assert parsed_data_second_call is None

