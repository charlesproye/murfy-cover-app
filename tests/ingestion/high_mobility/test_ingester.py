import json

import fakeredis
import pytest

from ingestion.high_mobility.ingester import HMIngester
from ingestion.high_mobility.vehicle import Vehicle

# Mark all tests in this module as integration tests (require real High Mobility API)
pytestmark = pytest.mark.integration


@pytest.fixture
def fake_redis(monkeypatch):
    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("ingestion.ingestion_cache.Redis", lambda **kwargs: r)
    return r


@pytest.fixture
def hm_ingester(fake_redis):
    ingester = HMIngester()
    return ingester


@pytest.fixture
def vehicules(hm_ingester: HMIngester) -> list[Vehicle]:
    clearances = hm_ingester._fetch_clearances()

    return clearances


def test_process_vehicle_renault(
    hm_ingester: HMIngester, fake_redis, vehicules: list[Vehicle]
):
    vehicule = next(v for v in vehicules if v.brand == "renault")

    parsed_data = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert json.loads(parsed_data)["vin"] == vehicule.vin

    parsed_data_2nd = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert parsed_data_2nd is None  # cached


def test_process_vehicle_mercedes_benz(
    hm_ingester: HMIngester, fake_redis, vehicules: list[Vehicle]
):
    vehicule = next(v for v in vehicules if v.brand == "mercedes-benz")

    parsed_data = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert json.loads(parsed_data)["vin"] == vehicule.vin

    parsed_data_2nd = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert parsed_data_2nd is None  # cached


def test_process_vehicle_ford(
    hm_ingester: HMIngester, fake_redis, vehicules: list[Vehicle]
):
    vehicule = next(v for v in vehicules if v.brand == "ford")

    parsed_data = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert json.loads(parsed_data)["vin"] == vehicule.vin

    parsed_data_2nd = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert parsed_data_2nd is None  # cached


def test_process_vehicle_kia(
    hm_ingester: HMIngester, fake_redis, vehicules: list[Vehicle]
):
    vehicule = next(v for v in vehicules if v.brand == "kia")

    parsed_data = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert json.loads(parsed_data)["vin"] == vehicule.vin

    parsed_data_2nd = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert parsed_data_2nd is None  # cached


def test_process_vehicle_volvo_cars(
    hm_ingester: HMIngester, fake_redis, vehicules: list[Vehicle]
):
    vehicule = next(v for v in vehicules if v.brand == "volvo-cars")

    parsed_data = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert json.loads(parsed_data)["vin"] == vehicule.vin

    parsed_data_2nd = hm_ingester._process_vehicle(vehicule, auto_upload=False)
    assert parsed_data_2nd is None  # cached
