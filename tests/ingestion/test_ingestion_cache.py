import time

import fakeredis
import pytest

from ingestion.ingestion_cache import IngestionCache, _remove_paths


@pytest.fixture
def fake_redis(monkeypatch):
    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("ingestion.ingestion_cache.Redis", lambda **kwargs: r)
    return r


def test_ingestion_cache(fake_redis):
    cache = IngestionCache(make="mobilisights")

    test_data_in_cache = {"test": "test", "deep": {"deep_key": "deep_value"}}
    assert cache.json_in_db(vin="1234567890", json_data=test_data_in_cache) is False
    cache.set_json_in_db(vin="1234567890", json_data=test_data_in_cache)
    assert cache.json_in_db(vin="1234567890", json_data=test_data_in_cache) is True

    assert (
        cache.json_in_db(vin="1234567890", json_data={"test": "test"}) is False
    )  # not deep

    assert cache.json_in_db(vin="different_vin", json_data={"test": "test"}) is False


def test_ingestion_cache_sort_keys(fake_redis):
    cache = IngestionCache(make="mobilisights")
    test_data = {"a": "a", "b": "b", "$": {"-": 1, "Q": 2}}
    data_new_order = {
        "$": {"Q": 2, "-": 1},
        "b": "b",
        "a": "a",
    }

    assert cache.json_in_db(vin="1234567890", json_data=data_new_order) is False

    cache.set_json_in_db(vin="1234567890", json_data=test_data)
    assert cache.json_in_db(vin="1234567890", json_data=test_data) is True

    assert cache.json_in_db(vin="1234567890", json_data=data_new_order) is True


def test_ingestion_cache_with_keys_to_ignore(fake_redis):
    cache = IngestionCache(
        make="mobilisights", keys_to_ignore=["date.time", "id", "date"]
    )

    test_data = {
        "test": "value",
        "date": {"day": "2023-01-01", "time": "12:00:00"},
        "id": "123",
    }

    vin_in_cache = "1234567890"

    assert cache.json_in_db(vin=vin_in_cache, json_data=test_data) is False
    cache.set_json_in_db(vin=vin_in_cache, json_data=test_data)
    assert cache.json_in_db(vin=vin_in_cache, json_data=test_data) is True

    test_data_different_ignored = {
        "test": "value",
        "id": "456",
    }
    assert (
        cache.json_in_db(vin=vin_in_cache, json_data=test_data_different_ignored)
        is True
    )

    # Should not be in cache with different non-ignored keys
    test_data_different = {"test": "different", "timestamp": "2023-01-01", "id": "123"}
    assert cache.json_in_db(vin="1234567890", json_data=test_data_different) is False


def test_remove_paths_wildcard():
    test_data = {
        "test": "value",
        "datetime": "2024-01-01",
        "odometer": {
            "datetime": "2024-01-01",
            "value": 100,
            "last_updated": {"datetime": "2024-01-01"},
        },
    }

    assert _remove_paths(test_data, [["datetime"], ["*", "datetime"]]) == {
        "test": "value",
        "odometer": {
            "value": 100,
            "last_updated": {},
        },
    }


def test_ingestion_cache_with_min_change_interval(fake_redis):
    cache = IngestionCache(make="mobilisights", min_change_interval=0.2)

    test_data = {"test": "value"}
    assert cache.json_in_db(vin="1234567890", json_data=test_data) is False
    cache.set_json_in_db(vin="1234567890", json_data=test_data)

    test_data["test"] = "new_value"
    assert cache.json_in_db(vin="1234567890", json_data=test_data) is True

    time.sleep(0.2)
    assert cache.json_in_db(vin="1234567890", json_data=test_data) is False

