import hashlib
import json
import logging
import os
import time
from typing import Any

import msgspec
from redis import Redis

LOGGER = logging.getLogger(__name__)


def _remove_paths(data: dict, paths: list[tuple[str, ...]]) -> Any:
    """Recursively remove keys given as path strings or lists, with wildcard support."""
    if not isinstance(data, dict):
        return data

    result = {}
    for k, v in data.items():
        # collect subpaths for this key
        subpaths = []
        for path in paths:
            if not path:
                continue
            head, *tail = path
            if head == "*" or head == k:
                subpaths.append(tail)
            # Keep wildcard paths for recursive application at all levels
            if head == "*" and tail:
                subpaths.append(path)

        # if there is an empty subpath, drop this key entirely
        if any(len(path) == 0 for path in subpaths):
            continue

        # recurse if there are still subpaths to follow
        if subpaths:
            result[k] = _remove_paths(v, subpaths)
        else:
            result[k] = v

    return result


class IngestionCache:
    def __init__(
        self,
        make: str,
        keys_to_ignore: list[str] | None = None,
        min_change_interval: float | None = None,
    ):
        """
        Parameters
        ----------
        make: str
            The make of the vehicle
        keys_to_ignore: list[str] | None
            The keys to ignore
        min_change_interval: float | None
            The minimum change interval in seconds. If provided, the cache will only store the data if it has changed since the last time it was stored.
        """
        redis_host = os.environ.get("REDIS_HOST", "redis")
        redis_port = int(os.environ.get("REDIS_PORT", 6379))
        redis_user = os.environ.get("REDIS_USER")
        redis_password = os.environ.get("REDIS_PASSWORD")
        redis_db = int(os.environ.get("REDIS_DB_INGESTION", 1))

        self.__redis = Redis(
            host=redis_host,
            port=redis_port,
            username=redis_user,
            password=redis_password,
            decode_responses=True,
            db=redis_db,
        )

        LOGGER.info(f"Redis host: {redis_host}, port: {redis_port}, db: {redis_db}")

        self.__make = make
        # store ignore paths as list of lists, e.g. "alerts.value" -> ["alerts","value"]
        self.__ignore_paths = (
            [tuple(p.split(".")) for p in keys_to_ignore] if keys_to_ignore else []
        )

        self._min_change_interval = min_change_interval

    def __compute_hash(self, json_data: dict[str, Any] | msgspec.Struct) -> str:
        if self.__ignore_paths:
            hash_data = _remove_paths(json_data, self.__ignore_paths)
        else:
            hash_data = json_data

        serialized = json.dumps(hash_data, sort_keys=True).encode()
        return hashlib.sha256(serialized).hexdigest()

    def json_in_db(self, vin: str, json_data: dict[str, Any]) -> bool:
        hash_data = self.__compute_hash(json_data)

        key = f"{self.__make}:{vin}"
        cached_data = self.__redis.get(key)

        if cached_data is None:
            return False

        if self._min_change_interval and ":" in cached_data:
            # Might have some old keys without the last change timestamp
            cached_hash, last_change = cached_data.split(":")
            if (
                last_change
                and time.time() - float(last_change) < self._min_change_interval
            ):
                return True  # data is still valid
        else:
            cached_hash = cached_data

        is_cached = cached_hash == hash_data

        return is_cached

    def set_json_in_db(
        self,
        vin: str,
        json_data: dict[str, Any],
        ttl: int | None = None,
    ) -> None:
        hash_data = self.__compute_hash(json_data)
        key = f"{self.__make}:{vin}"

        if self._min_change_interval:
            hash_data = f"{hash_data}:{time.time()}"

        if ttl:
            self.__redis.set(key, hash_data, ex=ttl)
        else:
            self.__redis.set(key, hash_data)

