from unittest.mock import patch

import pytest

from transform.base_spark import BaseSpark


@pytest.fixture
@patch("transform.base_spark.BaseSpark.__abstractmethods__", set())
def base_spark():
    return BaseSpark("test")


def test_file_path_in_docker(base_spark):
    assert BaseSpark.file_path_in_docker() == "local:///app/src/transform/base_spark.py"
