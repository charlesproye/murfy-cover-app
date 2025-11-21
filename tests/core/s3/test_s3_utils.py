import pytest

from core.s3.s3_utils import S3Service


@pytest.mark.integration
def test_store_object():
    s3 = S3Service()
    s3.store_object(b"test", "response/stellantis/test.json")
    assert s3.get_file("response/stellantis/test.json") == b"test"
