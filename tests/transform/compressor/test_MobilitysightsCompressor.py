import pytest

from transform.compressor.providers.mobilisight import MobilisightCompressor

# Mark all tests in this module as integration tests (require real S3/Minio)
pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_compress_temp_vin_data_buffer_mobilisight():
    compressor = MobilisightCompressor("stellantis")
    await compressor._compress_temp_vin_data_buffer(
        "response/stellantis/VF31NZKYZHU801009/", upload=False
    )
