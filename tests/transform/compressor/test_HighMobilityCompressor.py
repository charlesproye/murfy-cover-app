import pytest

from transform.compressor.providers.high_mobility import HighMobilityCompressor

# Mark all tests in this module as integration tests (require real S3/Minio)
pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test__compress_temp_vin_data_buffer():
    compressor = HighMobilityCompressor("mercedes-benz")
    await compressor._compress_temp_vin_data_buffer(
        "response/mercedes-benz/W1VVVKFZ5P4323755/", upload=False
    )
