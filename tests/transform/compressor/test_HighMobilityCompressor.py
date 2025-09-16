from transform.compressor.providers.high_mobility import HighMobilityCompressor
import pytest

@pytest.mark.asyncio
async def test__compress_temp_vin_data_buffer():
    compressor = HighMobilityCompressor("mercedes-benz")
    await compressor._compress_temp_vin_data_buffer("response/mercedes-benz/W1VVVKFZ5P4323755/", upload=False) 
