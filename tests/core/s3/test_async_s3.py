import json
import pytest
import time
from core.s3.async_s3 import AsyncS3

@pytest.mark.asyncio
async def test_download_folder_in_batches():
    vin = "W1VVVKFZ5P4323755"
    s3 = AsyncS3(max_concurrency=50)
    
    start_time = time.time()
    total_files = 0
    
    async for batch in s3.download_folder_in_batches(f"response/mercedes-benz/{vin}/temp/", batch_size=1000):
        assert len(batch) > 0
        total_files += len(batch)
    
    elapsed = time.time() - start_time
    print(f"Downloaded {total_files} files in {elapsed:.2f}s ({total_files/elapsed:.1f} files/s)")
    
    file0 = json.loads(list(batch.values())[0])
    assert file0.get("vin") == vin
    
    if total_files > 100:  # Only check performance for substantial datasets
        assert elapsed < 10, f"Download took {elapsed:.2f}s, expected < 10s for {total_files} files"
