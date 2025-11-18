import asyncio
import json
import time

import pytest

from core.s3.async_s3 import AsyncS3

# Mark all tests in this module as integration tests (require real S3/Minio)
pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_download_folder_in_batches():
    vin = "W1VVVKFZ5P4323755"
    s3 = AsyncS3(max_concurrency=50)

    start_time = time.time()
    total_files = 0

    async for batch in s3.download_folder_in_batches(
        f"response/mercedes-benz/{vin}/temp/", batch_size=1000
    ):
        assert len(batch) > 0
        total_files += len(batch)

    elapsed = time.time() - start_time
    print(
        f"Downloaded {total_files} files in {elapsed:.2f}s ({total_files / elapsed:.1f} files/s)"
    )

    file0 = json.loads(next(batch.values()))
    assert file0.get("vin") == vin

    if total_files > 100:  # Only check performance for substantial datasets
        assert elapsed < 10, (
            f"Download took {elapsed:.2f}s, expected < 10s for {total_files} files"
        )


@pytest.mark.asyncio
async def test_list_content():
    s3 = AsyncS3(max_concurrency=200)
    folders, files = await s3.list_content("response/stellantis/")

    assert len(folders) > 1

    all_files = []

    tasks = [s3.list_content(f"{vin_path}temp/") for vin_path in folders]

    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        folders_files = await coro
        _, files = folders_files
        all_files.extend(files)
        print(f"Progress: {i}/{len(tasks)} folders processed")

    assert len(all_files) > 0
