from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from core.s3.async_s3 import AsyncS3
from core.s3.settings import S3Settings


@pytest.mark.asyncio
async def test_upload_files_reuses_client():
    # Mock settings to avoid environment variable issues
    settings = S3Settings(
        S3_ENDPOINT="https://localhost:9000",
        S3_REGION="us-east-1",
        S3_BUCKET="test-bucket",
        S3_KEY="minioadmin",
        S3_SECRET="minioadmin",
    )

    # Initialize AsyncS3
    s3 = AsyncS3(settings=settings)

    # Mock the client object
    mock_client = AsyncMock()
    mock_client.put_object = AsyncMock(return_value={})

    # Mock the _client method to return our mock client
    # Since _client is an async context manager, we need to mock it appropriately
    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    # We replace the _client method on the instance
    with patch.object(s3, "_client", side_effect=mock_client_ctx) as mock_ctx_method:
        files = {"key1": b"content1", "key2": b"content2", "key3": b"content3"}

        results = await s3.upload_files(files)

        # Verify _client() was called only once (meaning one session/connection setup)
        mock_ctx_method.assert_called_once()

        # Verify put_object was called 3 times (once for each file)
        assert mock_client.put_object.call_count == 3

        # Verify arguments
        # Note: tasks execute in parallel so order is not guaranteed
        called_keys = sorted(
            [call.kwargs["Key"] for call in mock_client.put_object.call_args_list]
        )
        assert called_keys == ["key1", "key2", "key3"]

        # Verify results (None means success)
        assert len(results) == 3
        assert all(r is None for r in results)


@pytest.mark.asyncio
async def test_upload_files_handles_exceptions():
    # Mock settings
    settings = S3Settings(
        S3_ENDPOINT="https://localhost:9000",
        S3_REGION="us-east-1",
        S3_BUCKET="test-bucket",
        S3_KEY="minioadmin",
        S3_SECRET="minioadmin",
    )

    s3 = AsyncS3(settings=settings)

    mock_client = AsyncMock()

    # Make put_object fail for one key
    async def side_effect(*args, **kwargs):
        if kwargs["Key"] == "key2":
            raise Exception("Upload failed")
        return {}

    mock_client.put_object = AsyncMock(side_effect=side_effect)

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    with patch.object(s3, "_client", side_effect=mock_client_ctx):
        files = {"key1": b"content1", "key2": b"content2", "key3": b"content3"}

        results = await s3.upload_files(files)

        # Should have 3 results
        assert len(results) == 3

        # Check that we have 2 successes (None) and 1 exception
        exceptions = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if r is None]

        assert len(exceptions) == 1
        assert str(exceptions[0]) == "Upload failed"
        assert len(successes) == 2
