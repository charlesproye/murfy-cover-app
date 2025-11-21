"""
Tests for S3 client security features.

These tests verify that encryption in transit (HTTPS/TLS) is enforced
for all S3 operations, ensuring ISO27001 compliance.
"""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from core.s3.async_s3 import AsyncS3
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings, get_s3_settings


class TestS3Settings:
    """Test S3Settings validation for HTTPS enforcement."""

    def test_https_endpoint_valid(self):
        """Test that HTTPS endpoints are accepted."""
        settings = S3Settings(
            S3_ENDPOINT="https://s3.fr-par.scw.cloud",
            S3_BUCKET="test-bucket",
            S3_KEY="test-key",
            S3_SECRET="test-secret",
        )
        assert settings.S3_ENDPOINT == "https://s3.fr-par.scw.cloud"

    def test_http_endpoint_rejected(self):
        """Test that HTTP endpoints are rejected with validation error."""
        with pytest.raises(ValidationError) as exc_info:
            S3Settings(
                S3_ENDPOINT="http://s3.fr-par.scw.cloud",  # HTTP not allowed
                S3_BUCKET="test-bucket",
                S3_KEY="test-key",
                S3_SECRET="test-secret",
            )

        # Verify the error message mentions HTTPS requirement
        error_msg = str(exc_info.value)
        assert "HTTPS" in error_msg or "https" in error_msg

    def test_get_s3_settings_prod_validates_https(self):
        """Test that get_s3_settings validates HTTPS for prod environment."""
        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT": "http://insecure.endpoint.com",
                    "S3_BUCKET": "test-bucket",
                    "S3_KEY": "test-key",
                    "S3_SECRET": "test-secret",
                },
            ),
            pytest.raises(ValidationError),
        ):
            get_s3_settings(env="prod")

    def test_get_s3_settings_dev_validates_https(self):
        """Test that get_s3_settings validates HTTPS for dev environment."""
        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_DEV": "http://insecure.endpoint.com",
                    "S3_BUCKET_DEV": "test-bucket",
                    "S3_KEY_DEV": "test-key",
                    "S3_SECRET_DEV": "test-secret",
                },
            ),
            pytest.raises(ValidationError),
        ):
            get_s3_settings(env="dev")


class TestS3ServiceSecurity:
    """Test S3Service security enforcement."""

    def test_s3_service_rejects_http_endpoint(self):
        """Test that S3Service rejects HTTP endpoints."""
        # Create settings with HTTP endpoint (this should fail at settings level)
        with pytest.raises(ValidationError):
            settings = S3Settings(
                S3_ENDPOINT="http://insecure.endpoint.com",
                S3_BUCKET="test-bucket",
                S3_KEY="test-key",
                S3_SECRET="test-secret",
            )
            S3Service(settings)

    def test_s3_service_accepts_https_endpoint(self):
        """Test that S3Service accepts HTTPS endpoints."""
        settings = S3Settings(
            S3_ENDPOINT="https://s3.fr-par.scw.cloud",
            S3_BUCKET="test-bucket",
            S3_KEY="test-key",
            S3_SECRET="test-secret",
        )
        # Should not raise any exception
        service = S3Service(settings)
        assert service._settings.S3_ENDPOINT == "https://s3.fr-par.scw.cloud"

    def test_s3_service_client_has_ssl_enabled(self):
        """Test that the boto3 client is created with SSL enabled."""
        settings = S3Settings(
            S3_ENDPOINT="https://s3.fr-par.scw.cloud",
            S3_BUCKET="test-bucket",
            S3_KEY="test-key",
            S3_SECRET="test-secret",
        )
        service = S3Service(settings)

        # Verify the client was created (this validates the config was accepted)
        assert service._s3_client is not None

        # Verify endpoint is HTTPS
        endpoint = service._s3_client.meta.endpoint_url
        assert endpoint.startswith("https://")


class TestAsyncS3Security:
    """Test AsyncS3 security enforcement."""

    def test_async_s3_rejects_http_endpoint(self):
        """Test that AsyncS3 rejects HTTP endpoints."""
        # Mock environment with HTTP endpoint
        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT": "http://insecure.endpoint.com",
                    "S3_BUCKET": "test-bucket",
                    "S3_KEY": "test-key",
                    "S3_SECRET": "test-secret",
                },
            ),
            pytest.raises(ValidationError),
        ):
            # This will fail at validation when get_s3_settings is called
            AsyncS3(env="prod")

    def test_async_s3_accepts_https_endpoint(self):
        """Test that AsyncS3 accepts HTTPS endpoints."""
        with patch.dict(
            os.environ,
            {
                "S3_ENDPOINT": "https://s3.fr-par.scw.cloud",
                "S3_BUCKET": "test-bucket",
                "S3_KEY": "test-key",
                "S3_SECRET": "test-secret",
            },
        ):
            # Should not raise any exception
            async_s3 = AsyncS3(env="prod")
            assert async_s3._settings.S3_ENDPOINT == "https://s3.fr-par.scw.cloud"


class TestSecurityDocumentation:
    """Test that security documentation is clear and accessible."""

    def test_s3_service_has_security_docstring(self):
        """Test that S3Service has security-focused documentation."""
        docstring = S3Service.__doc__
        assert docstring is not None
        assert "encryption in transit" in docstring.lower() or "HTTPS" in docstring

    def test_async_s3_has_security_docstring(self):
        """Test that AsyncS3 has security-focused documentation."""
        docstring = AsyncS3.__doc__
        assert docstring is not None
        assert "encryption in transit" in docstring.lower() or "HTTPS" in docstring
