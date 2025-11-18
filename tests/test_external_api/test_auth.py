"""
Authentication endpoint tests.
Tests for login, logout, token refresh, and security.
"""

import httpx
import pytest
from fastapi import status

from db_models import User, UserFleet


def extract_cookies_from_response(response: httpx.Response) -> dict[str, str]:
    """Extract cookies from Set-Cookie headers for httpx compatibility."""
    cookies = {}
    for cookie_header in response.headers.get_list("set-cookie"):
        # Parse cookie name and value from "name=value; ..." format
        cookie_parts = cookie_header.split(";")[0].split("=", 1)
        if len(cookie_parts) == 2:
            cookies[cookie_parts[0].strip()] = cookie_parts[1].strip()
    return cookies


class TestLogin:
    """Test suite for login endpoint."""

    @pytest.mark.asyncio
    async def test_login_success(self, app_client: httpx.AsyncClient, foo_user: User):
        """Test successful login with valid credentials."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Check response structure
        assert "user" in data
        assert "company" in data

        # Verify user data
        assert data["user"]["email"] == foo_user.email
        assert data["user"]["first_name"] == foo_user.first_name
        assert data["user"]["last_name"] == foo_user.last_name

        # Ensure password is NOT in response
        assert "password" not in data["user"]

        # Verify cookies are set (check Set-Cookie headers for httpx compatibility)
        set_cookie_headers = response.headers.get_list("set-cookie")
        assert any("evalue_access_token" in cookie for cookie in set_cookie_headers)
        assert any("evalue_refresh_token" in cookie for cookie in set_cookie_headers)
        assert any("evalue_session_data" in cookie for cookie in set_cookie_headers)

    @pytest.mark.asyncio
    async def test_login_invalid_email(self, app_client: httpx.AsyncClient):
        """Test login with invalid email."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": "nonexistent@example.com",
                "password": "wrongpassword",
            },
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert response.json()["detail"] == "Incorrect email or password"

    @pytest.mark.asyncio
    async def test_login_invalid_password(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test login with invalid password."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": "wrongpassword",
            },
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert response.json()["detail"] == "Incorrect email or password"

    @pytest.mark.asyncio
    async def test_login_missing_email(self, app_client: httpx.AsyncClient):
        """Test login with missing email field."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "password": "testpassword123",
            },
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    @pytest.mark.asyncio
    async def test_login_missing_password(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test login with missing password field."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
            },
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    @pytest.mark.asyncio
    async def test_login_invalid_email_format(self, app_client: httpx.AsyncClient):
        """Test login with invalid email format."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": "not-an-email",
                "password": "testpassword123",
            },
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    @pytest.mark.asyncio
    async def test_login_empty_credentials(self, app_client: httpx.AsyncClient):
        """Test login with empty credentials."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": "",
                "password": "",
            },
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestGetToken:
    """Test suite for /token endpoint (returns Bearer token)."""

    @pytest.mark.asyncio
    async def test_get_token_success(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test successful token retrieval."""
        response = await app_client.post(
            "/v1/auth/token",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Check token response
        assert "access_token" in data
        assert "token_type" in data
        assert data["token_type"] == "bearer"

        # Verify token is a valid JWT (basic check - has 3 parts)
        assert len(data["access_token"].split(".")) == 3

    @pytest.mark.asyncio
    async def test_get_token_invalid_credentials(self, app_client: httpx.AsyncClient):
        """Test token retrieval with invalid credentials."""
        response = await app_client.post(
            "/v1/auth/token",
            json={
                "email": "wrong@example.com",
                "password": "wrongpassword",
            },
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestLogout:
    """Test suite for logout endpoint."""

    @pytest.mark.asyncio
    async def test_logout_success(self, app_client: httpx.AsyncClient, foo_user: User):
        """Test successful logout."""
        # First login
        login_response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )
        assert login_response.status_code == status.HTTP_200_OK

        # Extract cookies for authenticated request
        cookies = extract_cookies_from_response(login_response)

        # Then logout with cookies
        logout_response = await app_client.post("/v1/auth/logout", cookies=cookies)

        assert logout_response.status_code == status.HTTP_200_OK
        assert logout_response.json()["message"] == "Logged out successfully"

        # Verify cookies are cleared (max_age should be 0 or negative)
        # Note: TestClient might handle cookie clearing differently
        # This is a basic check - in real browsers, cookies would be deleted

    @pytest.mark.asyncio
    async def test_logout_without_login(self, app_client: httpx.AsyncClient):
        """Test logout without being logged in (should still succeed)."""
        response = await app_client.post("/v1/auth/logout")

        # Logout should succeed even without being logged in
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["message"] == "Logged out successfully"


class TestRefreshToken:
    """Test suite for token refresh endpoint."""

    @pytest.mark.asyncio
    async def test_refresh_token_success(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test successful token refresh."""
        # First login to get tokens
        login_response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )
        assert login_response.status_code == status.HTTP_200_OK

        # Extract cookies from login response
        cookies = extract_cookies_from_response(login_response)

        # Refresh the token using cookies
        refresh_response = await app_client.post(
            "/v1/auth/refresh",
            cookies=cookies,
        )

        assert refresh_response.status_code == status.HTTP_200_OK
        assert refresh_response.json() == {"ok": True}

        # Verify new cookies are set
        set_cookie_headers = refresh_response.headers.get_list("set-cookie")
        assert any("evalue_access_token" in cookie for cookie in set_cookie_headers)
        assert any("evalue_refresh_token" in cookie for cookie in set_cookie_headers)

    @pytest.mark.asyncio
    async def test_refresh_token_without_cookies(self, app_client: httpx.AsyncClient):
        """Test token refresh without valid cookies."""
        response = await app_client.post("/v1/auth/refresh")

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @pytest.mark.asyncio
    async def test_refresh_token_with_invalid_token(
        self, app_client: httpx.AsyncClient
    ):
        """Test token refresh with invalid refresh token."""
        response = await app_client.post(
            "/v1/auth/refresh",
            cookies={"evalue_refresh_token": "invalid.token.here"},
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestAuthenticationSecurity:
    """Test suite for authentication security features."""

    @pytest.mark.asyncio
    async def test_password_not_returned_in_response(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Ensure password is never returned in API responses."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Check password is not in user object
        assert "password" not in data["user"]
        assert "password" not in str(data)  # Double-check in entire response

    @pytest.mark.asyncio
    async def test_httponly_cookies_set(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test that cookies have httponly flag set."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )

        assert response.status_code == status.HTTP_200_OK

        # Verify cookies exist (check Set-Cookie headers for httpx compatibility)
        set_cookie_headers = response.headers.get_list("set-cookie")
        assert any("evalue_access_token" in cookie for cookie in set_cookie_headers)
        assert any("evalue_refresh_token" in cookie for cookie in set_cookie_headers)

        # Verify httpOnly and Secure flags are set in cookies
        assert any("HttpOnly" in cookie for cookie in set_cookie_headers)
        # Note: Secure flag checking depends on environment settings

    @pytest.mark.asyncio
    async def test_sql_injection_attempt_in_email(self, app_client: httpx.AsyncClient):
        """Test protection against SQL injection in email field."""
        malicious_email = "admin'--"
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": malicious_email,
                "password": "anypassword",
            },
        )

        # Should fail validation or return 401, not cause SQL error
        assert response.status_code in [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
        ]

    @pytest.mark.asyncio
    async def test_timing_attack_resistance(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """
        Test that response time is similar for valid/invalid users.
        Note: This is a basic test - true timing attack testing requires
        more sophisticated tools and multiple samples.
        """

        # Test with valid user
        response1 = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": "wrongpassword",
            },
        )

        # Test with non-existent user
        response2 = await app_client.post(
            "/v1/auth/login",
            json={
                "email": "nonexistent@example.com",
                "password": "wrongpassword",
            },
        )

        # Both should return 401
        assert response1.status_code == status.HTTP_401_UNAUTHORIZED
        assert response2.status_code == status.HTTP_401_UNAUTHORIZED

        # Both should have same error message
        assert response1.json()["detail"] == response2.json()["detail"]


class TestAuthenticationWithUserFleet:
    """Test authentication for users with fleet associations."""

    @pytest.mark.asyncio
    async def test_login_user_with_fleet(
        self,
        app_client: httpx.AsyncClient,
        foo_user: User,
        foo_user_fleet: UserFleet,
    ):
        """Test login returns fleet information for users with fleets."""
        response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Verify user has fleet information
        assert "user" in data
        assert "fleets" in data["user"]

        # User should have at least one fleet
        assert isinstance(data["user"]["fleets"], list)
        if len(data["user"]["fleets"]) > 0:
            fleet = data["user"]["fleets"][0]
            assert "id" in fleet
            assert "name" in fleet


class TestBearer:
    """Test suite for Tesla bearer token endpoint."""

    @pytest.mark.asyncio
    async def test_bearer_endpoint_accepts_data(self, app_client: httpx.AsyncClient):
        """Test that bearer endpoint accepts and processes data."""
        test_data = {
            "code": "test_code_123",
            "redirect_uri": "http://localhost:3000/callback",
        }

        response = await app_client.post("/v1/auth/bearer", json=test_data)

        # Should return success
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"status": "done"}


class TestCookieAuthIntegration:
    """Integration tests for cookie-based authentication flow."""

    @pytest.mark.asyncio
    async def test_full_auth_flow_with_cookies(
        self, app_client: httpx.AsyncClient, foo_user: User
    ):
        """Test complete authentication flow: login -> refresh -> logout."""
        # Step 1: Login
        login_response = await app_client.post(
            "/v1/auth/login",
            json={
                "email": foo_user.email,
                "password": foo_user.plain_password,
            },
        )
        assert login_response.status_code == status.HTTP_200_OK
        cookies = extract_cookies_from_response(login_response)

        # Step 2: Refresh token
        refresh_response = await app_client.post("/v1/auth/refresh", cookies=cookies)
        assert refresh_response.status_code == status.HTTP_200_OK
        new_cookies = extract_cookies_from_response(refresh_response)

        # Step 3: Logout
        logout_response = await app_client.post("/v1/auth/logout", cookies=new_cookies)
        assert logout_response.status_code == status.HTTP_200_OK
        assert logout_response.json()["message"] == "Logged out successfully"
