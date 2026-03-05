"""Tests for REST authentication handlers."""

from unittest.mock import MagicMock

import httpx
import pytest

from interloper.errors import AuthenticationError
from interloper.rest.auth import HTTPBearerAuth, OAuth2Auth, OAuth2RefreshTokenAuth


class TestHTTPBearerAuth:
    """Tests for HTTP Bearer authentication."""

    def test_sets_authorization_header(self):
        """Bearer auth sets the Authorization header on the request."""
        auth = HTTPBearerAuth(token="my-secret-token")
        request = httpx.Request("GET", "https://example.com")

        flow = auth.sync_auth_flow(request)
        authed_request = next(flow)

        assert authed_request.headers["Authorization"] == "Bearer my-secret-token"


class TestOAuth2Auth:
    """Tests for OAuth2 authentication."""

    @pytest.fixture()
    def auth(self):
        """Create an OAuth2Auth instance with standard test params.

        Returns:
            OAuth2Auth fixture.
        """
        return OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

    @pytest.fixture()
    def auth_with_scope(self):
        """Create an OAuth2Auth instance with a scope.

        Returns:
            OAuth2Auth fixture with scope.
        """
        return OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            scope="read write",
        )

    @pytest.fixture()
    def auth_with_refresh(self):
        """Create an OAuth2Auth instance with a refresh token.

        Returns:
            OAuth2Auth fixture with refresh token.
        """
        return OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            refresh_token="my-refresh-token",
            access_token="initial-access-token",
        )

    # -- auth_data ---------------------------------------------------------

    def test_auth_data_without_scope(self, auth):
        """auth_data contains client_id, client_secret, and grant_type."""
        data = auth.auth_data
        assert data == {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "grant_type": "client_credentials",
        }

    def test_auth_data_with_scope(self, auth_with_scope):
        """auth_data includes scope when provided."""
        data = auth_with_scope.auth_data
        assert data == {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "grant_type": "client_credentials",
            "scope": "read write",
        }

    # -- access_token property ---------------------------------------------

    def test_access_token_raises_when_none(self, auth):
        """Accessing access_token raises AuthenticationError when not set."""
        with pytest.raises(AuthenticationError, match="No access token available"):
            _ = auth.access_token

    def test_access_token_returns_value(self):
        """Accessing access_token returns the token when set."""
        auth = OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="cid",
            client_secret="csecret",
            access_token="my-token",
        )
        assert auth.access_token == "my-token"

    # -- refresh_token property --------------------------------------------

    def test_refresh_token_returns_none_by_default(self, auth):
        """refresh_token returns None when not provided."""
        assert auth.refresh_token is None

    def test_refresh_token_returns_value(self, auth_with_refresh):
        """refresh_token returns the token when provided."""
        assert auth_with_refresh.refresh_token == "my-refresh-token"

    # -- clear_token -------------------------------------------------------

    def test_clear_token(self):
        """clear_token sets _access_token to None."""
        auth = OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="cid",
            client_secret="csecret",
            access_token="some-token",
        )
        assert auth.access_token == "some-token"
        auth.clear_token()
        with pytest.raises(AuthenticationError):
            _ = auth.access_token

    # -- token_endpoint normalization --------------------------------------

    def test_token_endpoint_adds_leading_slash(self):
        """Token endpoint without leading slash gets one prepended."""
        auth = OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="cid",
            client_secret="csecret",
            token_endpoint="oauth2/token",
        )
        assert auth._token_endpoint == "/oauth2/token"

    def test_token_endpoint_preserves_leading_slash(self, auth):
        """Token endpoint with leading slash is left unchanged."""
        assert auth._token_endpoint == "/oauth2/token"

    # -- _acquire_token ----------------------------------------------------

    def test_acquire_token(self, auth):
        """_acquire_token POSTs to token endpoint and stores tokens."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "new-access-token",
            "refresh_token": "new-refresh-token",
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        auth._token_client = mock_client
        auth._acquire_token()

        mock_client.post.assert_called_once_with(
            "/oauth2/token",
            data=auth.auth_data,
            headers=None,
        )
        assert auth._access_token == "new-access-token"
        assert auth._refresh_token == "new-refresh-token"

    def test_acquire_token_preserves_existing_refresh_token(self, auth):
        """_acquire_token keeps existing refresh_token if response omits it."""
        auth._refresh_token = "existing-refresh"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "new-access-token"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        auth._token_client = mock_client
        auth._acquire_token()

        assert auth._access_token == "new-access-token"
        assert auth._refresh_token == "existing-refresh"

    # -- _refresh_access_token ---------------------------------------------

    def test_refresh_access_token_raises_without_refresh_token(self, auth):
        """_refresh_access_token raises AuthenticationError when no refresh token."""
        with pytest.raises(AuthenticationError, match="no refresh token available"):
            auth._refresh_access_token()

    def test_refresh_access_token(self, auth_with_refresh):
        """_refresh_access_token POSTs refresh grant and stores tokens."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "refreshed-access-token",
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        auth_with_refresh._token_client = mock_client
        auth_with_refresh._refresh_access_token()

        mock_client.post.assert_called_once_with(
            "/oauth2/token",
            data={
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "grant_type": "refresh_token",
                "refresh_token": "my-refresh-token",
            },
            headers=None,
        )
        assert auth_with_refresh._access_token == "refreshed-access-token"

    # -- sync_auth_flow ----------------------------------------------------

    def test_sync_auth_flow_acquires_token_when_none(self, auth):
        """sync_auth_flow acquires a token if none is set, then sets header."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "acquired-token"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        auth._token_client = mock_client

        request = httpx.Request("GET", "https://api.example.com/data")
        flow = auth.sync_auth_flow(request)
        authed_request = next(flow)

        assert authed_request.headers["Authorization"] == "Bearer acquired-token"

        # Simulate a 200 response -- flow should finish
        ok_response = httpx.Response(200, request=authed_request)
        with pytest.raises(StopIteration):
            flow.send(ok_response)

    def test_sync_auth_flow_uses_existing_token(self):
        """sync_auth_flow uses existing token without acquiring."""
        auth = OAuth2Auth(
            base_url="https://auth.example.com",
            client_id="cid",
            client_secret="csecret",
            access_token="pre-existing-token",
        )

        request = httpx.Request("GET", "https://api.example.com/data")
        flow = auth.sync_auth_flow(request)
        authed_request = next(flow)

        assert authed_request.headers["Authorization"] == "Bearer pre-existing-token"

    def test_sync_auth_flow_refreshes_on_401(self, auth_with_refresh):
        """sync_auth_flow refreshes the token and retries on 401."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "refreshed-token"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        auth_with_refresh._token_client = mock_client

        request = httpx.Request("GET", "https://api.example.com/data")
        flow = auth_with_refresh.sync_auth_flow(request)
        authed_request = next(flow)

        assert authed_request.headers["Authorization"] == "Bearer initial-access-token"

        # Simulate a 401 response
        unauthorized = httpx.Response(401, request=authed_request)
        retry_request = flow.send(unauthorized)

        assert retry_request.headers["Authorization"] == "Bearer refreshed-token"

    def test_sync_auth_flow_acquires_on_401_without_refresh_token(self, auth):
        """sync_auth_flow falls back to acquire when refresh fails on 401."""
        auth._access_token = "stale-token"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "new-acquired-token"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        auth._token_client = mock_client

        request = httpx.Request("GET", "https://api.example.com/data")
        flow = auth.sync_auth_flow(request)
        authed_request = next(flow)

        assert authed_request.headers["Authorization"] == "Bearer stale-token"

        # Simulate 401 -- no refresh token, so should fall back to acquire
        unauthorized = httpx.Response(401, request=authed_request)
        retry_request = flow.send(unauthorized)

        assert retry_request.headers["Authorization"] == "Bearer new-acquired-token"


class TestOAuth2RefreshTokenAuth:
    """Tests for OAuth2 refresh token authentication."""

    @pytest.fixture()
    def auth(self):
        """Create an OAuth2RefreshTokenAuth instance.

        Returns:
            OAuth2RefreshTokenAuth fixture.
        """
        return OAuth2RefreshTokenAuth(
            base_url="https://auth.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            refresh_token="my-refresh-token",
        )

    def test_grant_type(self, auth):
        """grant_type is 'refresh_token'."""
        assert auth.grant_type == "refresh_token"

    def test_auth_data_includes_refresh_token(self, auth):
        """auth_data includes the refresh_token."""
        data = auth.auth_data
        assert data == {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "grant_type": "refresh_token",
            "refresh_token": "my-refresh-token",
        }

    def test_auth_data_raises_when_refresh_token_none(self):
        """auth_data raises AuthenticationError if refresh_token is removed."""
        auth = OAuth2RefreshTokenAuth(
            base_url="https://auth.example.com",
            client_id="cid",
            client_secret="csecret",
            refresh_token="my-token",
        )
        auth._refresh_token = None

        with pytest.raises(AuthenticationError, match="Refresh token is required"):
            _ = auth.auth_data

    def test_refresh_calls_refresh_access_token(self, auth):
        """refresh() delegates to _refresh_access_token."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "refreshed"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        auth._token_client = mock_client

        auth.refresh()

        mock_client.post.assert_called_once()
        assert auth._access_token == "refreshed"
