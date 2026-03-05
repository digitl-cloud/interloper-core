"""Tests for REST pagination strategies."""

from unittest.mock import MagicMock

import pytest

from interloper.rest.paginator import PageNumberPaginator, _json_path_extract


class TestJsonPathExtract:
    """Tests for _json_path_extract helper."""

    def test_simple_key(self):
        """Extracts a top-level key."""
        assert _json_path_extract({"count": 42}, "count") == 42

    def test_nested_key(self):
        """Extracts a value via dot-separated nested path."""
        data = {"meta": {"pagination": {"total_pages": 5}}}
        assert _json_path_extract(data, "meta.pagination.total_pages") == 5

    def test_empty_path_raises(self):
        """Raises ValueError for an empty path string."""
        with pytest.raises(ValueError, match="non-empty string"):
            _json_path_extract({"a": 1}, "")

    def test_malformed_path_raises(self):
        """Raises ValueError for a path with invalid characters."""
        with pytest.raises(ValueError, match="Invalid path format"):
            _json_path_extract({"a": 1}, "a..b")

    def test_missing_key_raises(self):
        """Raises ValueError when a key in the path does not exist."""
        with pytest.raises(ValueError, match="not found"):
            _json_path_extract({"a": 1}, "b")

    def test_non_dict_intermediate_raises(self):
        """Raises TypeError when an intermediate value is not a dict."""
        with pytest.raises(TypeError, match="Expected dict"):
            _json_path_extract({"a": "not-a-dict"}, "a.b")


class TestPageNumberPaginator:
    """Tests for PageNumberPaginator."""

    @pytest.fixture()
    def mock_client(self):
        """Create a mock httpx client that returns configurable responses.

        Returns:
            Mock httpx client fixture.
        """
        return MagicMock()

    def _make_response(self, data):
        """Create a mock response with .json() and .raise_for_status().

        Returns:
            Mock response with the given data.
        """
        response = MagicMock()
        response.json.return_value = data
        response.raise_for_status = MagicMock()
        return response

    def test_single_page(self, mock_client):
        """Yields a single page when data is empty after first fetch."""
        paginator = PageNumberPaginator(max_pages=1)
        mock_client.get.return_value = self._make_response({"items": [1, 2]})

        results = list(paginator.paginate(mock_client, "/items"))

        assert results == [{"items": [1, 2]}]
        mock_client.get.assert_called_once_with("/items", params={"page": 1})

    def test_multiple_pages(self, mock_client):
        """Yields multiple pages until max_pages is reached."""
        paginator = PageNumberPaginator(max_pages=3)
        mock_client.get.side_effect = [
            self._make_response({"items": [1]}),
            self._make_response({"items": [2]}),
            self._make_response({"items": [3]}),
        ]

        results = list(paginator.paginate(mock_client, "/items"))

        assert len(results) == 3
        assert results[0] == {"items": [1]}
        assert results[2] == {"items": [3]}

    def test_stops_at_total_pages(self, mock_client):
        """Stops when the page number reaches total_pages from the response."""
        paginator = PageNumberPaginator(
            total_pages_path="meta.total_pages",
            max_pages=50,
        )
        mock_client.get.side_effect = [
            self._make_response({"data": [1], "meta": {"total_pages": 2}}),
            self._make_response({"data": [2], "meta": {"total_pages": 2}}),
        ]

        results = list(paginator.paginate(mock_client, "/items"))

        assert len(results) == 2

    def test_stops_at_max_pages(self, mock_client):
        """Stops at max_pages even if more data is available."""
        paginator = PageNumberPaginator(max_pages=2)
        mock_client.get.side_effect = [
            self._make_response({"items": [1]}),
            self._make_response({"items": [2]}),
        ]

        results = list(paginator.paginate(mock_client, "/items"))

        assert len(results) == 2

    def test_stops_on_empty_data(self, mock_client):
        """Stops when the response data is empty (falsy)."""
        paginator = PageNumberPaginator(max_pages=50)
        mock_client.get.side_effect = [
            self._make_response({"items": [1]}),
            self._make_response({}),
        ]

        results = list(paginator.paginate(mock_client, "/items"))

        assert len(results) == 2

    def test_custom_initial_page(self, mock_client):
        """Starts from the configured initial_page."""
        paginator = PageNumberPaginator(initial_page=5, max_pages=5)
        mock_client.get.return_value = self._make_response({"items": [1]})

        list(paginator.paginate(mock_client, "/items"))

        mock_client.get.assert_called_with("/items", params={"page": 5})

    def test_custom_page_param(self, mock_client):
        """Uses the configured page_param in query parameters."""
        paginator = PageNumberPaginator(page_param="p", max_pages=1)
        mock_client.get.return_value = self._make_response({"items": [1]})

        list(paginator.paginate(mock_client, "/items"))

        mock_client.get.assert_called_once_with("/items", params={"p": 1})
