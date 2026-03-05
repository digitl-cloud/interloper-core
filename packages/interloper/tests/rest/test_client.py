"""Tests for REST client."""

from unittest.mock import MagicMock

import pytest

from interloper.errors import ConfigError
from interloper.rest.client import RESTClient
from interloper.rest.paginator import Paginator


class TestRESTClient:
    """Tests for the RESTClient class."""

    def test_stores_paginator(self):
        """RESTClient stores the paginator instance."""
        paginator = MagicMock(spec=Paginator)
        client = RESTClient(base_url="https://api.example.com", paginator=paginator)

        assert client._paginator is paginator
        client.close()

    def test_paginator_defaults_to_none(self):
        """RESTClient defaults paginator to None."""
        client = RESTClient(base_url="https://api.example.com")

        assert client._paginator is None
        client.close()

    def test_paginate_raises_without_paginator(self):
        """Paginate raises ConfigError when no paginator is configured."""
        client = RESTClient(base_url="https://api.example.com")

        with pytest.raises(ConfigError, match="no paginator configured"):
            list(client.paginate("/items"))

        client.close()

    def test_paginate_delegates_to_paginator(self):
        """Paginate yields from the paginator's paginate method."""
        paginator = MagicMock(spec=Paginator)
        paginator.paginate.return_value = iter([{"id": 1}, {"id": 2}])

        client = RESTClient(base_url="https://api.example.com", paginator=paginator)
        results = list(client.paginate("/items"))

        paginator.paginate.assert_called_once_with(client, "/items")
        assert results == [{"id": 1}, {"id": 2}]
        client.close()
