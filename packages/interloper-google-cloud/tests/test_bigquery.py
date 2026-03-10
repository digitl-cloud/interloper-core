"""Tests for BigQueryIO."""

import datetime
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from interloper.errors import ConfigError
from interloper.io.database import WriteDisposition
from interloper.serialization.base import ComponentInstanceSpec

from interloper_google_cloud.io.bigquery import BigQueryIO, _bq_to_py_type, _py_to_bq_type

# A minimal service account key JSON for testing.
_SA_KEY = json.dumps({"type": "service_account", "project_id": "test-proj"})

# ------------------------------------------------------------------
# _py_to_bq_type
# ------------------------------------------------------------------


class TestPyToBqType:
    """Map BigQuery field types to Python values."""

    def test_bool(self):
        assert _py_to_bq_type(True) == "BOOLEAN"
        assert _py_to_bq_type(False) == "BOOLEAN"

    def test_int(self):
        assert _py_to_bq_type(42) == "INTEGER"
        assert _py_to_bq_type(0) == "INTEGER"
        assert _py_to_bq_type(-1) == "INTEGER"

    def test_float(self):
        assert _py_to_bq_type(3.14) == "FLOAT"
        assert _py_to_bq_type(0.0) == "FLOAT"

    def test_decimal(self):
        assert _py_to_bq_type(Decimal("9.99")) == "NUMERIC"

    def test_datetime(self):
        assert _py_to_bq_type(datetime.datetime(2024, 1, 1, 12, 0)) == "TIMESTAMP"

    def test_date(self):
        assert _py_to_bq_type(datetime.date(2024, 1, 1)) == "DATE"

    def test_bytes(self):
        assert _py_to_bq_type(b"raw") == "BYTES"

    def test_string(self):
        assert _py_to_bq_type("hello") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _py_to_bq_type(None) == "STRING"

    def test_list_falls_back_to_string(self):
        assert _py_to_bq_type([1, 2, 3]) == "STRING"

    def test_dict_falls_back_to_string(self):
        assert _py_to_bq_type({"a": 1}) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        # True is also isinstance(True, int), so order matters
        assert _py_to_bq_type(True) == "BOOLEAN"
        assert _py_to_bq_type(True) != "INTEGER"


# ------------------------------------------------------------------
# _py_to_bq_type
# ------------------------------------------------------------------


class TestBqToPyType:
    """Map Python values to BigQuery query parameter types."""

    def test_bool(self):
        assert _bq_to_py_type(True) == "BOOL"
        assert _bq_to_py_type(False) == "BOOL"

    def test_int(self):
        assert _bq_to_py_type(42) == "INT64"

    def test_float(self):
        assert _bq_to_py_type(3.14) == "FLOAT64"

    def test_decimal(self):
        assert _bq_to_py_type(Decimal("1.5")) == "NUMERIC"

    def test_datetime(self):
        assert _bq_to_py_type(datetime.datetime(2024, 6, 15, 8, 30)) == "TIMESTAMP"

    def test_date(self):
        assert _bq_to_py_type(datetime.date(2024, 6, 15)) == "DATE"

    def test_bytes(self):
        assert _bq_to_py_type(b"\x00") == "BYTES"

    def test_string(self):
        assert _bq_to_py_type("text") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _bq_to_py_type(None) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        assert _bq_to_py_type(True) == "BOOL"


# ------------------------------------------------------------------
# BigQueryIO.__init__
# ------------------------------------------------------------------


class TestBigQueryIOInit:
    """BigQueryIO constructor and attribute storage."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_default_params(self, mock_client_cls, mock_sa):
        mock_creds = MagicMock()
        mock_sa.Credentials.from_service_account_info.return_value = mock_creds
        io = BigQueryIO(project="my-project", service_account_key=_SA_KEY)
        assert io.project == "my-project"
        assert io.default_dataset is None
        assert io.location == "EU"
        assert io.write_disposition == WriteDisposition.REPLACE
        assert io.chunk_size == 1000
        mock_client_cls.assert_called_once_with(project="my-project", credentials=mock_creds, location="EU")

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_custom_params(self, mock_client_cls, mock_sa):
        mock_creds = MagicMock()
        mock_sa.Credentials.from_service_account_info.return_value = mock_creds
        io = BigQueryIO(
            project="other-project",
            default_dataset="analytics",
            location="US",
            service_account_key=_SA_KEY,
            write_disposition=WriteDisposition.APPEND,
            chunk_size=500,
        )
        assert io.project == "other-project"
        assert io.default_dataset == "analytics"
        assert io.location == "US"
        assert io.write_disposition == WriteDisposition.APPEND
        assert io.chunk_size == 500
        mock_client_cls.assert_called_once_with(project="other-project", credentials=mock_creds, location="US")

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_service_account_key_parsed(self, mock_client_cls, mock_sa):
        mock_creds = MagicMock()
        mock_sa.Credentials.from_service_account_info.return_value = mock_creds
        BigQueryIO(project="p", service_account_key=_SA_KEY)
        mock_sa.Credentials.from_service_account_info.assert_called_once_with(json.loads(_SA_KEY))
        mock_client_cls.assert_called_once_with(project="p", credentials=mock_creds, location="EU")


# ------------------------------------------------------------------
# _resolve_dataset
# ------------------------------------------------------------------


class TestResolveDataset:
    """Dataset resolution from schema parameter and default_dataset."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_schema_takes_precedence(self, _mock, _mock_sa):
        io = BigQueryIO(project="p", default_dataset="fallback", service_account_key=_SA_KEY)
        assert io._resolve_dataset("explicit") == "explicit"

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_falls_back_to_default_dataset(self, _mock, _mock_sa):
        io = BigQueryIO(project="p", default_dataset="fallback", service_account_key=_SA_KEY)
        assert io._resolve_dataset(None) == "fallback"

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_raises_when_both_none(self, _mock, _mock_sa):
        io = BigQueryIO(project="p", service_account_key=_SA_KEY)
        with pytest.raises(ConfigError, match="BigQueryIO requires a dataset"):
            io._resolve_dataset(None)

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_empty_string_schema_falls_back(self, _mock, _mock_sa):
        """Empty string is falsy, so it should fall back to default_dataset."""
        io = BigQueryIO(project="p", default_dataset="fallback", service_account_key=_SA_KEY)
        assert io._resolve_dataset("") == "fallback"


# ------------------------------------------------------------------
# _table_ref
# ------------------------------------------------------------------


class TestTableRef:
    """Fully-qualified BigQuery table reference construction."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_with_explicit_schema(self, _mock, _mock_sa):
        io = BigQueryIO(project="my-project", default_dataset="default_ds", service_account_key=_SA_KEY)
        assert io._table_ref("my_table", "explicit_ds") == "my-project.explicit_ds.my_table"

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_with_default_dataset(self, _mock, _mock_sa):
        io = BigQueryIO(project="my-project", default_dataset="default_ds", service_account_key=_SA_KEY)
        assert io._table_ref("my_table", None) == "my-project.default_ds.my_table"

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_raises_without_dataset(self, _mock, _mock_sa):
        io = BigQueryIO(project="my-project", service_account_key=_SA_KEY)
        with pytest.raises(ConfigError):
            io._table_ref("my_table", None)


# ------------------------------------------------------------------
# to_spec / roundtrip
# ------------------------------------------------------------------


class TestToSpec:
    """Serialization of BigQueryIO to ComponentInstanceSpec."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_minimal_spec(self, _mock, _mock_sa):
        io = BigQueryIO(project="proj", service_account_key=_SA_KEY)
        spec = io.to_spec()

        assert isinstance(spec, ComponentInstanceSpec)
        assert spec.path == "interloper_google_cloud.io.bigquery.BigQueryIO"
        assert spec.config["project"] == "proj"
        assert spec.config["location"] == "EU"
        assert spec.config["service_account_key"] == _SA_KEY
        assert "default_dataset" not in spec.config
        assert spec.config["write_disposition"] == "replace"
        assert spec.config["chunk_size"] == 1000

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_full_spec(self, _mock, _mock_sa):
        io = BigQueryIO(
            project="proj",
            default_dataset="ds",
            location="US",
            service_account_key=_SA_KEY,
            write_disposition=WriteDisposition.APPEND,
            chunk_size=2000,
        )
        spec = io.to_spec()

        assert spec.config["project"] == "proj"
        assert spec.config["default_dataset"] == "ds"
        assert spec.config["location"] == "US"
        assert spec.config["write_disposition"] == "append"
        assert spec.config["chunk_size"] == 2000

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_spec_roundtrip(self, mock_client_cls, _mock_sa):
        """to_spec() output can reconstruct an equivalent BigQueryIO."""
        original = BigQueryIO(
            project="roundtrip-proj",
            default_dataset="my_dataset",
            location="US",
            service_account_key=_SA_KEY,
            write_disposition=WriteDisposition.APPEND,
            chunk_size=750,
        )
        spec = original.to_spec()
        reconstructed = spec.reconstruct()

        assert isinstance(reconstructed, BigQueryIO)
        assert reconstructed.project == original.project
        assert reconstructed.default_dataset == original.default_dataset
        assert reconstructed.location == original.location
        assert reconstructed.write_disposition == original.write_disposition
        assert reconstructed.chunk_size == original.chunk_size


# ------------------------------------------------------------------
# _table_exists
# ------------------------------------------------------------------


class TestTableExists:
    """Table existence check delegates to the BQ client."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_returns_true_when_found(self, mock_client_cls, _mock_sa):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p", default_dataset="ds", service_account_key=_SA_KEY)

        assert io._table_exists("tbl", None) is True
        mock_client.get_table.assert_called_once_with("p.ds.tbl")

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_returns_false_when_not_found(self, mock_client_cls, _mock_sa):
        from google.cloud.exceptions import NotFound

        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("nope")
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p", default_dataset="ds", service_account_key=_SA_KEY)

        assert io._table_exists("tbl", None) is False


# ------------------------------------------------------------------
# dispose
# ------------------------------------------------------------------


class TestDispose:
    """Lifecycle: dispose closes the client."""

    @patch("interloper_google_cloud.io.bigquery.service_account")
    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_dispose_closes_client(self, mock_client_cls, _mock_sa):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p", service_account_key=_SA_KEY)
        io.dispose()
        mock_client.close.assert_called_once()
