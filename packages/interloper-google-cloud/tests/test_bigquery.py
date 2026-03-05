"""Tests for BigQueryIO."""

import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from interloper.errors import ConfigError
from interloper.io.database import WriteDisposition
from interloper.serialization.io import IOSpec

from interloper_google_cloud.io.bigquery import BigQueryIO, _bq_param_type, _infer_bq_type

# ------------------------------------------------------------------
# _infer_bq_type
# ------------------------------------------------------------------


class TestInferBqType:
    """Map Python values to BigQuery field types."""

    def test_bool(self):
        assert _infer_bq_type(True) == "BOOLEAN"
        assert _infer_bq_type(False) == "BOOLEAN"

    def test_int(self):
        assert _infer_bq_type(42) == "INTEGER"
        assert _infer_bq_type(0) == "INTEGER"
        assert _infer_bq_type(-1) == "INTEGER"

    def test_float(self):
        assert _infer_bq_type(3.14) == "FLOAT"
        assert _infer_bq_type(0.0) == "FLOAT"

    def test_decimal(self):
        assert _infer_bq_type(Decimal("9.99")) == "NUMERIC"

    def test_datetime(self):
        assert _infer_bq_type(datetime.datetime(2024, 1, 1, 12, 0)) == "TIMESTAMP"

    def test_date(self):
        assert _infer_bq_type(datetime.date(2024, 1, 1)) == "DATE"

    def test_bytes(self):
        assert _infer_bq_type(b"raw") == "BYTES"

    def test_string(self):
        assert _infer_bq_type("hello") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _infer_bq_type(None) == "STRING"

    def test_list_falls_back_to_string(self):
        assert _infer_bq_type([1, 2, 3]) == "STRING"

    def test_dict_falls_back_to_string(self):
        assert _infer_bq_type({"a": 1}) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        # True is also isinstance(True, int), so order matters
        assert _infer_bq_type(True) == "BOOLEAN"
        assert _infer_bq_type(True) != "INTEGER"


# ------------------------------------------------------------------
# _bq_param_type
# ------------------------------------------------------------------


class TestBqParamType:
    """Map Python values to BigQuery query parameter types."""

    def test_bool(self):
        assert _bq_param_type(True) == "BOOL"
        assert _bq_param_type(False) == "BOOL"

    def test_int(self):
        assert _bq_param_type(42) == "INT64"

    def test_float(self):
        assert _bq_param_type(3.14) == "FLOAT64"

    def test_decimal(self):
        assert _bq_param_type(Decimal("1.5")) == "NUMERIC"

    def test_datetime(self):
        assert _bq_param_type(datetime.datetime(2024, 6, 15, 8, 30)) == "TIMESTAMP"

    def test_date(self):
        assert _bq_param_type(datetime.date(2024, 6, 15)) == "DATE"

    def test_bytes(self):
        assert _bq_param_type(b"\x00") == "BYTES"

    def test_string(self):
        assert _bq_param_type("text") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _bq_param_type(None) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        assert _bq_param_type(True) == "BOOL"


# ------------------------------------------------------------------
# BigQueryIO.__init__
# ------------------------------------------------------------------


class TestBigQueryIOInit:
    """BigQueryIO constructor and attribute storage."""

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_default_params(self, mock_client_cls):
        io = BigQueryIO(project="my-project")
        assert io.project == "my-project"
        assert io.default_dataset is None
        assert io.location == "EU"
        assert io.write_disposition == WriteDisposition.REPLACE
        assert io.chunk_size == 1000
        mock_client_cls.assert_called_once_with(project="my-project", credentials=None, location="EU")

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_custom_params(self, mock_client_cls):
        io = BigQueryIO(
            project="other-project",
            default_dataset="analytics",
            location="US",
            write_disposition=WriteDisposition.APPEND,
            chunk_size=500,
        )
        assert io.project == "other-project"
        assert io.default_dataset == "analytics"
        assert io.location == "US"
        assert io.write_disposition == WriteDisposition.APPEND
        assert io.chunk_size == 500
        mock_client_cls.assert_called_once_with(project="other-project", credentials=None, location="US")

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_credentials_passed_through(self, mock_client_cls):
        creds = MagicMock()
        BigQueryIO(project="p", credentials=creds)
        mock_client_cls.assert_called_once_with(project="p", credentials=creds, location="EU")


# ------------------------------------------------------------------
# _resolve_dataset
# ------------------------------------------------------------------


class TestResolveDataset:
    """Dataset resolution from schema parameter and default_dataset."""

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_schema_takes_precedence(self, _mock):
        io = BigQueryIO(project="p", default_dataset="fallback")
        assert io._resolve_dataset("explicit") == "explicit"

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_falls_back_to_default_dataset(self, _mock):
        io = BigQueryIO(project="p", default_dataset="fallback")
        assert io._resolve_dataset(None) == "fallback"

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_raises_when_both_none(self, _mock):
        io = BigQueryIO(project="p")
        with pytest.raises(ConfigError, match="BigQueryIO requires a dataset"):
            io._resolve_dataset(None)

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_empty_string_schema_falls_back(self, _mock):
        """Empty string is falsy, so it should fall back to default_dataset."""
        io = BigQueryIO(project="p", default_dataset="fallback")
        assert io._resolve_dataset("") == "fallback"


# ------------------------------------------------------------------
# _table_ref
# ------------------------------------------------------------------


class TestTableRef:
    """Fully-qualified BigQuery table reference construction."""

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_with_explicit_schema(self, _mock):
        io = BigQueryIO(project="my-project", default_dataset="default_ds")
        assert io._table_ref("my_table", "explicit_ds") == "my-project.explicit_ds.my_table"

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_with_default_dataset(self, _mock):
        io = BigQueryIO(project="my-project", default_dataset="default_ds")
        assert io._table_ref("my_table", None) == "my-project.default_ds.my_table"

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_raises_without_dataset(self, _mock):
        io = BigQueryIO(project="my-project")
        with pytest.raises(ConfigError):
            io._table_ref("my_table", None)


# ------------------------------------------------------------------
# to_spec / roundtrip
# ------------------------------------------------------------------


class TestToSpec:
    """Serialization of BigQueryIO to IOSpec."""

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_minimal_spec(self, _mock):
        io = BigQueryIO(project="proj")
        spec = io.to_spec()

        assert isinstance(spec, IOSpec)
        assert spec.path == "interloper_google_cloud.io.bigquery.BigQueryIO"
        assert spec.init["project"] == "proj"
        assert spec.init["location"] == "EU"
        assert "default_dataset" not in spec.init
        assert spec.init["write_disposition"] == "replace"
        assert spec.init["chunk_size"] == 1000

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_full_spec(self, _mock):
        io = BigQueryIO(
            project="proj",
            default_dataset="ds",
            location="US",
            write_disposition=WriteDisposition.APPEND,
            chunk_size=2000,
        )
        spec = io.to_spec()

        assert spec.init["project"] == "proj"
        assert spec.init["default_dataset"] == "ds"
        assert spec.init["location"] == "US"
        assert spec.init["write_disposition"] == "append"
        assert spec.init["chunk_size"] == 2000

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_spec_roundtrip(self, mock_client_cls):
        """to_spec() output can reconstruct an equivalent BigQueryIO."""
        original = BigQueryIO(
            project="roundtrip-proj",
            default_dataset="my_dataset",
            location="US",
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

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_returns_true_when_found(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p", default_dataset="ds")

        assert io._table_exists("tbl", None) is True
        mock_client.get_table.assert_called_once_with("p.ds.tbl")

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_returns_false_when_not_found(self, mock_client_cls):
        from google.cloud.exceptions import NotFound

        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("nope")
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p", default_dataset="ds")

        assert io._table_exists("tbl", None) is False


# ------------------------------------------------------------------
# dispose
# ------------------------------------------------------------------


class TestDispose:
    """Lifecycle: dispose closes the client."""

    @patch("interloper_google_cloud.io.bigquery.bigquery.Client")
    def test_dispose_closes_client(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        io = BigQueryIO(project="p")
        io.dispose()
        mock_client.close.assert_called_once()
