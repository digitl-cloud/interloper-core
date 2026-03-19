"""BigQuery destination implementation."""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from typing import Any

import google.auth
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from interloper.destination.context import DestinationContext
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError, TableNotFoundError
from pydantic import PrivateAttr


class BigQueryDestination(DatabaseDestination):
    """BigQuery destination manager.

    Provides read and write access to Google BigQuery tables.  Uses the
    ``google-cloud-bigquery`` client directly (no SQLAlchemy).

    The BigQuery *dataset* is resolved from the asset's ``dataset`` attribute
    (i.e. the schema parameter in :class:`DatabaseDestination` hooks).  If the
    asset has no ``dataset``, the ``default_dataset`` from the config is used as
    a fallback.
    """

    project: str
    default_dataset: str | None = None
    location: str = "EU"
    service_account_key: str | None = None
    _client: Any = PrivateAttr(default=None)

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)

        if self.service_account_key is not None:
            key_info = json.loads(self.service_account_key)
            credentials = service_account.Credentials.from_service_account_info(key_info)
        else:
            credentials, _ = google.auth.default()

        self._client = bigquery.Client(
            project=self.project,
            credentials=credentials,
            location=self.location,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_dataset(self, schema: str | None) -> str:
        """Return the BigQuery dataset to use.

        Prefers ``schema`` (from the asset's ``dataset``).  Falls back to
        the config's ``default_dataset``.

        Args:
            schema: Schema parameter from the asset context.

        Returns:
            The resolved dataset name.

        Raises:
            ConfigError: If neither *schema* nor *default_dataset* is set.
        """
        dataset = schema or self.default_dataset
        if dataset is None:
            raise ConfigError(
                "BigQueryDestination requires a dataset. Either set 'dataset' on the asset "
                "or provide 'default_dataset' when constructing BigQueryDestination."
            )
        return dataset

    def _table_ref(self, table: str, schema: str | None) -> str:
        """Build a fully-qualified BigQuery table reference.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            ``project.dataset.table`` string.
        """
        dataset = self._resolve_dataset(schema)
        return f"{self.project}.{dataset}.{table}"

    def _table_exists(self, table: str, schema: str | None) -> bool:
        """Check whether a BigQuery table exists.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.
        """
        try:
            self._client.get_table(self._table_ref(table, schema))
        except NotFound:
            return False
        return True

    def _create_table(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Create a BigQuery table from sample row data.

        Column types are inferred from the Python values in the first row
        using :func:`_infer_bq_type`.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            rows: Row data (at least one row required for schema inference).
        """
        sample = rows[0]
        bq_schema = [bigquery.SchemaField(name, _py_to_bq_type(value)) for name, value in sample.items()]
        bq_table = bigquery.Table(self._table_ref(table, schema), schema=bq_schema)
        self._client.create_table(bq_table)

    def _ensure_dataset(self, schema: str | None) -> None:
        """Create the BigQuery dataset if it does not already exist.

        Args:
            schema: Schema (dataset) override.
        """
        dataset = self._resolve_dataset(schema)
        dataset_ref = bigquery.DatasetReference(self.project, dataset)
        try:
            self._client.get_dataset(dataset_ref)
        except NotFound:
            bq_dataset = bigquery.Dataset(dataset_ref)
            bq_dataset.location = self.location
            self._client.create_dataset(bq_dataset)

    # ------------------------------------------------------------------
    # DatabaseDestination hooks
    # ------------------------------------------------------------------

    def _insert(self, context: DestinationContext, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into BigQuery using a load job.

        If the table does not exist yet, the dataset is ensured and the table is
        created from the row data before loading.

        Args:
            context: Destination context with asset and partition information.
            table: Target table name.
            schema: Database schema (dataset).
            rows: Row data as list of dicts.
        """
        if not self._table_exists(table, schema):
            self._ensure_dataset(schema)
            self._create_table(table, schema, rows)

        ref = self._table_ref(table, schema)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        # Serialize non-JSON-native types (date, datetime, Decimal) before
        # passing to load_table_from_json, which calls json.dumps internally.
        safe_rows = [json.loads(json.dumps(row, default=_json_default)) for row in rows]

        context.logger.info(f"Inserting {len(safe_rows)} rows into table `{ref}`")
        job = self._client.load_table_from_json(safe_rows, ref, job_config=job_config)
        job.result()

    def _delete_all(self, context: DestinationContext, table: str, schema: str | None) -> None:
        """Truncate all rows from the BigQuery table.

        No-op when the table does not exist yet.

        Args:
            context: Destination context with asset and partition information.
            table: Target table name.
            schema: Database schema (dataset).
        """
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)

        context.logger.info(f"Truncating all rows from table `{ref}`")
        self._client.query(f"TRUNCATE TABLE `{ref}`").result()

    def _delete_partition(
        self,
        context: DestinationContext,
        table: str,
        schema: str | None,
        column: str,
        value: Any,
    ) -> None:
        """Delete rows matching a partition value.

        No-op when the table does not exist yet.

        Args:
            context: Destination context with asset and partition information.
            table: Target table name.
            schema: Database schema (dataset).
            column: Partition column name.
            value: Partition value to match.
        """
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)
        query = f"DELETE FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_to_py_type(value), value)],
        )
        context.logger.info(f"Deleting rows matching partition value `{value}` from table `{ref}`")
        self._client.query(query, job_config=job_config).result()

    def _select_all(self, context: DestinationContext, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the BigQuery table.

        Args:
            context: Destination context with asset and partition information.
            table: Target table name.
            schema: Database schema (dataset).

        Returns:
            All rows as list of dicts.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise TableNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)

        context.logger.info(f"Selecting all rows from table `{ref}`")
        rows = self._client.query(f"SELECT * FROM `{ref}`").result()
        return [dict(row) for row in rows]

    def _select_partition(
        self,
        context: DestinationContext,
        table: str,
        schema: str | None,
        column: str,
        value: Any,
    ) -> list[dict[str, Any]]:
        """Select rows matching a partition value.

        Args:
            context: Destination context with asset and partition information.
            table: Target table name.
            schema: Database schema (dataset).
            column: Partition column name.
            value: Partition value to match.

        Returns:
            Matching rows as list of dicts.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise TableNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        query = f"SELECT * FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_to_py_type(value), value)],
        )

        context.logger.info(f"Selecting rows matching partition value `{value}` from table `{ref}`")
        rows = self._client.query(query, job_config=job_config).result()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def _count_by_partition(
        self,
        context: DestinationContext,
        table: str,
        schema: str | None,
        column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by partition column via BigQuery SQL.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            column: Column to group by.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            ref = self._table_ref(table, schema)
            raise TableNotFoundError(f"Table '{ref}' does not exist. Has the asset been materialized?")

        ref = self._table_ref(table, schema)
        query = f"SELECT CAST(`{column}` AS STRING) AS partition_value, COUNT(*) AS cnt FROM `{ref}` GROUP BY 1"
        rows = self._client.query(query).result()
        return {row["partition_value"]: row["cnt"] for row in rows}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        """Close the BigQuery client."""
        if self._client:
            self._client.close()


def _json_default(o: Any) -> Any:
    """JSON serializer for types not handled by the default encoder.

    Used by :meth:`BigQueryDestination._insert` to convert rows to JSON-safe dicts
    before passing them to ``load_table_from_json``.

    Args:
        o: Object to serialize.

    Returns:
        A JSON-serializable representation.

    Raises:
        TypeError: If the object type is not supported.
    """
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _py_to_bq_type(value: Any) -> str:
    """Infer a BigQuery field type from a Python value.

    Args:
        value: A sample Python value used to determine the field type.

    Returns:
        A BigQuery standard SQL type name.
    """
    import datetime
    from decimal import Decimal

    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, bytes):
        return "BYTES"
    return "STRING"


def _bq_to_py_type(value: Any) -> str:
    """Map a Python value to a BigQuery query parameter type.

    Args:
        value: A Python value.

    Returns:
        BigQuery parameter type string.
    """
    import datetime
    from decimal import Decimal

    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, bytes):
        return "BYTES"
    return "STRING"
