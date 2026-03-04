"""BigQuery IO implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from interloper.errors import ConfigError, TableNotFoundError
from interloper.io.database import DatabaseIO, WriteDisposition
from interloper.serialization.io import IOSpec

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter


def _infer_bq_type(value: Any) -> str:
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


class BigQueryIO(DatabaseIO):
    """BigQuery IO manager.

    Provides read and write access to Google BigQuery tables.  Uses the
    ``google-cloud-bigquery`` client directly (no SQLAlchemy).

    The BigQuery *dataset* is resolved from the asset's ``dataset`` attribute
    (i.e. the schema parameter in :class:`DatabaseIO` hooks).  If the asset has
    no ``dataset``, the ``default_dataset`` constructor argument is used as a
    fallback.

    Args:
        project: Google Cloud project ID.
        default_dataset: Fallback BigQuery dataset when the asset has no
            ``dataset`` attribute.  At least one of the asset's ``dataset`` or
            this parameter must be set.
        location: BigQuery location (e.g. ``"US"``, ``"EU"``).
        credentials: Optional Google credentials object.  When *None*, the
            default application credentials are used.
        write_disposition: Controls whether existing rows are deleted before
            writing.  Defaults to :attr:`WriteDisposition.REPLACE`.
        chunk_size: Number of rows per insert batch.
        adapter: Optional data adapter for type conversion.
    """

    def __init__(
        self,
        project: str,
        default_dataset: str | None = None,
        location: str = "EU",
        credentials: Any = None,
        write_disposition: WriteDisposition = WriteDisposition.REPLACE,
        chunk_size: int = 1000,
        adapter: DataAdapter | str | None = None,
    ) -> None:
        super().__init__(write_disposition, chunk_size, adapter)
        self.project = project
        self.default_dataset = default_dataset
        self.location = location
        self._client = bigquery.Client(project=project, credentials=credentials, location=location)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_dataset(self, schema: str | None) -> str:
        """Return the BigQuery dataset to use.

        Prefers ``schema`` (from the asset's ``dataset``).  Falls back to
        :attr:`default_dataset`.

        Args:
            schema: Schema parameter from the asset context.

        Returns:
            The resolved dataset name.

        Raises:
            ValueError: If neither *schema* nor *default_dataset* is set.
        """
        dataset = schema or self.default_dataset
        if dataset is None:
            raise ConfigError(
                "BigQueryIO requires a dataset. Either set 'dataset' on the asset "
                "or provide 'default_dataset' to BigQueryIO."
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
        bq_schema = [bigquery.SchemaField(name, _infer_bq_type(value)) for name, value in sample.items()]
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
    # DatabaseIO hooks
    # ------------------------------------------------------------------

    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into BigQuery using a load job.

        If the table does not exist yet, the dataset is ensured and the table is
        created from the row data before loading.

        Args:
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
        job = self._client.load_table_from_json(rows, ref, job_config=job_config)
        job.result()  # Wait for completion

    def _delete_all(self, table: str, schema: str | None) -> None:
        """Truncate all rows from the BigQuery table.

        No-op when the table does not exist yet.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
        """
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)
        self._client.query(f"TRUNCATE TABLE `{ref}`").result()

    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a partition value.

        No-op when the table does not exist yet.

        Args:
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
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_param_type(value), value)],
        )
        self._client.query(query, job_config=job_config).result()

    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the BigQuery table.

        Args:
            table: Target table name.
            schema: Database schema (dataset).

        Returns:
            All rows as list of dicts.

        Raises:
            ValueError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise TableNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        rows = self._client.query(f"SELECT * FROM `{ref}`").result()
        return [dict(row) for row in rows]

    def _select_partition(self, table: str, schema: str | None, column: str, value: Any) -> list[dict[str, Any]]:
        """Select rows matching a partition value.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            column: Partition column name.
            value: Partition value to match.

        Returns:
            Matching rows as list of dicts.

        Raises:
            ValueError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise TableNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        query = f"SELECT * FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_param_type(value), value)],
        )
        rows = self._client.query(query, job_config=job_config).result()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_spec(self) -> IOSpec:
        """Convert to serializable spec."""
        init = self._base_init_kwargs()
        init["project"] = self.project
        if self.default_dataset is not None:
            init["default_dataset"] = self.default_dataset
        init["location"] = self.location
        return IOSpec(path=self.path, init=init)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        """Close the BigQuery client."""
        self._client.close()


def _bq_param_type(value: Any) -> str:
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
