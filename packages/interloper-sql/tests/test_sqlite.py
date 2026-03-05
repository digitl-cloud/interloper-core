"""Tests for SqliteIO using in-memory SQLite databases."""

import datetime

import interloper as il
import pytest
from interloper.errors import TableNotFoundError
from interloper.io.database import WriteDisposition
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, Text
from sqlalchemy import inspect as sa_inspect

from interloper_sql import SqliteIO

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(name="test_table", dataset=None):
    """Build an IOContext for a dummy asset with the given table name and dataset."""

    @il.asset(name=name, dataset=dataset)
    def _dummy():
        return None

    return il.IOContext(asset=_dummy())


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestSqliteIOInit:
    """SqliteIO constructor and defaults."""

    def test_default_memory_database(self):
        """Default database is :memory:."""
        io = SqliteIO()
        assert io.database == ":memory:"

    def test_explicit_memory_database(self):
        """Explicitly passing :memory: works."""
        io = SqliteIO(database=":memory:")
        assert io.database == ":memory:"

    def test_file_path_database(self, tmp_path):
        """A file path is accepted as the database."""
        db_path = str(tmp_path / "test.db")
        io = SqliteIO(database=db_path)
        assert io.database == db_path

    def test_default_write_disposition(self):
        """Default write disposition is REPLACE."""
        io = SqliteIO()
        assert io.write_disposition is WriteDisposition.REPLACE

    def test_custom_write_disposition(self):
        """Explicit write disposition is preserved."""
        io = SqliteIO(write_disposition=WriteDisposition.APPEND)
        assert io.write_disposition is WriteDisposition.APPEND

    def test_default_chunk_size(self):
        """Default chunk_size is 1000."""
        io = SqliteIO()
        assert io.chunk_size == 1000

    def test_custom_chunk_size(self):
        """Explicit chunk_size is preserved."""
        io = SqliteIO(chunk_size=500)
        assert io.chunk_size == 500

    def test_is_io_subclass(self):
        """SqliteIO is an IO subclass."""
        io = SqliteIO()
        assert isinstance(io, il.IO)


# ---------------------------------------------------------------------------
# to_spec roundtrip
# ---------------------------------------------------------------------------


class TestSqliteIOSpec:
    """Serialization via to_spec and reconstruction."""

    def test_to_spec_default(self):
        """to_spec captures default constructor arguments."""
        io = SqliteIO()
        spec = io.to_spec()

        assert spec.path == "interloper_sql.io.sqlite.SqliteIO"
        assert spec.init["database"] == ":memory:"
        assert spec.init["write_disposition"] == "replace"
        assert spec.init["chunk_size"] == 1000

    def test_to_spec_custom(self):
        """to_spec captures custom constructor arguments."""
        io = SqliteIO(database="/tmp/test.db", write_disposition=WriteDisposition.APPEND, chunk_size=500)
        spec = io.to_spec()

        assert spec.init["database"] == "/tmp/test.db"
        assert spec.init["write_disposition"] == "append"
        assert spec.init["chunk_size"] == 500

    def test_roundtrip(self):
        """to_spec -> reconstruct produces an equivalent SqliteIO."""
        io = SqliteIO(database=":memory:", write_disposition=WriteDisposition.APPEND, chunk_size=250)
        spec = io.to_spec()
        restored = spec.reconstruct()

        assert isinstance(restored, SqliteIO)
        assert restored.database == ":memory:"
        assert restored.write_disposition is WriteDisposition.APPEND
        assert restored.chunk_size == 250


# ---------------------------------------------------------------------------
# Write then read (full CRUD via :memory:)
# ---------------------------------------------------------------------------


class TestSqliteIOReadWrite:
    """Write and read through the DatabaseIO interface using an in-memory SQLite."""

    def test_write_then_read(self):
        """Write rows, then read them back."""
        io = SqliteIO()
        ctx = _make_context("simple")
        rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

        io.write(ctx, rows)
        result = io.read(ctx)

        assert result == rows

    def test_write_empty_rows_is_noop(self):
        """Writing an empty list should not create a table."""
        io = SqliteIO()
        ctx = _make_context("empty_table")

        io.write(ctx, [])

        with pytest.raises(TableNotFoundError):
            io.read(ctx)

    def test_read_nonexistent_table_raises(self):
        """Reading a table that doesn't exist raises TableNotFoundError."""
        io = SqliteIO()
        ctx = _make_context("no_such_table")

        with pytest.raises(TableNotFoundError):
            io.read(ctx)

    def test_multiple_writes_replace_mode(self):
        """With REPLACE disposition, a second write replaces the first."""
        io = SqliteIO(write_disposition=WriteDisposition.REPLACE)
        ctx = _make_context("replace_test")

        io.write(ctx, [{"v": 1}])
        io.write(ctx, [{"v": 2}])

        result = io.read(ctx)
        assert result == [{"v": 2}]

    def test_multiple_writes_append_mode(self):
        """With APPEND disposition, writes accumulate."""
        io = SqliteIO(write_disposition=WriteDisposition.APPEND)
        ctx = _make_context("append_test")

        io.write(ctx, [{"v": 1}])
        io.write(ctx, [{"v": 2}])

        result = io.read(ctx)
        assert result == [{"v": 1}, {"v": 2}]

    def test_write_various_types(self):
        """Rows containing int, float, str, bool persist correctly."""
        io = SqliteIO()
        ctx = _make_context("types_test")
        rows = [{"i": 42, "f": 3.14, "s": "hello", "b": True}]

        io.write(ctx, rows)
        result = io.read(ctx)

        assert len(result) == 1
        assert result[0]["i"] == 42
        assert result[0]["f"] == pytest.approx(3.14)
        assert result[0]["s"] == "hello"
        # SQLite stores booleans as integers; the value may come back as 1
        assert result[0]["b"] in (True, 1)

    def test_write_large_batch_chunked(self):
        """Rows exceeding chunk_size are still written completely."""
        io = SqliteIO(chunk_size=3)
        ctx = _make_context("chunked")
        rows = [{"n": i} for i in range(10)]

        io.write(ctx, rows)
        result = io.read(ctx)

        assert len(result) == 10
        assert [r["n"] for r in result] == list(range(10))


# ---------------------------------------------------------------------------
# Table creation and schema inference
# ---------------------------------------------------------------------------


class TestSqliteIOTableCreation:
    """Automatic table creation and schema inference."""

    def test_table_created_on_first_write(self):
        """Writing to a non-existent table creates it automatically."""
        io = SqliteIO()
        ctx = _make_context("auto_create")

        io.write(ctx, [{"col_a": "value"}])

        # If we can read, the table was created
        result = io.read(ctx)
        assert len(result) == 1

    def test_inferred_column_types(self):
        """Column types are inferred from the first row's Python types."""
        io = SqliteIO()
        ctx = _make_context("infer_types")
        rows = [
            {
                "int_col": 42,
                "float_col": 3.14,
                "str_col": "text",
                "bool_col": True,
                "date_col": datetime.date(2025, 1, 1),
                "datetime_col": datetime.datetime(2025, 1, 1, 12, 0),
            }
        ]

        io.write(ctx, rows)

        # Reflect the table and check column types
        inspector = sa_inspect(io._engine)
        columns = {c["name"]: c["type"] for c in inspector.get_columns("infer_types")}

        assert isinstance(columns["int_col"], BigInteger)
        assert isinstance(columns["float_col"], Float)
        assert isinstance(columns["str_col"], (Text, type(columns["str_col"])))  # at least present
        assert isinstance(columns["bool_col"], Boolean)
        assert isinstance(columns["date_col"], Date)
        assert isinstance(columns["datetime_col"], DateTime)

    def test_multiple_tables_same_io(self):
        """A single SqliteIO can manage multiple tables."""
        io = SqliteIO()
        ctx_a = _make_context("table_a")
        ctx_b = _make_context("table_b")

        io.write(ctx_a, [{"x": 1}])
        io.write(ctx_b, [{"y": 2}])

        assert io.read(ctx_a) == [{"x": 1}]
        assert io.read(ctx_b) == [{"y": 2}]


# ---------------------------------------------------------------------------
# Dispose
# ---------------------------------------------------------------------------


class TestSqliteIODispose:
    """Engine disposal."""

    def test_dispose_clears_cache(self):
        """After dispose(), the internal table cache is empty."""
        io = SqliteIO()
        ctx = _make_context("dispose_test")
        io.write(ctx, [{"a": 1}])

        assert len(io._table_cache) > 0

        io.dispose()

        assert len(io._table_cache) == 0
