"""Tests for SQLAlchemy type inference from Python values."""

import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, LargeBinary, Numeric, Text

from interloper_sql.io.base import _infer_sa_type


class TestInferSaType:
    """Map Python values to the expected SQLAlchemy column types."""

    def test_bool_returns_boolean(self):
        """True/False should map to Boolean."""
        assert isinstance(_infer_sa_type(True), Boolean)
        assert isinstance(_infer_sa_type(False), Boolean)

    def test_int_returns_biginteger(self):
        """Plain int should map to BigInteger."""
        assert isinstance(_infer_sa_type(0), BigInteger)
        assert isinstance(_infer_sa_type(42), BigInteger)
        assert isinstance(_infer_sa_type(-1), BigInteger)

    def test_float_returns_float(self):
        """Float should map to Float."""
        assert isinstance(_infer_sa_type(3.14), Float)
        assert isinstance(_infer_sa_type(0.0), Float)

    def test_decimal_returns_numeric(self):
        """Decimal should map to Numeric."""
        assert isinstance(_infer_sa_type(Decimal("10.50")), Numeric)

    def test_datetime_returns_datetime(self):
        """datetime.datetime should map to DateTime."""
        assert isinstance(_infer_sa_type(datetime.datetime(2025, 1, 1, 12, 0)), DateTime)

    def test_date_returns_date(self):
        """datetime.date should map to Date."""
        assert isinstance(_infer_sa_type(datetime.date(2025, 1, 1)), Date)

    def test_bytes_returns_largebinary(self):
        """bytes should map to LargeBinary."""
        assert isinstance(_infer_sa_type(b"hello"), LargeBinary)

    def test_str_returns_text(self):
        """str should map to Text."""
        assert isinstance(_infer_sa_type("hello"), Text)
        assert isinstance(_infer_sa_type(""), Text)

    def test_none_returns_text(self):
        """None (no other match) should fall through to Text."""
        assert isinstance(_infer_sa_type(None), Text)

    def test_list_returns_text(self):
        """Unsupported types should fall through to Text."""
        assert isinstance(_infer_sa_type([1, 2, 3]), Text)

    def test_dict_returns_text(self):
        """dict should fall through to Text."""
        assert isinstance(_infer_sa_type({"a": 1}), Text)

    def test_bool_before_int(self):
        """bool is a subclass of int; ensure Boolean wins over BigInteger."""
        result_true = _infer_sa_type(True)
        result_false = _infer_sa_type(False)
        assert isinstance(result_true, Boolean)
        assert not isinstance(result_true, BigInteger)
        assert isinstance(result_false, Boolean)
        assert not isinstance(result_false, BigInteger)
