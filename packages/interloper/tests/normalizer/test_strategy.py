"""Tests for MaterializationStrategy with assets and sources."""

import pytest
from pydantic import BaseModel

import interloper as il
from interloper.errors import AssetError, SchemaError
from interloper.normalizer.base import Normalizer
from interloper.normalizer.strategy import MaterializationStrategy


class TestMaterializationStrategy:
    """Tests for MaterializationStrategy with assets and sources."""

    def test_auto_strategy_unchanged(self):
        """AUTO strategy produces the same result as default normalizer."""
        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.AUTO)
        def my_asset():
            return [{"UserName": "alice"}, {"UserAge": 30}]

        result = my_asset().run()
        assert result == [
            {"user_name": "alice", "user_age": None},
            {"user_name": None, "user_age": 30},
        ]

    def test_strict_requires_schema(self):
        """STRICT strategy raises when no schema is provided."""
        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.STRICT)
        def my_asset():
            return [{"a": 1}]

        with pytest.raises(AssetError, match="strategy='strict' requires a schema"):
            my_asset().run()

    def test_strict_valid_data_passes(self):
        """STRICT strategy accepts valid data."""
        class Schema(BaseModel):
            name: str
            age: int | None = None

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": "alice", "age": 30}]

        assert my_asset().run() == [{"name": "alice", "age": 30}]

    def test_strict_invalid_data_fails(self):
        """STRICT strategy rejects invalid data."""
        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": 123}]

        with pytest.raises(SchemaError, match="Schema validation failed"):
            my_asset().run()

    def test_strict_rejects_extra_columns(self):
        """STRICT strategy fails on extra columns."""
        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": "alice", "extra": "should_fail"}]

        with pytest.raises(SchemaError, match="extra fields not in schema"):
            my_asset().run()

    def test_strict_rejects_missing_required_fields(self):
        """STRICT strategy fails on missing required fields."""
        class Schema(BaseModel):
            name: str
            age: int

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": "alice"}]

        with pytest.raises(SchemaError, match="missing required fields"):
            my_asset().run()

    def test_strict_allows_missing_optional_fields(self):
        """STRICT strategy allows missing optional fields."""
        class Schema(BaseModel):
            name: str
            age: int | None = None

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": "alice"}]

        assert my_asset().run() == [{"name": "alice"}]

    def test_reconcile_requires_schema(self):
        """RECONCILE strategy raises when no schema is provided."""
        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.RECONCILE)
        def my_asset():
            return [{"a": 1}]

        with pytest.raises(AssetError, match="strategy='reconcile' requires a schema"):
            my_asset().run()

    def test_reconcile_coerces_types(self):
        """RECONCILE strategy coerces types."""
        class Schema(BaseModel):
            value: int

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        def my_asset():
            return [{"value": "42"}]

        assert my_asset().run() == [{"value": 42}]

    def test_reconcile_drops_extra_columns(self):
        """RECONCILE strategy drops extra columns."""
        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        def my_asset():
            return [{"name": "alice", "extra": "drop_me"}]

        assert my_asset().run() == [{"name": "alice"}]

    def test_source_strategy_propagation(self):
        """Source-level strategy is inherited by assets."""
        class Schema(BaseModel):
            value: int

        @il.source(
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        class MySource:
            @il.asset(schema=Schema)
            def my_asset(self):
                return [{"value": "99"}]

        assert MySource().my_asset.run() == [{"value": 99}]

    def test_asset_strategy_overrides_source(self):
        """Asset-level strategy overrides source-level strategy."""
        class Schema(BaseModel):
            name: str

        @il.source(
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        class MySource:
            @il.asset(schema=Schema, strategy=MaterializationStrategy.STRICT)
            def my_asset(self):
                return [{"name": "alice"}]

        assert MySource().my_asset.run() == [{"name": "alice"}]

    def test_no_strategy_uses_auto_behavior(self):
        """No strategy defaults to AUTO behavior with schema inference."""
        @il.asset(normalizer=Normalizer())
        def my_asset():
            return [{"name": "alice", "age": 30}]

        asset = my_asset()
        assert asset.strategy is None
        result = asset.run()
        assert result == [{"name": "alice", "age": 30}]
        assert asset.schema is not None

    def test_strategy_exported_from_top_level(self):
        """MaterializationStrategy is accessible from the top-level module."""
        assert hasattr(il, "MaterializationStrategy")
        assert il.MaterializationStrategy.AUTO == "auto"
        assert il.MaterializationStrategy.STRICT == "strict"
        assert il.MaterializationStrategy.RECONCILE == "reconcile"
