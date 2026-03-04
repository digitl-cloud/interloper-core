"""Integration tests for the normalizer with assets and sources."""

import pytest
from pydantic import BaseModel

import interloper as il
from interloper.normalizer.base import Normalizer
from interloper.normalizer.strategy import MaterializationStrategy

# ---------------------------------------------------------------------------
# Asset + normalizer
# ---------------------------------------------------------------------------


class TestAssetWithNormalizer:
    """Tests for assets decorated with a normalizer."""

    def test_run_applies_normalizer(self):
        @il.asset(normalizer=Normalizer(infer=False))
        def my_asset():
            return [{"UserName": "alice"}, {"UserAge": 30}]

        asset = my_asset()
        result = asset.run()
        assert result == [
            {"user_name": "alice", "user_age": None},
            {"user_name": None, "user_age": 30},
        ]

    def test_run_without_normalizer_unchanged(self):
        @il.asset
        def my_asset():
            return [{"UserName": "alice"}]

        asset = my_asset()
        result = asset.run()
        assert result == [{"UserName": "alice"}]

    def test_normalizer_infers_schema(self):
        @il.asset(normalizer=Normalizer())
        def my_asset():
            return [{"name": "alice", "age": 30}]

        asset = my_asset()
        assert asset.schema is None
        asset.run()
        assert asset.schema is not None
        assert issubclass(asset.schema, BaseModel)
        assert "name" in asset.schema.model_fields
        assert "age" in asset.schema.model_fields

    def test_normalizer_validates_provided_schema(self):
        class Schema(BaseModel):
            name: str
            age: int | None = None

        @il.asset(schema=Schema, normalizer=Normalizer(normalize_columns=False, fill_missing=False))
        def my_asset():
            return [{"name": "alice", "age": 30}]

        asset = my_asset()
        result = asset.run()
        assert result == [{"name": "alice", "age": 30}]
        assert asset.schema is Schema

    def test_normalizer_with_flatten(self):
        @il.asset(normalizer=Normalizer(flatten_max_level=None, infer=False))
        def my_asset():
            return [{"user": {"name": "alice", "age": 30}}]

        asset = my_asset()
        result = asset.run()
        assert result == [{"user_name": "alice", "user_age": 30}]

    def test_materialize_applies_normalizer(self):
        @il.asset(normalizer=Normalizer(infer=False), io=il.MemoryIO())
        def my_asset():
            return [{"UserName": "alice"}]

        asset = my_asset()
        result = asset.materialize()
        assert result == [{"user_name": "alice"}]

    def test_infer_disabled_no_schema(self):
        @il.asset(normalizer=Normalizer(infer=False))
        def my_asset():
            return [{"a": 1}]

        asset = my_asset()
        asset.run()
        assert asset.schema is None


# ---------------------------------------------------------------------------
# Source-level normalizer
# ---------------------------------------------------------------------------


class TestSourceNormalizer:
    """Tests for source-level normalizer inheritance."""

    def test_source_normalizer_applied_to_assets(self):
        @il.source(normalizer=Normalizer(infer=False))
        class MySource:
            @il.asset
            def my_asset(self):
                return [{"UserName": "alice"}]

        src = MySource()
        result = src.my_asset.run()
        assert result == [{"user_name": "alice"}]

    def test_asset_normalizer_overrides_source(self):
        asset_normalizer = Normalizer(normalize_columns=False, fill_missing=False, infer=False)

        @il.source(normalizer=Normalizer(infer=False))
        class MySource:
            @il.asset(normalizer=asset_normalizer)
            def my_asset(self):
                return [{"UserName": "alice"}]

        src = MySource()
        result = src.my_asset.run()
        # Asset-level normalizer has normalize_columns=False, so keys stay as-is
        assert result == [{"UserName": "alice"}]

    def test_source_without_normalizer(self):
        @il.source
        class MySource:
            @il.asset
            def my_asset(self):
                return [{"UserName": "alice"}]

        src = MySource()
        result = src.my_asset.run()
        # No normalizer, raw data returned
        assert result == [{"UserName": "alice"}]


# ---------------------------------------------------------------------------
# MaterializationStrategy integration tests
# ---------------------------------------------------------------------------


class TestMaterializationStrategy:
    """Tests for MaterializationStrategy with assets and sources."""

    def test_auto_strategy_unchanged(self):
        """AUTO strategy produces the same result as before."""

        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.AUTO)
        def my_asset():
            return [{"UserName": "alice"}, {"UserAge": 30}]

        asset = my_asset()
        result = asset.run()
        assert result == [
            {"user_name": "alice", "user_age": None},
            {"user_name": None, "user_age": 30},
        ]

    def test_strict_requires_schema(self):
        """STRICT strategy raises ValueError when no schema is provided."""

        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.STRICT)
        def my_asset():
            return [{"a": 1}]

        asset = my_asset()
        with pytest.raises(ValueError, match="strategy='strict' requires a schema"):
            asset.run()

    def test_strict_valid_data_passes(self):
        """STRICT strategy with valid data passes."""

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

        asset = my_asset()
        result = asset.run()
        assert result == [{"name": "alice", "age": 30}]

    def test_strict_invalid_data_fails(self):
        """STRICT strategy with invalid data raises ValueError."""

        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": 123}]

        asset = my_asset()
        with pytest.raises(ValueError, match="Schema validation failed"):
            asset.run()

    def test_strict_rejects_extra_columns(self):
        """STRICT strategy fails when data has columns not in the schema."""

        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.STRICT,
        )
        def my_asset():
            return [{"name": "alice", "extra": "should_fail"}]

        asset = my_asset()
        with pytest.raises(ValueError, match="extra fields not in schema"):
            asset.run()

    def test_strict_rejects_missing_required_fields(self):
        """STRICT strategy fails when data is missing required schema fields."""

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

        asset = my_asset()
        with pytest.raises(ValueError, match="missing required fields"):
            asset.run()

    def test_strict_allows_missing_optional_fields(self):
        """STRICT strategy allows missing fields that have defaults."""

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

        asset = my_asset()
        result = asset.run()
        assert result == [{"name": "alice"}]

    def test_reconcile_requires_schema(self):
        """RECONCILE strategy raises ValueError when no schema is provided."""

        @il.asset(normalizer=Normalizer(infer=False), strategy=MaterializationStrategy.RECONCILE)
        def my_asset():
            return [{"a": 1}]

        asset = my_asset()
        with pytest.raises(ValueError, match="strategy='reconcile' requires a schema"):
            asset.run()

    def test_reconcile_coerces_types(self):
        """RECONCILE strategy coerces string values to int."""

        class Schema(BaseModel):
            value: int

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        def my_asset():
            return [{"value": "42"}]

        asset = my_asset()
        result = asset.run()
        assert result == [{"value": 42}]

    def test_reconcile_drops_extra_columns(self):
        """RECONCILE strategy drops columns not in the schema."""

        class Schema(BaseModel):
            name: str

        @il.asset(
            schema=Schema,
            normalizer=Normalizer(normalize_columns=False, fill_missing=False, infer=False),
            strategy=MaterializationStrategy.RECONCILE,
        )
        def my_asset():
            return [{"name": "alice", "extra": "drop_me"}]

        asset = my_asset()
        result = asset.run()
        assert result == [{"name": "alice"}]

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

        src = MySource()
        result = src.my_asset.run()
        assert result == [{"value": 99}]

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

        src = MySource()
        # Asset strategy is STRICT, so it validates but doesn't reconcile
        result = src.my_asset.run()
        assert result == [{"name": "alice"}]

    def test_no_strategy_uses_auto_behavior(self):
        """When no strategy is set, AUTO behavior applies (infer schema)."""

        @il.asset(normalizer=Normalizer())
        def my_asset():
            return [{"name": "alice", "age": 30}]

        asset = my_asset()
        assert asset.strategy is None
        result = asset.run()
        assert result == [{"name": "alice", "age": 30}]
        # AUTO behavior: schema should be inferred
        assert asset.schema is not None

    def test_strategy_exported_from_top_level(self):
        """MaterializationStrategy is accessible from interloper top-level."""
        assert hasattr(il, "MaterializationStrategy")
        assert il.MaterializationStrategy.AUTO == "auto"
        assert il.MaterializationStrategy.STRICT == "strict"
        assert il.MaterializationStrategy.RECONCILE == "reconcile"
