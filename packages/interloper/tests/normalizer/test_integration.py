"""Integration tests for the normalizer with assets and sources."""

from pydantic import BaseModel

import interloper as il
from interloper.normalizer.base import Normalizer

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
