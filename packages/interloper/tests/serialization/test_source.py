"""Tests for SourceSpec serialization and reconstruction."""

import pytest

import interloper as il
from interloper.serialization.io import IOSpec
from interloper.serialization.source import SourceSpec
from interloper.source.base import Source


# Module-level source definition for reconstruction tests.
@il.source
class TestSerSource:
    """Minimal source with two assets for serialization tests."""

    @il.asset
    def asset_a(self, context: il.ExecutionContext) -> list[dict]:
        return [{"v": 1}]

    @il.asset
    def asset_b(self, context: il.ExecutionContext) -> list[dict]:
        return [{"v": 2}]


class TestSourceSpec:
    """Test SourceSpec creation, serialization, reconstruction, and asset filtering."""

    @pytest.fixture()
    def source_path(self) -> str:
        """Dotted import path to the module-level TestSerSource.

        Returns:
            Import path string fixture.
        """
        return TestSerSource.path

    @pytest.fixture()
    def spec(self, source_path: str) -> SourceSpec:
        """A SourceSpec pointing at TestSerSource.

        Returns:
            SourceSpec fixture.
        """
        return SourceSpec(path=source_path)

    # -- type field ----------------------------------------------------------

    def test_type_field_is_source(self, spec: SourceSpec):
        """The type discriminator is always 'source'."""
        assert spec.type == "source"

    def test_type_field_in_json(self, spec: SourceSpec):
        """Type field is included in JSON output."""
        data = spec.model_dump()
        assert data["type"] == "source"

    # -- creation ------------------------------------------------------------

    def test_creation_with_path(self, spec: SourceSpec, source_path: str):
        """SourceSpec stores the import path."""
        assert spec.path == source_path

    def test_optional_fields_default_to_none(self, spec: SourceSpec):
        """io, config, and assets default to None."""
        assert spec.io is None
        assert spec.config is None
        assert spec.assets is None

    # -- JSON roundtrip ------------------------------------------------------

    def test_json_roundtrip(self, spec: SourceSpec):
        """SourceSpec survives JSON serialization and deserialization."""
        json_str = spec.model_dump_json()
        parsed = SourceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.type == "source"
        assert parsed.io is None
        assert parsed.config is None
        assert parsed.assets is None

    def test_json_roundtrip_with_assets(self, source_path: str):
        """SourceSpec with an assets list survives JSON roundtrip."""
        spec = SourceSpec(path=source_path, assets=["asset_a"])
        json_str = spec.model_dump_json()
        parsed = SourceSpec.model_validate_json(json_str)
        assert parsed.assets == ["asset_a"]

    # -- reconstruct ---------------------------------------------------------

    def test_reconstruct_creates_source(self, spec: SourceSpec):
        """reconstruct() produces a Source with the expected assets."""
        source = spec.reconstruct()
        assert isinstance(source, Source)
        assert "asset_a" in source.assets
        assert "asset_b" in source.assets

    # -- assets filtering ----------------------------------------------------

    def test_assets_filtering_materializable(self, source_path: str):
        """Only listed assets are marked materializable when assets is set."""
        spec = SourceSpec(path=source_path, assets=["asset_a"])
        source = spec.reconstruct()
        assert source.assets["asset_a"].materializable is True
        assert source.assets["asset_b"].materializable is False

    def test_assets_filtering_all(self, source_path: str):
        """All assets are materializable when assets is None."""
        spec = SourceSpec(path=source_path)
        source = spec.reconstruct()
        assert source.assets["asset_a"].materializable is True
        assert source.assets["asset_b"].materializable is True

    # -- _reconstruct_io -----------------------------------------------------

    def test_reconstruct_io_none(self, spec: SourceSpec):
        """_reconstruct_io returns None when io is None."""
        assert spec._reconstruct_io(None) is None

    def test_reconstruct_io_single(self, spec: SourceSpec):
        """_reconstruct_io reconstructs a single IOSpec."""
        io_spec = IOSpec(path="interloper.io.memory.MemoryIO")
        result = spec._reconstruct_io(io_spec)
        assert isinstance(result, il.MemoryIO)

    def test_reconstruct_io_dict(self, spec: SourceSpec):
        """_reconstruct_io reconstructs a dict of IOSpecs."""
        io_specs = {
            "a": IOSpec(path="interloper.io.memory.MemoryIO"),
            "b": IOSpec(path="interloper.io.memory.MemoryIO"),
        }
        result = spec._reconstruct_io(io_specs)
        assert isinstance(result, dict)
        assert "a" in result
        assert "b" in result
        assert isinstance(result["a"], il.MemoryIO)
        assert isinstance(result["b"], il.MemoryIO)
