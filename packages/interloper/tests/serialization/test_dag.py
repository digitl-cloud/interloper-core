"""Tests for DAGSpec serialization and reconstruction."""

import json
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization import AssetSpec, DAGSpec


class TestDAGSpec:
    """Test DAGSpec serialization and reconstruction."""

    def test_dagspec_roundtrip(self):
        """Test DAGSpec roundtrip."""

        # Create a simple DAG
        @il.asset
        def asset_a():
            return "a"

        @il.asset
        def asset_b(asset_a):
            return f"b_{asset_a}"

        dag = il.DAG(asset_a(), asset_b())

        # Convert to spec
        spec = dag.to_spec()
        assert isinstance(spec, DAGSpec)
        assert len(spec.assets) == 2
        assert all(isinstance(asset_spec, AssetSpec) for asset_spec in spec.assets)

        # Test JSON serialization
        json_str = spec.model_dump_json()
        parsed_spec = DAGSpec.model_validate_json(json_str)
        assert len(parsed_spec.assets) == 2
        assert all(isinstance(asset_spec, AssetSpec) for asset_spec in parsed_spec.assets)

    def test_dagspec_validation(self):
        """Test DAGSpec Pydantic validation."""
        # Valid spec
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        spec = DAGSpec(assets=[asset_spec])
        assert len(spec.assets) == 1

        # Invalid asset_specs
        with pytest.raises(ValidationError):
            DAGSpec(assets="not_a_list")  # type: ignore[arg-type]

        # Missing required field
        with pytest.raises(ValidationError):
            DAGSpec()  # type: ignore[call-arg]

    def test_dagspec_json_serialization(self):
        """Test DAGSpec JSON serialization."""
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        spec = DAGSpec(assets=[asset_spec])

        # Convert to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Parse from JSON
        parsed = DAGSpec.model_validate_json(json_str)
        assert len(parsed.assets) == 1
        assert parsed.assets[0].path == asset_spec.path

    def test_complex_dag_serialization(self):
        """Complex DAG with multiple assets serializes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_io = il.FileIO(base_path=temp_dir)

            @il.asset
            def source_asset():
                return "source_data"

            @il.asset
            def processed_asset(source_asset):
                return f"processed_{source_asset}"

            @il.asset
            def final_asset(processed_asset):
                return f"final_{processed_asset}"

            dag = il.DAG(source_asset(io=file_io), processed_asset(io=file_io), final_asset(io=file_io))
            dag_spec = dag.to_spec()
            assert isinstance(dag_spec, DAGSpec)
            assert len(dag_spec.assets) == 3

            json_str = dag_spec.model_dump_json()
            parsed_spec = DAGSpec.model_validate_json(json_str)
            assert len(parsed_spec.assets) == 3

    def test_spec_persistence(self):
        """DAGSpec can be saved and loaded from a JSON file."""
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        dag_spec = DAGSpec(assets=[asset_spec])

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(dag_spec.model_dump_json())
            temp_file = f.name

        try:
            with open(temp_file) as f:
                loaded_data = json.load(f)
            loaded_spec = DAGSpec.model_validate(loaded_data)
            assert len(loaded_spec.assets) == 1
            assert loaded_spec.assets[0].path == "myapp.assets.my_asset"
        finally:
            Path(temp_file).unlink()

