"""Test serialization integration scenarios."""

import json
import tempfile
from pathlib import Path

import interloper as il
from interloper.serialization import AssetSpec, DAGSpec

# Helper source with a closure for testing top-level import
_closed_over_value = "secret_value"


@il.source(dataset="closure_source")
def my_source():
    """A source defined at the module level for serialization tests.

    Returns:
        The asset
    """

    @il.asset
    def my_asset(context: il.ExecutionContext) -> str:
        return f"Asset has access to: {_closed_over_value}"

    return (my_asset,)


class TestSerializationIntegration:
    """Test serialization integration scenarios."""

    def test_complex_dag_serialization(self):
        """Test serialization of a complex DAG with multiple assets and IOs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_io = il.FileIO(base_path=temp_dir)

            @il.asset(io=file_io)
            def source_asset():
                return "source_data"

            @il.asset(io=file_io)
            def processed_asset(source_asset):
                return f"processed_{source_asset}"

            @il.asset(io=file_io)
            def final_asset(processed_asset):
                return f"final_{processed_asset}"

            # Create DAG directly from decorated functions
            dag = il.DAG(source_asset(), processed_asset(), final_asset())

            # Serialize DAG
            dag_spec = dag.to_spec()
            assert isinstance(dag_spec, DAGSpec)
            assert len(dag_spec.assets) == 3

            # Test JSON serialization
            json_str = dag_spec.model_dump_json()
            parsed_spec = DAGSpec.model_validate_json(json_str)
            assert len(parsed_spec.assets) == 3

            # Verify asset paths
            asset_paths = {spec.path for spec in parsed_spec.assets}
            # Validate names irrespective of module prefix
            assert {p.split(".")[-1] for p in asset_paths} == {
                "source_asset",
                "processed_asset",
                "final_asset",
            }
            assert all("test_integration" in p for p in asset_paths)

    def test_spec_persistence(self):
        """Test saving and loading specs from files."""
        # Create a simple spec
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        dag_spec = DAGSpec(assets=[asset_spec])

        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(dag_spec.model_dump_json())
            temp_file = f.name

        try:
            # Load from file
            with open(temp_file) as f:
                loaded_data = json.load(f)

            # Reconstruct spec
            loaded_spec = DAGSpec.model_validate(loaded_data)
            assert len(loaded_spec.assets) == 1
            assert loaded_spec.assets[0].path == "myapp.assets.my_asset"

        finally:
            # Clean up
            Path(temp_file).unlink()


class TestSourcedAssetSerialization:
    """Tests for serializing assets that belong to sources."""

    def test_asset_from_source_with_closure_serialization(self):
        """Test that an asset from a source with a closure can be serialized."""
        source_instance = my_source()
        asset_instance = source_instance.assets["my_asset"]

        # Check the special path format
        spec = asset_instance.to_spec()
        assert ":" in spec.path
        # The path should now correctly point to the top-level source function
        assert "my_source:my_asset" in spec.path

    def test_asset_from_source_with_closure_reconstruction(self):
        """Test reconstructing a sourced asset and executing it."""
        source_instance = my_source()
        asset_instance = source_instance.assets["my_asset"]

        # Get the spec
        spec = asset_instance.to_spec()

        # Reconstruct the asset from the spec
        reconstructed_asset = spec.reconstruct()

        # Execute the reconstructed asset to check if the closure was preserved
        result = reconstructed_asset.run()
        assert result == "Asset has access to: secret_value"

