"""Tests for @source decorator."""

from pydantic_settings import BaseSettings

import interloper as il


class SampleConfig(BaseSettings):
    """Sample config."""

    api_key: str = "test"


class TestSourceDecorator:
    """Tests for @source decorator."""

    def test_decorator_without_parentheses(self):
        """Test @source decorator without parentheses."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert isinstance(my_source, il.SourceDefinition)
        assert my_source.func is not None

    def test_decorator_with_empty_parentheses(self):
        """Test @source decorator with empty parentheses."""

        @il.source()
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert isinstance(my_source, il.SourceDefinition)

    def test_decorator_with_config(self):
        """Test @source decorator with config."""

        @il.source(config=SampleConfig)
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert my_source.config == SampleConfig

    def test_decorator_with_dataset(self):
        """Test @source decorator with dataset."""

        @il.source(dataset="my_dataset")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert my_source.dataset == "my_dataset"

    def test_decorator_with_io(self, tmp_path):
        """Test @source decorator with IO."""
        io = il.FileIO(tmp_path)

        @il.source(io=io)
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert my_source.io == io

    def test_decorator_with_default_io_key(self, tmp_path):
        """Test @source decorator with default_io_key."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)
        
        ios = {"local": il.FileIO(str(local_dir)), "cloud": il.FileIO(str(cloud_dir))}

        @il.source(io=ios, default_io_key="local")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert my_source.io == ios
        assert my_source.default_io_key == "local"

    def test_decorator_with_all_parameters(self, tmp_path):
        """Test @source decorator with all parameters."""
        local_dir = tmp_path / "data" / "local"
        local_dir.mkdir(parents=True)
        ios = {"local": il.FileIO(str(local_dir))}

        @il.source(
            config=SampleConfig,
            dataset="data",
            io=ios,
            default_io_key="local",
        )
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

            return (asset1,)

        assert my_source.config == SampleConfig
        assert my_source.dataset == "data"
        assert my_source.io == ios
        assert my_source.default_io_key == "local"

