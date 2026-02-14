"""Tests for @source decorator."""

import interloper as il


class SampleConfig(il.Config):
    """Sample config."""

    api_key: str = "test"


class TestSourceDecorator:
    """Tests for @source decorator."""

    def test_decorator_without_parentheses(self):
        """Test @source decorator without parentheses."""

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

        assert isinstance(MySource, il.SourceDefinition)
        assert MySource.cls is not None

    def test_decorator_with_empty_parentheses(self):
        """Test @source decorator with empty parentheses."""

        @il.source()
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

        assert isinstance(MySource, il.SourceDefinition)

    def test_decorator_with_config(self):
        """Test @source decorator with config."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

        assert MySource.config == SampleConfig

    def test_decorator_with_dataset(self):
        """Test @source decorator with dataset."""

        @il.source(dataset="my_dataset")
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

        assert MySource.dataset == "my_dataset"

    def test_decorator_with_all_parameters(self):
        """Test @source decorator with all parameters."""

        @il.source(
            config=SampleConfig,
            dataset="data",
        )
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

        assert MySource.config == SampleConfig
        assert MySource.dataset == "data"
