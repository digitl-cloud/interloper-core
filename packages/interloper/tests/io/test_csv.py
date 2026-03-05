"""Tests for CsvIO."""

import datetime as dt

import interloper as il


class TestCsvIO:
    """Tests for CsvIO."""

    def test_initialization(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))
        assert csv_io.base_path == str(tmp_path)

    def test_write_read_non_partitioned(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset
        def my_asset():
            return [{"a": "1", "b": "2"}]

        context = il.IOContext(asset=my_asset())
        data = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

        csv_io.write(context, data)
        result = csv_io.read(context)
        assert result == data

    def test_write_read_partitioned(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return [{"x": "1"}]

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.IOContext(asset=my_asset(), partition_or_window=partition)
        data = [{"x": "1"}, {"x": "2"}]

        csv_io.write(context, data)
        result = csv_io.read(context)
        assert result == data

    def test_write_read_partition_window(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return [{"v": "1"}]

        window = il.TimePartitionWindow(
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 3),
        )
        data = [{"v": "hello"}]

        csv_io.write(il.IOContext(asset=my_asset(), partition_or_window=window), data)

        # Reading a window returns a list per partition
        result = csv_io.read(il.IOContext(asset=my_asset(), partition_or_window=window))
        assert len(result) == 3
        assert all(r == data for r in result)

    def test_read_missing_file(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset
        def my_asset():
            return []

        context = il.IOContext(asset=my_asset())
        try:
            csv_io.read(context)
            assert False, "Expected FileNotFoundError"
        except FileNotFoundError:
            pass

    def test_write_empty_data(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset
        def my_asset():
            return []

        context = il.IOContext(asset=my_asset())
        csv_io.write(context, [])
        result = csv_io.read(context)
        assert result == []

    def test_partition_row_counts(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return []

        asset_instance = my_asset()

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))

        csv_io.write(il.IOContext(asset=asset_instance, partition_or_window=p1), [{"a": "1"}])
        csv_io.write(il.IOContext(asset=asset_instance, partition_or_window=p2), [{"a": "1"}, {"a": "2"}, {"a": "3"}])

        counts = csv_io.partition_row_counts(il.IOContext(asset=asset_instance))
        assert counts == {"2025-01-01": 1, "2025-01-02": 3}

    def test_partition_row_counts_empty(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return []

        counts = csv_io.partition_row_counts(il.IOContext(asset=my_asset()))
        assert counts == {}

    def test_to_spec(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))
        spec = csv_io.to_spec()
        assert spec.init == {"base_path": str(tmp_path)}
        reconstructed = spec.reconstruct()
        assert isinstance(reconstructed, il.CsvIO)
        assert reconstructed.base_path == str(tmp_path)

    def test_with_dataset(self, tmp_path):
        csv_io = il.CsvIO(str(tmp_path))

        @il.asset(dataset="my_dataset")
        def my_asset():
            return []

        context = il.IOContext(asset=my_asset())
        data = [{"col": "val"}]

        csv_io.write(context, data)
        result = csv_io.read(context)
        assert result == data
