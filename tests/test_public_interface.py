import datetime
from pathlib import Path
import levi
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import random
import pyarrow as pa


def test_skipped_stats():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', '=', 4.5)]
    res = levi.skipped_stats(delta_table, filters)
    expected = {'num_files': 3, 'num_files_skipped': 2, 'num_bytes_skipped': 3887}
    assert res == expected


def test_skipped_stats_between():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', '>', 1), ('a_float', "<", 3)]
    res = levi.skipped_stats(delta_table, filters)
    expected = {'num_files': 3, 'num_files_skipped': 1, 'num_bytes_skipped': 984}
    assert res == expected


def test_skipped_stats_less_than():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', "<", 4.5)]
    res2 = levi.skipped_stats(delta_table, filters)
    expected2 = {'num_files': 3, 'num_files_skipped': 0, 'num_bytes_skipped': 0}
    assert res2 == expected2


def test_skipped_stats_less_than_or_equal():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', "<=", 2.3)]
    res2 = levi.skipped_stats(delta_table, filters)
    expected2 = {'num_files': 3, 'num_files_skipped': 1, 'num_bytes_skipped': 984}
    assert res2 == expected2


def test_skipped_stats_greater_than():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', ">", 4.5)]
    res2 = levi.skipped_stats(delta_table, filters)
    expected2 = {'num_files': 3, 'num_files_skipped': 2, 'num_bytes_skipped': 3887}
    assert res2 == expected2


def test_skipped_stats_greater_than_or_equal():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', ">", 4.5)]
    res2 = levi.skipped_stats(delta_table, filters)
    expected2 = {'num_files': 3, 'num_files_skipped': 2, 'num_bytes_skipped': 3887}
    assert res2 == expected2


def test_filters_to_sql():
    assert levi.filter_to_sql(("a_float", "=", 4.5)) == "(`min.a_float` <= 4.5 and `max.a_float` >= 4.5)"


def test_filter_to_sql():
    assert levi.filter_to_sql(("a_float", "=", 4.5)) == "(`min.a_float` <= 4.5 and `max.a_float` >= 4.5)"
    assert levi.filter_to_sql(("a_float", ">", 3)) == "(`max.a_float` > 3)"


def test_delta_file_sizes():
    dt = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    res = levi.delta_file_sizes(dt, ["<300b", "300b-1kb", "1kb-100kb", ">100kb"])
    expected = {'num_files_<300b': 0, 'num_files_300b-1kb': 2, 'num_files_1kb-100kb': 1, 'num_files_>100kb': 0}
    assert res == expected


def test_latest_version():
    dt = DeltaTable("./tests/reader_tests/generated/multi_partitioned/delta")
    res = levi.latest_version(dt)
    expected = 2
    assert res == expected


def test_str_to_bytes():
    assert levi.str_to_bytes("100b") == 100
    assert levi.str_to_bytes("1kb") == 1_000
    assert levi.str_to_bytes("4gb") == 4_000_000_000


def test_boundary_parser():
    ten_tb = 10_000_000_000_000
    assert levi.boundary_parser("<=1kb") == (0, 1_000)
    assert levi.boundary_parser("<1kb") == (0, 999)
    assert levi.boundary_parser(">=1kb") == (1000, ten_tb)
    assert levi.boundary_parser(">1kb") == (1001, ten_tb)
    assert levi.boundary_parser("10kb-4gb") == (10_000, 4_000_000_000)


def test_updated_partitions_without_time_filter(tmp_path: Path):
    table_location = tmp_path / "test_table"

    df = pd.DataFrame(
        {
            "data": random.sample(range(0, 1000), 1000), 
            "partition_1": [1] * 1000, 
            "partition_2": ["a"] * 1000,
        }
    )

    write_deltalake(table_location, df, mode="append", partition_by=["partition_1", "partition_2"])

    df = pd.DataFrame(
        {
            "data": random.sample(range(0, 1000), 1000), 
            "partition_1": [2] * 1000, 
            "partition_2": ["b"] * 1000,
        }
    )

    write_deltalake(table_location, df, mode="append", partition_by=["partition_1", "partition_2"])

    delta_table = DeltaTable(table_location)

    updated_partitions = levi.updated_partitions(delta_table)

    assert updated_partitions == [{"partition_1": 1, "partition_2": "a"}, {"partition_1": 2, "partition_2": "b"}]

def test_updated_partitions_with_time_filter(tmp_path: Path):
    table_location = tmp_path / "test_table"

    df = pd.DataFrame(
        {
            "data": random.sample(range(0, 1000), 1000), 
            "partition_1": [1] * 1000, 
            "partition_2": ["a"] * 1000,
        }
    )

    start_time = datetime.datetime.now(datetime.timezone.utc)
    write_deltalake(table_location, df, mode="append", partition_by=["partition_1", "partition_2"])

    df = pd.DataFrame(
        {
            "data": random.sample(range(0, 1000), 1000), 
            "partition_1": [2] * 1000, 
            "partition_2": ["b"] * 1000,
        }
    )

    end_time = datetime.datetime.now(datetime.timezone.utc)
    write_deltalake(table_location, df, mode="append", partition_by=["partition_1", "partition_2"])

    delta_table = DeltaTable(table_location)

    updated_partitions = levi.updated_partitions(delta_table, start_time, end_time)

    assert updated_partitions == [{"partition_1": 1, "partition_2": "a"}]


def test_kills_duplicates_in_a_delta_table(tmp_path):
    path = f"{tmp_path}/deduplicate2"

    schema = pa.schema([
        ("col1", pa.int64()),
        ("col2", pa.string()),
        ("col3", pa.string()),
    ])

    df = pa.Table.from_pydict(
        {
            "col1": [1, 2, 3, 4, 5, 6, 9],
            "col2": ["A", "A", "A", "A", "B", "D", "B"],
            "col3": ["A", "B", "A", "A", "B", "D", "B"]
        },
        schema=schema
    )

    write_deltalake(path, df)

    delta_table = DeltaTable(path)

    levi.kill_duplicates(delta_table, ["col3", "col2"])

    actual_table = DeltaTable(path).to_pyarrow_table()
    actual_table_sort_indices = pa.compute.sort_indices(actual_table, sort_keys=[("col1", "ascending"), ("col2", "ascending"), ("col3", "ascending")])
    actual_table_sorted = actual_table.take(actual_table_sort_indices)

    expected_table = pa.Table.from_pydict(
        {
            "col1": [2, 6],
            "col2": ["A", "D"],
            "col3": ["B", "D"]
        },
        schema=schema   
    )
    expected_table_sort_indices = pa.compute.sort_indices(expected_table, sort_keys=[("col1", "ascending"), ("col2", "ascending"), ("col3", "ascending")])
    expected_table_sorted = expected_table.take(expected_table_sort_indices)

    assert actual_table_sorted == expected_table_sorted