from pathlib import Path
import levi
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import random


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
