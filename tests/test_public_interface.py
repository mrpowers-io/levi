import pytest

import levi
from deltalake import DeltaTable


def test_skipped_stats():
    delta_table = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    filters = [('a_float', '=', 4.5)]
    res = levi.skipped_stats(delta_table, filters)
    expected = {'num_files': 2, 'num_files_skipped': 1, 'num_bytes_skipped': 996}
    assert res == expected

    filters2 = [('a_float', '>', 1), ('a_float', "<", 3)]
    res2 = levi.skipped_stats(delta_table, filters2)
    expected2 = {'num_files': 2, 'num_files_skipped': 2, 'num_bytes_skipped': 1980}
    assert res2 == expected2


def test_filters_to_sql():
    assert levi.filter_to_sql(("a_float", "=", 4.5)) == "(`min.a_float` <= 4.5 and `max.a_float` >= 4.5)"


def test_filter_to_sql():
    assert levi.filter_to_sql(("a_float", "=", 4.5)) == "(`min.a_float` <= 4.5 and `max.a_float` >= 4.5)"
    assert levi.filter_to_sql(("a_float", ">", 3)) == "(`min.a_float` < 3)"


def test_delta_file_sizes():
    dt = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    res = levi.delta_file_sizes(dt, ["<300b", "300b-1kb", "1kb-100kb", ">100kb"])
    expected = {'num_files_<300b': 0, 'num_files_300b-1kb': 2, 'num_files_1kb-100kb': 0, 'num_files_>100kb': 0}
    assert res == expected


@pytest.mark.skip(reason="There is a bug in the .get_actions implementation of delta-rs")
def test_get_all_columns_with_statistics():
    dt = DeltaTable("./tests/reader_tests/generated/with_changing_number_of_stat_cols/delta")
    columns = levi.get_all_columns_with_statistics(dt)
    assert columns == ['col1', 'col2', 'col3']


@pytest.mark.skip(reason="There is a bug in the .get_actions implementation of delta-rs")
def test_get_files_without_statistics_for_column():
    dt = DeltaTable("./tests/reader_tests/generated/with_changing_number_of_stat_cols/delta")
    res_col1 = levi.get_files_without_statistics_for_column(dt, 'col1')
    res_col2 = levi.get_files_without_statistics_for_column(dt, 'col2')
    assert res_col1 == []
    assert res_col2 == ['part-00000-1344c9ae-d450-49ca-aea9-ef32ad16fdf2-c000.snappy.parquet']


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
