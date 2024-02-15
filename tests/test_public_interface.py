import datetime
from pathlib import Path
import levi
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pandas as pd
import random
import pytest


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

def test_type_2_scd_upsert_with_single_attribute(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
        
    schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_time', pa.timestamp('us')),
        ('end_time', pa.timestamp('us')),                        

    ])
    data = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('effective_time', pa.timestamp('us')),
    ])
    updates_data = pa.Table.from_pydict(
        {
            'pkey': [2, 3],
            'attr': ["Z", "C"],
            'effective_time': [datetime.datetime(2025,1,1), datetime.datetime(2025,9,15)],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)

    levi.type_2_scd_upsert(
        delta_table=delta_table,
        updates_df=updates_data,
        primary_key="pkey",
        attr_col_names=["attr"],
        is_current_col_name="is_current",
        effective_time_col_name="effective_time",
        end_time_col_name="end_time",
    )

    actual_table = DeltaTable(path).to_pyarrow_table()
    expected_table = pa.Table.from_pydict(
        {
            'pkey': [2, 3, 2, 4, 1],
            'attr': ["B", "C", "Z", "D", "A"],
            'is_current': [False, True, True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2025,9,15), datetime.datetime(2025,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [datetime.datetime(2025,1,1), None, None, None, None]
        },
        schema=schema
    )

    actual_table_sort_indices = pa.compute.sort_indices(actual_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_actual_table = actual_table.take(actual_table_sort_indices)

    expected_table_sort_indices = pa.compute.sort_indices(expected_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_expected_table = expected_table.take(expected_table_sort_indices)

    assert sorted_actual_table == sorted_expected_table

def test_type_2_scd_upsert_with_multiple_attributes(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
    
    schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr1', pa.string()),
        ('attr2', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_time', pa.timestamp('us')),
        ('end_time', pa.timestamp('us')),                        

    ])
    data = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr1': ["A", "B", "D"],
            'attr2': ["foo", "bar", "baz"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr1', pa.string()),
        ('attr2', pa.string()),
        ('effective_time', pa.timestamp('us')),
    ])
    updates_data = pa.Table.from_pydict(
        {
            'pkey': [2, 3],
            'attr1': ["Z", "C"],
            'attr2': ["qux", "quux"],
            'effective_time': [datetime.datetime(2025,1,1), datetime.datetime(2025,9,15)],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)

    levi.type_2_scd_upsert(
        delta_table=delta_table,
        updates_df=updates_data,
        primary_key="pkey",
        attr_col_names=["attr1","attr2"],
        is_current_col_name="is_current",
        effective_time_col_name="effective_time",
        end_time_col_name="end_time",
    )

    actual_table = DeltaTable(path).to_pyarrow_table()
    expected_table = pa.Table.from_pydict(
        {
            'pkey': [2, 3, 2, 4, 1],
            'attr1': ["B", "C", "Z", "D", "A"],
            'attr2': ["bar", "quux", "qux", "baz", "foo"],
            'is_current': [False, True, True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2025,9,15), datetime.datetime(2025,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [datetime.datetime(2025,1,1), None, None, None, None]
        },
        schema=schema
    )

    actual_table_sort_indices = pa.compute.sort_indices(actual_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_actual_table = actual_table.take(actual_table_sort_indices)

    expected_table_sort_indices = pa.compute.sort_indices(expected_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_expected_table = expected_table.take(expected_table_sort_indices)

    assert sorted_actual_table == sorted_expected_table    


def test_type_2_scd_upsert_errors_out_if_base_df_does_not_have_all_required_columns(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
        
    schema = pa.schema([
        # ('pkey', pa.int64()), pkey missing from base
        ('attr', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_time', pa.timestamp('us')),
        ('end_time', pa.timestamp('us')),                        

    ])
    data = pa.Table.from_pydict(
        {
            # 'pkey': [1, 2, 4], pkey missing from base
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('effective_time', pa.timestamp('us')),
    ])
    updates_data = pa.Table.from_pydict(
        {
            'pkey': [2, 3],
            'attr': ["Z", "C"],
            'effective_time': [datetime.datetime(2025,1,1), datetime.datetime(2025,9,15)],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)


    with pytest.raises(TypeError):
        levi.type_2_scd_upsert(
            delta_table=delta_table,
            updates_df=updates_data,
            primary_key="pkey",
            attr_col_names=["attr"],
            is_current_col_name="is_current",
            effective_time_col_name="effective_time",
            end_time_col_name="end_time",
        )


def test_type_2_scd_upsert_errors_out_if_updates_df_does_not_have_all_required_columns(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
        
    schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_time', pa.timestamp('us')),
        ('end_time', pa.timestamp('us')),                        

    ])
    data = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        # ('pkey', pa.int64()), pkey missing
        ('attr', pa.string()),
        ('effective_time', pa.timestamp('us')),
    ])
    updates_data = pa.Table.from_pydict(
        {
            # 'pkey': [2, 3], pkey missing
            'attr': ["Z", "C"],
            'effective_time': [datetime.datetime(2025,1,1), datetime.datetime(2025,9,15)],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)


    with pytest.raises(TypeError):
        levi.type_2_scd_upsert(
            delta_table=delta_table,
            updates_df=updates_data,
            primary_key="pkey",
            attr_col_names=["attr"],
            is_current_col_name="is_current",
            effective_time_col_name="effective_time",
            end_time_col_name="end_time",
        )



def test_type_2_scd_upsert_does_not_insert_duplicate(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
        
    schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_time', pa.timestamp('us')),
        ('end_time', pa.timestamp('us')),                        

    ])
    data = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('effective_time', pa.timestamp('us')),
    ])
    updates_data = pa.Table.from_pydict(
        {
            'pkey': [1],
            'attr': ["A"],
            'effective_time': [datetime.datetime(2024,1,1)],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)


    levi.type_2_scd_upsert(
        delta_table=delta_table,
        updates_df=updates_data,
        primary_key="pkey",
        attr_col_names=["attr"],
        is_current_col_name="is_current",
        effective_time_col_name="effective_time",
        end_time_col_name="end_time",
    )

    actual_table = DeltaTable(path).to_pyarrow_table()
    expected_table = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_time': [datetime.datetime(2024,1,1), datetime.datetime(2024,1,1), datetime.datetime(2024,1,1)],
            'end_time': [None, None, None]
        },
        schema=schema
    )

    actual_table_sort_indices = pa.compute.sort_indices(actual_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_actual_table = actual_table.take(actual_table_sort_indices)

    expected_table_sort_indices = pa.compute.sort_indices(expected_table, sort_keys=[("pkey", "ascending"), ("effective_time", "ascending")])
    sorted_expected_table = expected_table.take(expected_table_sort_indices)

    assert sorted_actual_table == sorted_expected_table



def test_type_2_scd_upsert_with_version_number(tmp_path: Path):
    path = f"{tmp_path}/tmp/delta-upsert-single_attr"
    
    schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('is_current', pa.bool_()),
        ('effective_ver', pa.int64()),
        ('end_ver', pa.int64()),                        

    ])
    data = pa.Table.from_pydict(
        {
            'pkey': [1, 2, 4],
            'attr': ["A", "B", "D"],
            'is_current': [True, True, True],
            'effective_ver': [1, 1, 1],
            'end_ver': [None, None, None]
        },
        schema=schema
    )

    write_deltalake(path, data)

    updates_schema = pa.schema([
        ('pkey', pa.int64()),
        ('attr', pa.string()),
        ('effective_ver', pa.int64()),
    ])
    updates_data = pa.Table.from_pydict(
        {
            'pkey': [2, 3],
            'attr': ["Z", "C"],
            'effective_ver': [2, 3],
        },
        schema=updates_schema
    )

    delta_table = DeltaTable(path)

    levi.type_2_scd_upsert(
        delta_table=delta_table,
        updates_df=updates_data,
        primary_key="pkey",
        attr_col_names=["attr"],
        is_current_col_name="is_current",
        effective_time_col_name="effective_ver",
        end_time_col_name="end_ver",
    )

    actual_table = DeltaTable(path).to_pyarrow_table()
    expected_table = pa.Table.from_pydict(
        {
            'pkey': [2, 3, 2, 4, 1],
            'attr': ["B", "C", "Z", "D", "A"],
            'is_current': [False, True, True, True, True],
            'effective_ver': [1, 3, 2, 1, 1],
            'end_ver': [2, None, None, None, None]
        },
        schema=schema
    )

    actual_table_sort_indices = pa.compute.sort_indices(actual_table, sort_keys=[("pkey", "ascending"), ("effective_ver", "ascending")])
    sorted_actual_table = actual_table.take(actual_table_sort_indices)

    expected_table_sort_indices = pa.compute.sort_indices(expected_table, sort_keys=[("pkey", "ascending"), ("effective_ver", "ascending")])
    sorted_expected_table = expected_table.take(expected_table_sort_indices)

    assert sorted_actual_table == sorted_expected_table