import re
from typing import Optional, List
from deltalake import DeltaTable
import datetime
import numpy as np

import pyarrow.compute as pc 
import pyarrow as pa
from pyarrow.interchange.from_dataframe import DataFrameObject


def skipped_stats(delta_table, filters):
    df = delta_table.get_add_actions(flatten=True).to_pandas()
    total_files = len(df)
    total_bytes = df["size_bytes"].sum()
    res = df.query(filters_to_sql(filters))
    num_files_skipped = total_files - len(res)
    num_bytes_skipped = total_bytes - res["size_bytes"].sum()
    return {
        "num_files": total_files,
        "num_files_skipped": num_files_skipped,
        "num_bytes_skipped": num_bytes_skipped
    }


def filters_to_sql(filters):
    res = []
    for filter in filters:
        res.append(filter_to_sql(filter))
    return " and ".join(res)


def filter_to_sql(filter):
    col, operator, val = filter
    if operator == "=":
        return f"(`min.{col}` <= {val} and `max.{col}` >= {val})"
    elif operator == "<":
        return f"(`min.{col}` < {val})"
    elif operator == "<=":
        return f"(`min.{col}` <= {val})"
    elif operator == ">":
        return f"(`max.{col}` > {val})"
    elif operator == ">=":
        return f"(`max.{col}` >= {val})"
    else:
        raise ValueError(f"{filter} cannot be parsed.")


def latest_version(delta_table: DeltaTable):
    return delta_table.version()


def delta_file_sizes(delta_table: DeltaTable, boundaries=None):
    if boundaries is None:
        boundaries = ["<1mb", "1mb-500mb", "500mb-1gb", "1gb-2gb", ">2gb"]
    df = delta_table.get_add_actions(flatten=True).to_pandas()
    res = {}
    for boundary in boundaries:
        min, max = boundary_parser(boundary)
        count = len(df[df['size_bytes'].between(min, max)])
        res[f"num_files_{boundary}"] = count
    return res


def str_to_bytes(s):
    kilobyte = 1_000
    megabyte = 1_000_000
    gigabyte = 1_000_000_000
    terabyte = 1_000_000_000_000
    parts = re.split('(\d+)', s)
    num = int(parts[1])
    units = parts[2]
    if units == "b":
        return num
    elif units == "kb":
        return num * kilobyte
    elif units == "mb":
        return num * megabyte
    elif units == "gb":
        return num * gigabyte
    elif units == "tb":
        return num * terabyte
    else:
        raise ValueError(
            f"{s} cannot be parsed.  Valid units are b, kb, mb, gb, or tb.  45b, 8kb, and 4gb are valid examples.")


def boundary_parser(boundary):
    # TODO: Figure out the proper way to represent this large value
    ten_tb = 10_000_000_000_000
    # Supports <, <=, >, >=, and between which is indicated with a dash
    if boundary.startswith("<="):
        return 0, str_to_bytes(boundary.replace("<=", ""))
    elif boundary.startswith("<"):
        return 0, str_to_bytes(boundary.replace("<=", "")) - 1
    elif boundary.startswith(">="):
        return str_to_bytes(boundary.replace("<=", "")), ten_tb
    elif boundary.startswith(">"):
        return str_to_bytes(boundary.replace("<=", "")) + 1, ten_tb
    elif "-" in boundary:
        first, second = boundary.split("-")
        return str_to_bytes(first), str_to_bytes(second)
    else:
        raise ValueError(
            f"{boundary} cannot be parsed.  Valid prefixes are <, <=, >, and >=.  Two values can be delimited with a dash.  Valid examples include <10kb and 10kb-5gb.")

def updated_partitions(delta_table: DeltaTable, start_time: Optional[datetime.datetime] = None, end_time: Optional[datetime.datetime] = None) -> dict[str, str]:
    add_actions_df = delta_table.get_add_actions().to_pandas()

    if start_time is not None:
        add_actions_df = add_actions_df[add_actions_df["modification_time"] >= np.datetime64(int(start_time.timestamp() * 1e6), "us")]
    if end_time is not None:
        add_actions_df = add_actions_df[add_actions_df["modification_time"] < np.datetime64(int(end_time.timestamp() * 1e6), "us")]

    return add_actions_df.drop_duplicates(subset=["partition_values"])["partition_values"].tolist()

def kill_duplicates(delta_table: DeltaTable, duplication_columns: List[str]):
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param duplication_columns: <description>
    :type duplication_columns: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type or are empty.
    :raises TypeError: Raises type error when required columns are missing in the provided delta table.
    """

    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not duplication_columns or len(duplication_columns) == 0:
        raise TypeError("Duplication columns must be specified")      

    data_frame = delta_table.to_pyarrow_table()

    # Make sure that all the required columns are present in the provided delta table
    append_data_columns = data_frame.column_names
    for required_column in duplication_columns:
        if required_column not in append_data_columns:
            raise TypeError(
                f"The base table has these columns {append_data_columns!r}, but these columns are required {duplication_columns!r}"
            )
        
    data_frame = (
        data_frame
        .group_by(duplication_columns)
        .aggregate([([], "count_all")])
        .filter(pc.field("count_all") > 1)
    )

    predicate = " AND ".join([f"source.{column} = target.{column}" for column in duplication_columns])

    (
        delta_table
        .merge(
            source=data_frame,
            predicate=predicate,
            source_alias="source",
            target_alias="target")
        .when_matched_delete()
        .execute()

def type_2_scd_upsert(
        delta_table: DeltaTable,
        updates_df: DataFrameObject,
        primary_key: str,
        attr_col_names: List[str],
        is_current_col_name: str,
        effective_time_col_name: str,
        end_time_col_name: str,
) -> None:
    """
    Upserts SCD2 changed to a DeltaTable.
    Based off: https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables

    :param delta_table: DeltaTable
    :type path: str
    :param updates_df: Object supporting the interchange protocol, i.e. `__dataframe__` method.
    :type updates_df: DataFrameObject
    :param primary_key: <description>
    :type primary_key: str
    :param attr_col_names: <description>
    :type attr_col_names: List[str]
    :param is_current_col_name: <description>
    :type is_current_col_name: str
    :param effective_time_col_name: <description>
    :type effective_time_col_name: str
    :param end_time_col_name: <description>
    :type effective_time_col_name: str

    :raises ValueError: Raises value error when updates data frame object does not support the interchange protocol.
    :raises TypeError: Raises type error when required column names are not in the base table.
    :raises TypeError: Raises type error when required column names for updates are not in the attributes columns list.

    :returns: <description>
    :rtype: None
    """
    # only need to compare to current records
    base_table = delta_table.to_pyarrow_table(
        filters=[
            (is_current_col_name,"=", True)
        ]
    )

    # validate the existing Delta table
    base_col_names = base_table.column_names
    required_base_col_names = (
        [primary_key]
        + attr_col_names
        + [is_current_col_name, effective_time_col_name, end_time_col_name]
    )
    if sorted(base_col_names) != sorted(required_base_col_names):
        raise TypeError(
            f"The base table has these columns {base_col_names!r}, but these columns are required {required_base_col_names!r}"
        )

    # validate the updates DataFrame
    updates_table = pa.interchange.from_dataframe(updates_df)
    updates_col_names = updates_table.column_names
    required_updates_col_names = (
        [primary_key] + attr_col_names + [effective_time_col_name]
    )
    if sorted(updates_col_names) != sorted(required_updates_col_names):
        raise TypeError(
            f"The updates DataFrame has these columns {updates_col_names!r}, but these columns are required {required_updates_col_names!r}"
        )

    base_table = base_table.filter(pa.compute.field(is_current_col_name) == True)

    # create not equal or filter expression search for new records
    for i, col in enumerate(attr_col_names):
        if i == 0:
            filter_expression = (pa.compute.field(col) != pa.compute.field(f"{col}_base"))
        else:
            filter_expression = filter_expression.__or__((pa.compute.field(col) != pa.compute.field(f"{col}_base")))

    new_records_to_insert_table = updates_table.join(
        right_table=base_table,
        keys=primary_key,
        join_type='inner',
        right_suffix='_base'
    ).filter(
        filter_expression
    )

    merge_key_data_type = base_table.schema.field(primary_key).type 

    # fill the merge_key column with nulls
    new_records_to_insert_table = new_records_to_insert_table.append_column(
        'merge_key', pa.array([None] * new_records_to_insert_table.num_rows, type=merge_key_data_type)
    )

    # copy primary key into merge_key column for the updates table
    updates_table = updates_table.append_column('merge_key', updates_table[primary_key])

    # select only the columns required - this drops the joined _base columns
    new_records_to_insert_table = new_records_to_insert_table.select(
        updates_table.column_names
    )

    upsert_table = pa.concat_tables(
        [
            updates_table,
            new_records_to_insert_table,
        ]
    )

    (
        delta_table.merge(
            source=upsert_table,
            predicate=f"target.{primary_key} = source.merge_key and target.is_current",
            source_alias="source",
            target_alias="target"
        ).when_not_matched_insert(
            updates={
                primary_key: f"source.{primary_key}",
                **{col: f"source.{col}" for col in attr_col_names},
                effective_time_col_name: f"source.{effective_time_col_name}", 
                is_current_col_name: "true",
                end_time_col_name: "null"
            },
        ).when_matched_update(
            updates={
                is_current_col_name: "false",
                end_time_col_name: f"source.{effective_time_col_name}"
            },
            predicate=' or '.join([f"source.{col} != target.{col}" for col in attr_col_names])
        ).execute()
    )