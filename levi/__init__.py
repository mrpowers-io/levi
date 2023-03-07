import re
from deltalake import DeltaTable


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
