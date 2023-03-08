import levi
from deltalake import DeltaTable


def test_delta_file_sizes():
    dt = DeltaTable("./tests/reader_tests/generated/basic_append/delta")
    print(levi.delta_file_sizes(dt))
    res = levi.delta_file_sizes(dt, ["<300b", "300b-1kb", "1kb-100kb", ">100kb"])
    expected = {'num_files_<300b': 0, 'num_files_300b-1kb': 2, 'num_files_1kb-100kb': 0, 'num_files_>100kb': 0}
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
