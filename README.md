# Levi

Delta Lake helper methods.  No Spark dependency.

## Installation

Install the latest version with `pip install levi`.

## Delta File Stats

The `delta_file_stats` function provides information on the number of bytes in files of a Delta table.  Example usage:

```python
import levi
from deltalake import DeltaTable

dt = DeltaTable("some_folder/some_table")
levi.delta_file_sizes(dt)

# return value
{
    'num_files_<1mb': 345, 
    'num_files_1mb-500mb': 588,
    'num_files_500mb-1gb': 960,
    'num_files_1gb-2gb': 0, 
    'num_files_>2gb': 5
}
```

This output shows that there are 345 small files with less than 1mb of data and 5 huge files with more than 2gb of data.  It'd be a good idea to compact the small files and split up the large files to make queries on this Delta table run faster.

You can also specify the boundaries when you invoke the function to get a custom result:

```python
levi.delta_file_sizes(dt, ["<1mb", "1mb-200mb", "200mb-800mb", "800mb-2gb", ">2gb"])
```
