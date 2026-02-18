# %%
import os
import subprocess
import time
from functools import partial

import numpy as np
import pandas as pd
import polars as pl
from caveclient import CAVEclient
from cloudpathlib import AnyPath as Path
from cloudvolume import CloudVolume
from deltalake import DeltaTable, write_deltalake
from deltalake.table import TableOptimizer
from deltalake.writer import BloomFilterProperties, ColumnProperties, WriterProperties
from shapely import wkb

total_time = time.time()
# %%

# PARAMETERS
# ----------

# ---Table details---

# cloud base path for mat dbs - this is currently not advertised anywhere and you need
# to know where it is for your datastack
# mat_db_cloud_path = "gs://mat_dbs/public/"
mat_db_cloud_path = os.getenv(
    "MAT_DB_CLOUD_PATH", "gs://cave_annotation_bucket/public/"
)

# name of the datastack
# datastack = "zheng_ca3"
datastack = os.getenv("DATASTACK", "v1dd")

# name of the table or view to process
# table_name = "synapses_ca3_v1_filtered_view"
table_name = os.getenv("TABLE_NAME", "connections_with_nuclei")

# materialization version
# version = 357
version = int(os.getenv("VERSION", "1196"))

# columns to drop from the table if seen, no error if any are missing
DROP_COLUMNS = ["created", "deleted", "superceded_id", "valid"]

# ---Processing details---

# number of rows to process and write at a time - bigger is usually better, up to
# memory limits
n_rows_per_chunk = int(os.getenv("N_ROWS_PER_CHUNK", "50000000"))

# where to put the output
out_path = os.getenv(
    "OUT_PATH",
    f"/Users/ben.pedigo/code/meshrep/meshrep/data/{datastack}_{table_name}_deltalake_v{version}",
)

# ---Deltalake output details---

# column to use for partitioning when writing to deltalake
# TODO could extend to multiple columns if desired
partition_column = os.getenv("PARTITION_COLUMN", "post_pt_root_id")

# number of partitions to create
n_partitions = int(os.getenv("N_PARTITIONS", "64"))

# whether to convert ids to segmentation ids before partitioning
# NOTE: I haven't seen any advantage to doing this so far
use_seg_id = False

# Z-order curve columns for optimizing the table after writing
zorder_columns_str = os.getenv("ZORDER_COLUMNS", "post_pt_root_id,id")
zorder_columns = [col.strip() for col in zorder_columns_str.split(",") if col.strip()]

# columns to add bloom filters to, can be empty list
bloom_filter_columns_str = os.getenv("BLOOM_FILTER_COLUMNS", "id")
bloom_filter_columns = [
    col.strip() for col in bloom_filter_columns_str.split(",") if col.strip()
]

# false positive probability for the bloom filters
fpp = float(os.getenv("FPP", "0.001"))


# %%
base_cloud_path = Path(f"{mat_db_cloud_path}/{datastack}/v{version}")
table_file_name = f"{table_name}.csv.gz"
header_file_name = f"{table_name}_header.csv"
table_cloud_path = base_cloud_path / table_file_name
header_cloud_path = base_cloud_path / header_file_name

print("Working on table:")
print(table_cloud_path)
print(header_cloud_path)
print()
# check the file sizes and that they exist
print(f"Table size: {table_cloud_path.stat().st_size / 1e9:.3f} GB")
print(f"Header size: {header_cloud_path.stat().st_size / 1e3:.3f} KB")
print()

# %%
# download the table and header files locally

download_time = time.time()

print("Downloading table and header files...")

temp_path = Path("/tmp/table_to_deltalake")
temp_path.mkdir(exist_ok=True)

subprocess.run(
    ["gsutil", "cp", str(table_cloud_path), str(temp_path / table_cloud_path.name)]
)
subprocess.run(
    [
        "gsutil",
        "cp",
        str(header_cloud_path),
        str(temp_path / header_cloud_path.name),
    ]
)

print(f"{time.time() - download_time:.3f} seconds elapsed to download files.")
print()

# %%
# unzip the table
# this was more reliable for large files than using pandas/polars unzip directly for me

unzip_time = time.time()

print("Unzipping table file...")

table_local_path = temp_path / table_file_name
header_local_path = temp_path / header_file_name
subprocess.run(
    [
        "gunzip",
        str(table_local_path),
    ]
)
table_local_path = temp_path / f"{table_name}.csv"

print(f"{time.time() - unzip_time:.3f} seconds elapsed to unzip table.")
print()

# %%

# create a plan for reading in data via polars

write_time = time.time()

print("Reading in table and writing to deltalake...")

SQL_TO_POLARS_DTYPE = {
    "bigint": pl.Int64,
    "integer": pl.Int32,
    "smallint": pl.Int16,
    "real": pl.Float32,
    "double precision": pl.Float64,
    "numeric": pl.Decimal,
    "boolean": pl.Boolean,
    "text": pl.String,
    "varchar": pl.String,
    "character varying": pl.String,
    "date": pl.Date,
    "timestamp without time zone": pl.Datetime,
    "timestamp with time zone": pl.Datetime,
}


def sql_to_polars_dtype(sql_type: str) -> pl.datatypes.DataType:
    """
    Convert a SQL dtype string to a Polars dtype.
    Raises ValueError if the dtype is not recognized.
    """
    sql_type = sql_type.strip().lower()
    # handle e.g. 'character varying(255)'
    if "(" in sql_type:
        sql_type = sql_type.split("(")[0].strip()
    if sql_type not in SQL_TO_POLARS_DTYPE:
        valid = ", ".join(sorted(SQL_TO_POLARS_DTYPE))
        raise ValueError(
            f"Unrecognized SQL dtype: {sql_type!r}. Valid options: {valid}"
        )
    return SQL_TO_POLARS_DTYPE[sql_type]


def build_polars_schema(schema_df):
    """
    Given a DataFrame with columns ['field', 'dtype'],
    return a dict usable as a Polars schema.
    """
    return {
        row.field: sql_to_polars_dtype(row.dtype)
        for row in schema_df.itertuples(index=False)
    }


# decoder for WKB point columns, if necessary
# TODO int32 is hard-coded here, make more flexible
def decoder(x: str) -> np.ndarray[np.int32]:
    point = wkb.loads(bytes.fromhex(x))
    out = np.array([point.x, point.y, point.z], dtype=np.int32)
    return out


def id_partition_func(
    id_to_encode: int,
    n_partitions: int = 256,
    use_seg_id: bool = False,
    cv: CloudVolume = None,
) -> np.uint16:
    if id_to_encode == 0:
        return np.uint16(0)
    if use_seg_id:
        id_to_encode = cv.meta.decode_segid(id_to_encode)

    # salt = 123456
    # partition = hash(id_to_encode ^ salt) % n_partitions
    # partition = ((id_to_encode * 2654435761) & 0xFFFFFFFF) % n_partitions

    partition = id_to_encode % n_partitions
    return np.uint16(partition)


header = pd.read_csv(header_local_path, header=None).rename(
    columns={0: "field", 1: "dtype"}
)

schema = build_polars_schema(header)


table = pl.scan_csv(table_local_path, has_header=False, schema=schema).drop(
    DROP_COLUMNS, strict=False
)

schema = table.collect_schema()

print("Reading in table with schema:")
for key, val in schema.items():
    print(f"{key}: {val}")
print()

columns = table.collect_schema().names()


# intended to only catch unpacked point columns
position_columns = [c for c in columns if (c.endswith("_pt_position"))]

if len(position_columns) > 0:
    print(f"Decoding {len(position_columns)} position columns...")
    table = table.with_columns(
        pl.col(position_columns).map_elements(decoder, return_dtype=pl.List(pl.Int32))
    )

use_seg_id = False
if use_seg_id:
    client = CAVEclient(datastack, version=version)
    cv = client.info.segmentation_cloudvolume()
else:
    cv = None

partial_id_partition_func = partial(
    id_partition_func,
    n_partitions=n_partitions,
    use_seg_id=use_seg_id,
    cv=cv,
)

partition_by = f"{partition_column}_partition"

table = table.with_columns(
    pl.col(partition_column)
    .map_elements(
        partial_id_partition_func,
        return_dtype=pl.UInt16,
    )
    .alias(partition_by)
)

write_mode = "append"
unfinished = True

start = 0
while unfinished:
    print(f"Processing chunk for rows {start:,} to {start + n_rows_per_chunk:,}...")
    chunk_table = table.slice(start, n_rows_per_chunk).collect()

    # Process the chunk...
    # If the chunk is empty, we're done
    if chunk_table.is_empty():
        unfinished = False
    else:
        start += n_rows_per_chunk

    write_deltalake(
        out_path,
        chunk_table,
        partition_by=partition_by,
        mode=write_mode,
    )

print(f"{time.time() - write_time:.3f} seconds elapsed to read and write table.")
print()

# %%

# delete the temporary files

delete_time = time.time()

print("Cleaning up temporary files...")
subprocess.run(["rm", str(table_local_path)])
subprocess.run(["rm", str(header_local_path)])
print(f"{time.time() - delete_time:.3f} seconds elapsed to delete temporary files.")
print()

# %%
# optimize the deltalake with z-ordering and bloom filters

optimize_time = time.time()

print("Optimizing deltalake...")
if len(bloom_filter_columns) > 0:
    bloom = BloomFilterProperties(
        set_bloom_filter_enabled=True,
        fpp=fpp,
    )
    column_properties = ColumnProperties(bloom_filter_properties=bloom)
    writer_properties = WriterProperties(
        column_properties={col: column_properties for col in bloom_filter_columns}
    )
else:
    writer_properties = None


dt = DeltaTable(out_path)
to = TableOptimizer(dt)
to.z_order(columns=zorder_columns, writer_properties=writer_properties)
dt.vacuum(dry_run=False, retention_hours=0, enforce_retention_duration=False, full=True)

print(f"{time.time() - optimize_time:.3f} seconds elapsed to optimize deltalake.")
print()

print("Done!")
print("-----------------")
print(f"{time.time() - total_time:.3f} seconds elapsed total.")
print("-----------------")
