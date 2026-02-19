# %%
import time

import polars as pl

from caveclient import CAVEclient

client = CAVEclient("minnie65_phase3_v1")

cell_type_df = client.materialize.tables.cell_type_multifeature_v1().query(
    desired_resolution=[1, 1, 1],
    split_positions=True,
)

seg_version = 1412
cell_info = (
    client.materialize.views.aibs_cell_info()
    .query(
        materialization_version=seg_version,
        desired_resolution=[1, 1, 1],
        split_positions=True,
    )
    .set_index("id")
)
cell_info["cell_type_multifeature"] = cell_type_df.set_index("id")["cell_type"]
cell_info["broad_type_multifeature"] = cell_type_df.set_index("id")[
    "classification_system"
]

# %%
query_cell_info = cell_info.query("broad_type_multifeature == 'inhibitory'").copy()
print(len(query_cell_info), "inhibitory neurons")

# %%
table_path = (
    "gs://allen-minnie-phase3/mat_deltalakes/v1412/synapses_pni_2_v1412_deltalake"
)

# %%
pl.scan_delta(
    table_path,
).collect_schema()

# %%


def synapse_query(
    pre_ids=None,
    post_ids=None,
    bounding_box=None,
    bounding_box_column="post_pt_position",
    remove_autapses=True,
):
    table = pl.scan_delta(table_path)

    if remove_autapses:
        table = table.filter(pl.col("post_pt_root_id") != pl.col("pre_pt_root_id"))

    if pre_ids is not None:
        pre_list = [pre_ids] if isinstance(pre_ids, (int,)) else list(pre_ids)
        table = table.filter(pl.col("pre_pt_root_id").is_in(pre_list))

    if post_ids is not None:
        post_list = [post_ids] if isinstance(post_ids, (int,)) else list(post_ids)
        partition_list = [root_id % 1024 for root_id in post_list]
        join_index = pl.DataFrame(
            {"post_pt_root_id": post_list, "post_pt_root_id_partition": partition_list}
        ).lazy()
        table = table.filter(
            pl.col("post_pt_root_id").is_in(post_list),
            pl.col("post_pt_root_id_partition").is_in(partition_list),
        )
        table = table.join(
            join_index, on=["post_pt_root_id", "post_pt_root_id_partition"], how="semi"
        )

    if bounding_box is not None:
        min_corner, max_corner = bounding_box
        x_col = f"{bounding_box_column}_x"
        y_col = f"{bounding_box_column}_y"
        z_col = f"{bounding_box_column}_z"
        table = table.filter(
            (pl.col(x_col) >= min_corner[0])
            & (pl.col(x_col) <= max_corner[0])
            & (pl.col(y_col) >= min_corner[1])
            & (pl.col(y_col) <= max_corner[1])
            & (pl.col(z_col) >= min_corner[2])
            & (pl.col(z_col) <= max_corner[2])
        )

    return table.collect(engine="streaming")


sample_roots = query_cell_info["pt_root_id"].sample(50).tolist()
sample_roots

currtime = time.time()
polars_synapses = synapse_query(post_ids=sample_roots)
print(f"{time.time() - currtime:.3f} seconds elapsed.")

currtime = time.time()
materialization_synapses = client.materialize.synapse_query(
    post_ids=sample_roots,
    materialization_version=1412,
)
print(f"{time.time() - currtime:.3f} seconds elapsed.")

print(polars_synapses.shape[0])
print(materialization_synapses.shape[0])

# %%

currtime = time.time()

synapses = (
    pl.scan_delta(
        table_path,
    )
    .filter(
        pl.col("post_pt_root_id").is_in(query_cell_info["pt_root_id"].unique()[:100]),
        pl.col("post_pt_root_id") != pl.col("pre_pt_root_id"),
    )
    .select(["post_pt_root_id", "pre_pt_root_id", "size"])
    .collect(engine="streaming")
)

print(f"{time.time() - currtime:.3f} seconds elapsed.")

# %%
currtime = time.time()

synapse_sums = (
    pl.scan_delta(
        table_path,
    )
    .filter(
        pl.col("post_pt_root_id").is_in(query_cell_info["pt_root_id"].unique()[:100]),
        pl.col("post_pt_root_id") != pl.col("pre_pt_root_id"),
    )
    .group_by(["post_pt_root_id"])
    .agg(
        pl.col("size").mean().alias("input_mean_size"),
        pl.col("size").std().alias("input_std_size"),
    )
    .collect(engine="streaming")
)

print(f"{time.time() - currtime:.3f} seconds elapsed.")
