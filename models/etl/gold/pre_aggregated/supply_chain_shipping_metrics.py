import json
import polars as pl

# from utils.metadata import get_latest_run_metrics

def create_dataset(wide_supplier):
    wide_shipping_length = wide_supplier.with_columns(
        (pl.col("shipdate") - pl.col("orderdate")).dt.total_days().alias("shipping_length")
    )

    # Aggregate by Supplier
    agg_supplier = df_with_length.group_by("supplier_name").agg([
        pl.col("shipping_length").max().alias("max_days"),
        pl.col("shipping_length").min().alias("min_days"),
        pl.col("shipping_length").mean().alias("avg_days")
    ]).sort("avg_days")

    # Aggregate by Shipping Mode
    agg_shipmode = df_with_length.group_by("shipmode").agg([
        pl.col("shipping_length").max().alias("max_days"),
        pl.col("shipping_length").min().alias("min_days"),
        pl.col("shipping_length").mean().alias("avg_days")
    ]).sort("avg_days")

    # Aggregate by Nation
    agg_nation = df_with_length.group_by("nation_name").agg([
        pl.col("shipping_length").max().alias("max_days"),
        pl.col("shipping_length").min().alias("min_days"),
        pl.col("shipping_length").mean().alias("avg_days")
    ]).sort("avg_days")

    return agg_supplier, agg_shipmode, agg_nation