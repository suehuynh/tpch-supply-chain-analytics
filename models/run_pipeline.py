from etl.bronze import load_nation, load_orders, load_supplier, load_lineitem
from etl.silver import dim_supplier, fct_lineitem, fct_orders, fct_shipping
from etl.gold.obt import wide_supplier
from etl.gold.pre_aggregated import metrics_by_supplier, metrics_by_nation, metrics_by_shipmode
import duckdb

def create_shipping_metrics():
    # Create necessary bronze table
    source_database = "tpch.db"
    nation_df = load_nation.create_dataset(table_name="nation", source_database=source_database)
    orders_df = load_orders.create_dataset(table_name="orders", source_database=source_database)
    supplier_df = load_supplier.create_dataset(table_name="supplier", source_database=source_database)
    lineitem_df = load_lineitem.create_dataset(table_name="lineitem", source_database=source_database)

    # Create silver tables
    dim_supplier_df = dim_supplier.create_dataset(
        supplier_df, nation_df
    )
    fct_lineitem_df = fct_lineitem.create_dataset(
        lineitem_df
    )
    fct_orders_df = fct_orders.create_dataset(
        orders_df
    )
    fct_shipping_df = fct_shipping.create_dataset(
        lineitem_df, orders_df
    )

    # Create gold OBT tables
    wide_supplier_df = wide_supplier.create_dataset(
        fct_shipping_df, dim_supplier_df
    )

    # Create gold pre-aggregated tables
    agg_supp = metrics_by_supplier.create_dataset(
        wide_supplier_df
    )

    agg_mode = metrics_by_shipmode.create_dataset(
        wide_supplier_df
    )

    agg_nat = metrics_by_nation.create_dataset(
        wide_supplier_df
    )

    with duckdb.connect(source_database) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        # DuckDB can "see" the Polars variables agg_supp, agg_mode, etc.
        con.execute("CREATE OR REPLACE TABLE gold.agg_supplier AS SELECT * FROM agg_supp")
        con.execute("CREATE OR REPLACE TABLE gold.agg_shipmode AS SELECT * FROM agg_mode")
        con.execute("CREATE OR REPLACE TABLE gold.agg_nation AS SELECT * FROM agg_nat")
    
    return agg_supp, agg_mode, agg_nat



if __name__ == "__main__":
    # Print output
    agg_supp, agg_mode, agg_nat = create_shipping_metrics()
    print(agg_supp, agg_mode, agg_nat)