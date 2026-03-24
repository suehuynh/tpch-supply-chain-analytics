from etl.bronze import load_nation, load_orders, load_supplier, load_lineitem
from etl.silver import dim_supplier, fct_lineitem, fct_orders, fct_shipping
from etl.gold.obt import wide_supplier
from etl.gold.pre_aggregated import supply_chain_shipping_metrics

def create_shipping_metrics():
    # Create necessary bronze table
    nation_df = load_nation.create_dataset()
    orders_df = load_orders.create_dataset()
    supplier_df = load_supplier.create_dataset()
    lineitem_df = load_lineitem.create_dataset()

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
    shipping_metrics_df = supply_chain_shipping_metrics.create_dataset(
        wide_supplier_df
    )

    return shipping_metrics_df

if __name__ == "__main__":
    # Print output
    print(create_shipping_metrics().limit(10))