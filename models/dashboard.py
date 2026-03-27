import duckdb

def generate_html_dashboard():
    con = duckdb.connect("tpch.db")

    # Get 10 slowest suppliers
    df_supplier = con.execute(
        """
        SELECT supplier_name, nation_name, avg_days
        FROM gold.shipping_metrics_df
        ORDER BY avg_days DESC
        LIMIT 10
        """
    ).pl()
    con.close()