import duckdb
import polars as pl
import pandas

def generate_dashboard():
    with duckdb.connect("tpch.db", read_only=True) as con:
        df_nation = con.sql("SELECT * FROM gold.agg_nation ORDER BY avg_days DESC LIMIT 10").pl()
        df_supplier = con.sql("SELECT * FROM gold.agg_supplier ORDER BY avg_days DESC LIMIT 10").pl()
        df_shipmode = con.sql("SELECT * FROM gold.agg_shipmode ORDER BY avg_days DESC LIMIT 10").pl()

    # Convert to HTML 
    html_content = f"""
    <html>
        <head><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"></head>
        <body class="container">
            <h1>Supply Chain Dashboard</h1>
            <h2>Avg Shipping by Nation</h2>
            {df_nation.to_pandas().to_html(classes='table table-striped')}
            <h2>Avg Shipping by Shipping Mode</h2>
            {df_supplier.to_pandas().to_html(classes='ttable table-striped')}
            <h2>SAvg Shipping by Suppliers</h2>
            {df_supplier.to_pandas().to_html(classes='table table-striped')}
        </body>
    </html>
    """
    
    with open("dashboard.html", "w") as f:
        f.write(html_content)
    print("🚀 Dashboard created: dashboard.html")

if __name__ == "__main__":
    generate_dashboard()