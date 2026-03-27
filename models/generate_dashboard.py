import duckdb
import polars as pl
import pandas

def generate_dashboard():
    with duckdb.connect("tpch.db", read_only=True) as con:
        df_nation = con.sql("SELECT * FROM gold.agg_nation").pl()
        df_supplier = con.sql("SELECT * FROM gold.agg_supplier").pl()

    # Convert to HTML 
    html_content = f"""
    <html>
        <head><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"></head>
        <body class="container">
            <h1>Supply Chain Dashboard</h1>
            <h2>Avg Shipping by Nation</h2>
            {df_nation.to_pandas().to_html(classes='table table-striped')}
            <h2>Slowest Suppliers</h2>
            {df_supplier.to_pandas().to_html(classes='table table-dark')}
        </body>
    </html>
    """
    
    with open("index.html", "w") as f:
        f.write(html_content)
    print("🚀 Dashboard created: index.html")

if __name__ == "__main__":
    generate_dashboard()