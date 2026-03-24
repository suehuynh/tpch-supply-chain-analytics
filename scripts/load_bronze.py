import duckdb
import os

source_database = 'tpch.db'
data_dir = 'data/raw'
table_names = ['lineitem', 'orders', 'supplier', 'nation']

def load_bronze_layer():
    con = duckdb.connect(source_database)

    print(f"--- Initializing Bronze Layer in {source_database} ---")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

    for table in table_names:
        file_path = os.path.join(data_dir, f"{table}.parquet")

        if not os.path.exists(file_path):
            print(f"Error: {file_path} not found. Skipping...")
            continue
        print(f"Loading and cleaning: {table}...")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table} AS 
            SELECT * FROM read_parquet('{file_path}');
        """)
        
        # Optional: Verify row count
        count = con.execute(f"SELECT count(*) FROM bronze.{table}").fetchone()[0]
        print(f"Successfully loaded {count} rows into bronze.{table}")

    con.close()
    print("--- Bronze Load Complete ---")

if __name__ == "__main__":
    load_bronze_layer()