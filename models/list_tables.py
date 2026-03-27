import duckdb
with duckdb.connect("tpch.db", read_only=True) as con:
    # This queries the internal DuckDB catalog
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables").pl()
    print(tables)
    con.close()