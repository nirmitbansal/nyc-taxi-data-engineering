import os
import duckdb
from src.config import PROCESSED_DIR, WAREHOUSE_PATH

def build_warehouse():
    # Ensure warehouse directory exists
    os.makedirs(os.path.dirname(WAREHOUSE_PATH), exist_ok=True)

    # Connect to DuckDB file (it will be created if not exists)
    con = duckdb.connect(WAREHOUSE_PATH)

    # All parquet files created by ingest_to_parquet.py
    parquet_glob = os.path.join(PROCESSED_DIR, "chunk_*.parquet")

    # Drop old table if it exists
    con.execute("DROP TABLE IF EXISTS fact_trips;")

    # Create new table from all parquet files
    create_sql = f"""
    CREATE TABLE fact_trips AS
    SELECT *
    FROM parquet_scan('{parquet_glob}', union_by_name=True);
    """


    con.execute(create_sql)
    con.close()

    print("Warehouse built at:", WAREHOUSE_PATH)

if __name__ == "__main__":
    build_warehouse()
