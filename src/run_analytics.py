import duckdb
from src.config import WAREHOUSE_PATH

def get_pickup_column(con):
    # Get list of columns in fact_trips
    rows = con.execute("PRAGMA table_info('fact_trips');").fetchall()
    cols = [row[1] for row in rows]

    # Try common pickup datetime names
    candidates = [
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "pickup_datetime"
    ]
    for c in candidates:
        if c in cols:
            return c

    return None

def run_queries():
    con = duckdb.connect(WAREHOUSE_PATH)

    print("\n1. Total number of trips:")
    print(con.execute("SELECT COUNT(*) AS total_trips FROM fact_trips;").fetchdf())

    print("\n2. Total revenue (sum of fare_amount):")
    print(con.execute("SELECT SUM(fare_amount) AS total_revenue FROM fact_trips;").fetchdf())

    # ---- Time-based analytics ----
    pickup_col = get_pickup_column(con)
    print(f"\nDetected pickup datetime column: {pickup_col}")

    if pickup_col is not None:
        print("\n3. Average fare and trips by pickup hour:")
        hourly_sql = f"""
        SELECT
            EXTRACT(HOUR FROM {pickup_col}) AS pickup_hour,
            COUNT(*) AS trips,
            AVG(fare_amount) AS avg_fare
        FROM fact_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour;
        """
        print(con.execute(hourly_sql).fetchdf())

        print("\n4. Trips and revenue per day (first 10 days):")
        daily_sql = f"""
        SELECT
            CAST({pickup_col} AS DATE) AS trip_date,
            COUNT(*) AS trips,
            SUM(fare_amount) AS revenue
        FROM fact_trips
        GROUP BY trip_date
        ORDER BY trip_date
        LIMIT 10;
        """
        print(con.execute(daily_sql).fetchdf())
    else:
        print("\n(No pickup datetime column found – skipping time-based analysis.)")

    print("\n5. Sample rows from fact_trips:")
    print(con.execute("SELECT * FROM fact_trips LIMIT 5;").fetchdf())

    con.close()

if __name__ == "__main__":
    run_queries()
