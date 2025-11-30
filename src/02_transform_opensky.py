from pathlib import Path
import duckdb


DB_PATH = Path("data/opensky.duckdb")


def main():
    # Connect to the DuckDB file
    con = duckdb.connect(DB_PATH)

    # Create a cleaned table with derived columns
    # Notes:
        # to_timestamp(snapshot_time) converts UNIX seconds to actual timestamp
        # date_trunc('minute', ...) helps to analyze by minute
        # velocity * 1.94384 converts m/s to knots (useful for aviation)
        # WHERE filters out rows without coordinates
    con.execute("""
        CREATE OR REPLACE TABLE states_clean AS
        SELECT
            snapshot_time,
            to_timestamp(snapshot_time) AS snapshot_ts,
            date_trunc('minute', to_timestamp(snapshot_time)) AS snapshot_minute,
            icao24,
            callsign,
            origin_country,
            longitude,
            latitude,
            baro_altitude,
            geo_altitude,
            on_ground,
            velocity,
            velocity * 1.94384 AS speed_knots,
            true_track,
            vertical_rate,
            position_source
        FROM states_raw
        WHERE longitude IS NOT NULL
          AND latitude IS NOT NULL;
    """)

    # Show summary info
    count = con.execute("SELECT COUNT(*) FROM states_clean").fetchone()[0]
    min_ts, max_ts = con.execute("""
        SELECT MIN(snapshot_ts), MAX(snapshot_ts) FROM states_clean
    """).fetchone()

    print(f"Rows in states_clean: {count}")
    print(f"Time range: {min_ts} → {max_ts}")

    con.close()

if __name__ == "__main__":
    main()