from pathlib import Path
import duckdb

DB_PATH = Path("data/opensky.duckdb")

def main():
    con = duckdb.connect(DB_PATH)

    # How many rows do we have?
    count = con.execute("SELECT COUNT(*) FROM states_raw").fetchone()[0]
    print("Total rows in states_raw:", count)

    # Show a few example rows
    sample = con.execute("""
        SELECT
            snapshot_time,
            icao24,
            callsign,
            origin_country,
            longitude,
            latitude,
            velocity
        FROM states_raw
        LIMIT 5
    """).fetchall()

    print("\nSample rows:")
    for row in sample:
        print(row)

    con.close()

if __name__ == "__main__":
    main()