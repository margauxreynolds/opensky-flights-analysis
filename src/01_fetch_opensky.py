from pathlib import Path
import duckdb
import requests
import time
from prefect import flow, task, get_run_logger

DB_PATH = Path("data/opensky.duckdb")

@task
def init_db() -> str:
    """
    Task 1: Create the DuckDB database and table if they don't exist.
    Returns the path to the database as a string.
    """
    logger = get_run_logger()
    try:
        # Make sure the data folder exists
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Connect to DuckDB (creates file if it doesn't exist)
        con = duckdb.connect(DB_PATH)

        # Drop tables if they already exist
        con.execute("DROP TABLE IF EXISTS states_raw;")
        con.execute("DROP TABLE IF EXISTS states_clean;")

        # Create a table for raw OpenSky data if it's not already there
        con.execute("""
            CREATE TABLE IF NOT EXISTS states_raw (
                snapshot_time BIGINT,
                icao24 TEXT,
                callsign TEXT,
                origin_country TEXT,
                time_position BIGINT,
                last_contact BIGINT,
                longitude DOUBLE,
                latitude DOUBLE,
                baro_altitude DOUBLE,
                on_ground BOOLEAN,
                velocity DOUBLE,
                true_track DOUBLE,
                vertical_rate DOUBLE,
                geo_altitude DOUBLE,
                squawk TEXT,
                spi BOOLEAN,
                position_source INTEGER
            );
        """)
        con.close() # Close the DB connection when done
        logger.info(f"Initialized DuckDB at {DB_PATH}")
    except Exception as e:
        logger.error(f"Error initializing DuckDB: {e}")
        raise
    return str(DB_PATH)

@task
def fetch_opensky_snapshot() -> tuple[int, list]:
    """
    Task 2: Call the OpenSky API and return (snapshot_time, states_list).
    """
    logger = get_run_logger()
    url = "https://opensky-network.org/api/states/all"

    try:
        logger.info(f"Requesting OpenSky data from {url}")
        resp = requests.get(url, timeout=30)
        logger.info(f"OpenSky response status: {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()       # parse JSON
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while calling OpenSky API: {e}")
        raise
    except ValueError as e:
        # JSON parsing failed
        logger.error(f"Failed to parse OpenSky JSON response: {e}")
        raise

    # Extract the timestamp and list of states from the response
    snapshot_time = data.get("time")
    states = data.get("states") or []

    if snapshot_time is None:
        logger.warning("No 'time' field in OpenSky response.")
    if not states:
        logger.warning("OpenSky returned an empty 'states' list.")

    logger.info(f"Fetched {len(states)} state vectors at time={snapshot_time}")
    return snapshot_time, states


@task
def insert_states(db_path: str, snapshot_time: int, states: list) -> int:
    """
    Task 3: Insert the fetched states into the DuckDB table.
    Returns the total row count in states_raw after insert.
    """
    logger = get_run_logger()

    # Make sure snapshot_time is valid
    if snapshot_time is None or not isinstance(snapshot_time, int):
        logger.error(f"Invalid snapshot_time: {snapshot_time}")
        raise ValueError("snapshot_time must be a valid integer timestamp.")
    # If no states, log and return current row count
    if not states:
        logger.warning("No states to insert; skipping insert.")
        con = duckdb.connect(db_path)
        count = con.execute("SELECT COUNT(*) FROM states_raw").fetchone()[0]
        con.close()
        return count
    # Convert each OpenSky "state" list into a tuple for insertion
    rows = []
    for s in states:
        # Ensure OpenSky states have at least 17 elements
        if not isinstance(s, list) or len(s) < 17:
            continue

        rows.append((
            snapshot_time,        # snapshot_time
            s[0],                 # icao24
            s[1].strip() if s[1] else None, # callsign (strip spaces)
            s[2],                 # origin_country
            s[3],                 # time_position
            s[4],                 # last_contact
            s[5],                 # longitude
            s[6],                 # latitude
            s[7],                 # baro_altitude
            bool(s[8]) if s[8] is not None else None, # on_ground
            s[9],                 # velocity
            s[10],                # true_track
            s[11],                # vertical_rate
            s[13],                # geo_altitude
            s[14],                # squawk
            bool(s[15]) if s[15] is not None else None, # spi
            s[16],                # position_source
        ))

    try:
        # Insert all rows into the states_raw table
        con = duckdb.connect(db_path)
        con.executemany("""
            INSERT INTO states_raw VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, rows)
        # Get total row count after insert
        count = con.execute("SELECT COUNT(*) FROM states_raw").fetchone()[0]
        con.close()
    except Exception as e:
        logger.error(f"Error inserting rows into DuckDB: {e}")
        raise

    logger.info(f"Inserted {len(rows)} rows; total rows in states_raw = {count}")
    return count


@flow
def fetch_single_opensky_snapshot():
    """
    Prefect flow: 
    1. init_db
    2. fetch_opensky_snapshot
    3. insert_states
    """
    logger = get_run_logger()
    logger.info("Starting OpenSky snapshot flow.")

    db_path = init_db()
    snapshot_time, states = fetch_opensky_snapshot()
    total_rows = insert_states(db_path, snapshot_time, states)

    logger.info(
        f"Flow complete. Snapshot time={snapshot_time}, "
        f"rows fetched={len(states)}, total rows in DB={total_rows}"
    )

@flow
def fetch_many_opensky_snapshots(num_snapshots: int, sleep_seconds: int):
    """
    Prefect flow: take multiple OpenSky snapshots in a loop.
    - num_snapshots: how many times to call the API
    - sleep_seconds: how many seconds to wait between calls
    """
    logger = get_run_logger()
    logger.info(
        f"Starting multi-snapshot flow: num_snapshots={num_snapshots}, "
        f"sleep_seconds={sleep_seconds}"
    )

    # Make sure DB and table exist
    db_path = init_db()

    total_rows = 0

    # Loop for each snapshot
    for i in range(num_snapshots):
        logger.info(f"Snapshot {i+1}/{num_snapshots}: fetching from OpenSky...")

        snapshot_time, states = fetch_opensky_snapshot()
        total_rows = insert_states(db_path, snapshot_time, states)

        logger.info(
            f"Snapshot {i+1}/{num_snapshots} complete: "
            f"fetched={len(states)}, total_rows={total_rows}"
        )

        # Sleep between snapshots (except after the last one)
        if i < num_snapshots - 1:
            logger.info(f"Sleeping {sleep_seconds} seconds before next snapshot...")
            time.sleep(sleep_seconds)

    logger.info(
        f"Multi-snapshot flow finished. "
        f"Total rows in states_raw after run = {total_rows}"
    )

if __name__ == "__main__":
    fetch_many_opensky_snapshots(
        num_snapshots=15, # Number of snapshots to fetch
        sleep_seconds=10 # Number of seconds between API calls
    )