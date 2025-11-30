from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = Path("data/opensky.duckdb")
PLOTS_DIR = Path("plots")

def plot_aircraft_over_time(con):
    # Count rows per minute to see how flight volume changes over time
    df = con.execute("""
        SELECT snapshot_minute, COUNT(*) AS count
        FROM states_clean
        GROUP BY snapshot_minute
        ORDER BY snapshot_minute
    """).fetch_df()

    # Plot to show flight volume over time
    plt.figure(figsize=(10, 5))
    plt.plot(df["snapshot_minute"], df["count"], marker='o')
    plt.title("Aircraft Observed Over Time (per minute)")
    plt.xlabel("Time")
    plt.ylabel("Number of Aircraft")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "aircraft_over_time.png")
    plt.close()

def plot_top_countries(con):
    # Top 10 countries by number of aircraft in our dataset
    df = con.execute("""
        SELECT origin_country, COUNT(*) AS count
        FROM states_clean
        GROUP BY origin_country
        ORDER BY count DESC
        LIMIT 10
    """).fetch_df()

    # Bar plot of top countries
    plt.figure(figsize=(10, 5))
    plt.bar(df["origin_country"], df["count"])
    plt.title("Top 10 Origin Countries of Aircraft")
    plt.xlabel("Country")
    plt.ylabel("Aircraft Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "top_countries.png")
    plt.close()

def plot_speed_histogram(con):
    # Distribution of aircraft speeds in knots
    df = con.execute("""
        SELECT speed_knots
        FROM states_clean
        WHERE speed_knots IS NOT NULL
    """).fetch_df()

    # Histogram of speeds
    plt.figure(figsize=(10, 5))
    plt.hist(df["speed_knots"], bins=40)
    plt.title("Distribution of Aircraft Speeds (knots)")
    plt.xlabel("Speed (knots)")
    plt.ylabel("Number of Aircraft")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "speed_histogram.png")
    plt.close()


def main():
    PLOTS_DIR.mkdir(exist_ok=True)
    con = duckdb.connect(DB_PATH)

    plot_aircraft_over_time(con)
    plot_top_countries(con)
    plot_speed_histogram(con)

    print("Plots saved to:", PLOTS_DIR)


if __name__ == "__main__":
    main()