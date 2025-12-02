from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DB_PATH = Path("data/opensky.duckdb")
PLOTS_DIR = Path("plots")

def plot_aircraft_over_time(con):
    # Number of aircraft observed over time
    df = con.execute("""
        SELECT snapshot_ts, COUNT(*) AS count
        FROM states_clean
        GROUP BY snapshot_ts
        ORDER BY snapshot_ts;
    """).fetch_df()

    # Line plot of aircraft count over time
    plt.figure(figsize=(10, 5))
    plt.plot(df["snapshot_ts"], df["count"], marker='o', markersize=4, linewidth=1.8, color="skyblue")
    plt.title("Aircraft Observed Over Time")
    plt.xlabel("Time")
    plt.ylabel("Number of Aircraft")
    
    # Format x-axis to show readable times
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)

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
    plt.bar(df["origin_country"], df["count"], color="skyblue")
    plt.title("Top 10 Origin Countries of Aircraft")
    plt.xlabel("Country")
    plt.ylabel("Aircraft Count")
    plt.xticks(rotation=45)

    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)

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
    plt.hist(df["speed_knots"], bins=40, color="skyblue")
    plt.title("Distribution of Aircraft Speeds (knots)")
    plt.xlabel("Speed (knots)")
    plt.ylabel("Number of Aircraft")

    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)

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