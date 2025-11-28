import pandas as pd
from pathlib import Path

# File paths
RAW_FILE = Path("data/raw/events_raw.csv")
BRONZE_DIR = Path("data/bronze")

# 👇 Change this if your timestamp column has a different name
TIMESTAMP_COL = "event_time"  # options: event_time, timestamp, event_timestamp

def main():
    print("🔹 Reading raw events from:", RAW_FILE)

    df = pd.read_csv(RAW_FILE)

    # Validate timestamp exists
    if TIMESTAMP_COL not in df.columns:
        raise ValueError(
            f"Timestamp column '{TIMESTAMP_COL}' not found. "
            f"Available columns: {list(df.columns)}"
        )

    print("🔹 Parsing timestamps...")
    df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL])

    # Sort by time
    df = df.sort_values(TIMESTAMP_COL).reset_index(drop=True)

    # Add date part for partitioning
    df["event_date"] = df[TIMESTAMP_COL].dt.date

    # Create bronze directory if not exists
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    print("🔹 Writing bronze partition files...")
    for event_date, group in df.groupby("event_date"):
        out_path = BRONZE_DIR / f"events_{event_date}.csv"
        group.to_csv(out_path, index=False)
        print(f"   ✔ {len(group)} rows → {out_path}")

    print("🎉 Bronze ingestion complete!")

if __name__ == "__main__":
    main()
