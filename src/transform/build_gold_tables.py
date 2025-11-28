import pandas as pd
from pathlib import Path

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

# Make sure these match your actual column names
TIMESTAMP_COL = "event_time"
EVENT_TYPE_COL = "event_type"


def load_events():
    df = pd.read_csv(SILVER_DIR / "events_silver.csv")
    df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL])
    return df


def build_events_by_minute(df: pd.DataFrame) -> pd.DataFrame:
    # round down to minute
    df["minute"] = df[TIMESTAMP_COL].dt.floor("Min")
    out = (
        df.groupby("minute")
        .size()
        .reset_index(name="events_count")
        .sort_values("minute")
    )
    return out


def build_events_by_type(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(EVENT_TYPE_COL)
        .size()
        .reset_index(name="events_count")
        .sort_values("events_count", ascending=False)
    )
    return out


def build_user_funnel(df: pd.DataFrame) -> pd.DataFrame:
    # counts of events per user & type
    pivot = (
        df.pivot_table(
            index="user_id",
            columns=EVENT_TYPE_COL,
            values="session_id",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )

    # Rename columns to be more readable (if present)
    rename_map = {}
    for col in pivot.columns:
        new_col = col
        if col == "view":
            new_col = "views"
        if col == "cart":
            new_col = "carts"
        if col == "purchase":
            new_col = "purchases"
        rename_map[col] = new_col

    pivot = pivot.rename(columns=rename_map)
    return pivot


def main():
    print("🔹 Loading silver events...")
    df = load_events()

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    print("🔹 Building events_by_minute...")
    events_by_minute = build_events_by_minute(df)
    events_by_minute.to_csv(GOLD_DIR / "events_by_minute.csv", index=False)

    print("🔹 Building events_by_type...")
    events_by_type = build_events_by_type(df)
    events_by_type.to_csv(GOLD_DIR / "events_by_type.csv", index=False)

    print("🔹 Building user funnel...")
    user_funnel = build_user_funnel(df)
    user_funnel.to_csv(GOLD_DIR / "user_funnel.csv", index=False)

    print("🎉 Gold layer tables created.")


if __name__ == "__main__":
    main()
