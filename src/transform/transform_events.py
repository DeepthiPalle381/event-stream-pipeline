import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

# 👉 Make sure these match your real column names
TIMESTAMP_COL = "event_time"   # e.g. event_time / event_timestamp / timestamp
USER_COL = "user_id"           # e.g. user_id / uid / customer_id
EVENT_TYPE_COL = "event_type"  # e.g. event_type / event / action

SESSION_GAP_MINUTES = 30  # new session if gap > 30 minutes


def load_bronze_events() -> pd.DataFrame:
    files = sorted(BRONZE_DIR.glob("events_*.csv"))
    if not files:
        raise FileNotFoundError(f"No bronze files found in {BRONZE_DIR}")

    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    # parse timestamp
    df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL])
    return df


def standardize_event_type(df: pd.DataFrame) -> pd.DataFrame:
    # lower-case + strip spaces
    df[EVENT_TYPE_COL] = (
        df[EVENT_TYPE_COL]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # map common variants to canonical names
    mapping = {
        "view": "view",
        "product_view": "view",
        "pageview": "view",
        "page_view": "view",
        "cart": "cart",
        "add_to_cart": "cart",
        "add-cart": "cart",
        "purchase": "purchase",
        "buy": "purchase",
        "order": "purchase",
    }

    df[EVENT_TYPE_COL] = df[EVENT_TYPE_COL].map(lambda x: mapping.get(x, x))
    return df


def add_sessions(df: pd.DataFrame) -> pd.DataFrame:
    # sort by user + time
    df = df.sort_values([USER_COL, TIMESTAMP_COL]).reset_index(drop=True)

    # previous event time per user
    df["prev_time"] = df.groupby(USER_COL)[TIMESTAMP_COL].shift(1)

    # gap in minutes
    df["gap_minutes"] = (
        (df[TIMESTAMP_COL] - df["prev_time"]).dt.total_seconds() / 60.0
    )

    # start of new session?
    df["new_session"] = df["prev_time"].isna() | (df["gap_minutes"] > SESSION_GAP_MINUTES)

    # session counter per user
    df["session_index"] = df.groupby(USER_COL)["new_session"].cumsum()

    # session_id = user_id-session_index
    df["session_id"] = (
        df[USER_COL].astype(str)
        + "-"
        + df["session_index"].astype(int).astype(str)
    )

    return df


def build_sessions_table(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("session_id")

    sessions = grp.agg(
        user_id=(USER_COL, "first"),
        session_start=(TIMESTAMP_COL, "min"),
        session_end=(TIMESTAMP_COL, "max"),
        events_count=("session_id", "size"),
        unique_event_types=(EVENT_TYPE_COL, "nunique"),
    ).reset_index()

    sessions["session_length_minutes"] = (
        (sessions["session_end"] - sessions["session_start"]).dt.total_seconds() / 60.0
    )

    return sessions


def main():
    print("🔹 Loading bronze events...")
    df = load_bronze_events()

    # basic sanity checks
    for col in [USER_COL, EVENT_TYPE_COL]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found. Columns: {list(df.columns)}")

    print("🔹 Standardizing event_type...")
    df = standardize_event_type(df)

    print("🔹 Adding session_id per user...")
    df = add_sessions(df)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # save detailed events (drop helper columns)
    events_silver = df.drop(columns=["prev_time", "gap_minutes", "new_session"])
    events_silver.to_csv(SILVER_DIR / "events_silver.csv", index=False)
    print(f"✅ Saved silver events → {SILVER_DIR / 'events_silver.csv'}")

    # build sessions table
    sessions = build_sessions_table(df)
    sessions.to_csv(SILVER_DIR / "sessions.csv", index=False)
    print(f"✅ Saved sessions table → {SILVER_DIR / 'sessions.csv'}")

    print("🎉 Silver layer complete.")


if __name__ == "__main__":
    main()
