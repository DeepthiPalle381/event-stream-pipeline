import pandas as pd

def test_events_by_minute_counts_positive():
    df = pd.read_csv("data/gold/events_by_minute.csv")
    assert (df["events_count"] >= 0).all()

def test_events_by_type_not_empty():
    df = pd.read_csv("data/gold/events_by_type.csv")
    assert len(df) > 0

def test_user_funnel_has_user_id():
    df = pd.read_csv("data/gold/user_funnel.csv")
    assert "user_id" in df.columns
