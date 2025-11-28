import pandas as pd

def test_sessions_have_start_and_end():
    df = pd.read_csv("data/silver/sessions.csv")
    assert df["session_start"].notnull().all()
    assert df["session_end"].notnull().all()

def test_session_length_non_negative():
    df = pd.read_csv("data/silver/sessions.csv")
    assert (df["session_length_minutes"] >= 0).all()
