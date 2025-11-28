import pandas as pd

# Path to your full dataset (the big file)
full_path = "data/raw/events_raw_full.csv"

# Path where the smaller sample will be saved
sample_path = "data/raw/events_raw.csv"

# Read only the first 100,000 rows
df = pd.read_csv(full_path, nrows=100000)

# Save the smaller dataset
df.to_csv(sample_path, index=False)

print(f"Sampled {len(df)} rows and saved to {sample_path}")
