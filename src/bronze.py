import pandas as pd
import os
import glob

INCOMING_DIR = 'data/incoming'
BRONZE_FILE = 'data/bronze/cumulative_raw.csv'

def ingest_to_bronze():
    # Find all the batch files in the folder
    all_files = glob.glob(os.path.join(INCOMING_DIR, "*.csv"))

    # Read and combine all of them
    df_list = [pd.read_csv(f) for f in all_files]
    combined = pd.concat(df_list, ignore_index=True)

    os.makedirs(os.path.dirname(BRONZE_FILE), exist_ok=True)
    combined.to_csv(BRONZE_FILE, index=False)
    print(f"Bronze updated: {len(combined)} rows.")

if __name__ == "__main__":
    ingest_to_bronze()