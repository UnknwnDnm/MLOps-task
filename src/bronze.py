import pandas as pd
import os

INCOMING_FILE = 'data/incoming/batch.csv'
BRONZE_FILE = 'data/bronze/cumulative_raw.csv'


def ingest_to_bronze():
    new_data = pd.read_csv(INCOMING_FILE)

    if os.path.exists(BRONZE_FILE):
        existing_data = pd.read_csv(BRONZE_FILE)
        # Append new data to existing data
        combined = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        os.makedirs(os.path.dirname(BRONZE_FILE), exist_ok=True)
        combined = new_data

    combined.to_csv(BRONZE_FILE, index=False)
    print(f"Bronze updated: {len(combined)} rows.")


if __name__ == "__main__":
    ingest_to_bronze()