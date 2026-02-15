import pandas as pd
import os

BRONZE_FILE = 'data/bronze/cumulative_raw.csv'
SILVER_FILE = 'data/silver/cleaned_data.csv'


def clean_data():
    df = pd.read_csv(BRONZE_FILE)

    # Make sure the dates are formatted correctly and in correct order
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Handle missing or duplicated lines
    df = df.drop_duplicates(subset=['date'])
    df = df.dropna()

    os.makedirs(os.path.dirname(SILVER_FILE), exist_ok=True)
    df.to_csv(SILVER_FILE, index=False)
    print(f"Silver updated: {len(df)} rows.")


if __name__ == "__main__":
    clean_data()