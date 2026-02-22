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

    full_date_range = pd.date_range(start=df['date'].min(),
                                    end=df['date'].max(), freq='D')

    df = df.set_index('date').reindex(full_date_range)

    # new values
    df = df.interpolate(method='linear')

    df.index.name = 'date'
    df = df.reset_index()

    df = df.dropna()

    os.makedirs(os.path.dirname(SILVER_FILE), exist_ok=True)
    df.to_csv(SILVER_FILE, index=False)
    print(f"Silver updated: {len(df)} rows.")

if __name__ == "__main__":
    clean_data()