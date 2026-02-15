import pandas as pd
import os

SILVER_FILE = 'data/silver/cleaned_data.csv'
GOLD_FILE = 'data/gold/ml_ready.csv'


def create_ml_ready_data():
    # load silver data
    df = pd.read_csv(SILVER_FILE)
    df = df.sort_values('date')

    # Yesterday's mean temperature
    df['yesterday_meantemp'] = df['meantemp'].shift(1)

    # tomorrow's mean temperature
    df['tomorrow_meantemp'] = df['meantemp'].shift(-1)

    # Remove rows with NaN values created by shifting
    df = df.dropna()

    # Keep the colums we need for the model
    cols_to_keep = ['date', 'yesterday_meantemp', 'meantemp', 'tomorrow_meantemp']
    df_gold = df[cols_to_keep]

    os.makedirs(os.path.dirname(GOLD_FILE), exist_ok=True)
    df_gold.to_csv(GOLD_FILE, index=False)
    print(f"Gold updated: {len(df_gold)} rows ready.")


if __name__ == "__main__":
    create_ml_ready_data()