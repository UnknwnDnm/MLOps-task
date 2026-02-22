import pandas as pd
import os
import shutil

SOURCE_FILE = 'train.csv'  #  train file
OUTPUT_DIR = 'mock_source_batches'  # save batches
NUM_BATCHES = 5
DATA_DIR = 'data' # pipeline data to clean

def clean_environment():
    # Wipes out old CSV files from the data folders before new split
    print("Cleaning old pipeline data.")
    if os.path.exists(DATA_DIR):
        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Error deleting {file_path}: {e}")

def split_dataset():

    clean_environment()

    # Read the data
    if not os.path.exists(SOURCE_FILE):
        print(f"Error: {SOURCE_FILE} not found!")
        return

    print(f"Reading {SOURCE_FILE}")
    df = pd.read_csv(SOURCE_FILE)

    # Make sure data is sorted
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

    # Calculate the split size
    total_rows = len(df)
    batch_size = total_rows // NUM_BATCHES

    # save files
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)

    print(f"Splitting {total_rows} rows into {NUM_BATCHES} batches.")

    # Split and save
    for i in range(NUM_BATCHES):
        start_idx = i * batch_size
        # remaining rows added to the last batch
        end_idx = (i + 1) * batch_size if i < NUM_BATCHES - 1 else total_rows

        batch_df = df.iloc[start_idx:end_idx]

        batch_filename = f"batch_{i + 1}.csv"
        batch_path = os.path.join(OUTPUT_DIR, batch_filename)

        # Save without index to keep it clean
        batch_df.to_csv(batch_path, index=False)
        print(f"Saved {batch_filename}: {len(batch_df)} rows ({batch_df['date'].min().date()} to {batch_df['date'].max().date()})")

    print("Batches saved in folder:", OUTPUT_DIR)


if __name__ == "__main__":
    split_dataset()