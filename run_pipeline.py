import os
import shutil
import subprocess
import time

BATCHES_DIR = 'mock_source_batches'
INCOMING_DIR = 'data/incoming'

def process_batches():
    while True:
        # Find the next batch
        batches = sorted([f for f in os.listdir(BATCHES_DIR) if f.endswith('.csv')])

        if batches:
            next_batch = batches[0]
            source = os.path.join(BATCHES_DIR, next_batch)

            print(f"\nNew data: {next_batch}")

            # Moving files to the pipeline
            os.makedirs(INCOMING_DIR, exist_ok=True)
            destination = os.path.join(INCOMING_DIR, next_batch)
            shutil.copy(source, destination)
            os.remove(source)

            # Trigger DVC
            subprocess.run("dvc repro", shell=True)

        else:
            # if no more files, sleep and check again later.
            time.sleep(5)

if __name__ == '__main__':
    try:
        process_batches()
    except KeyboardInterrupt:
        print("\nStopping automation...")