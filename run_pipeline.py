import os
import shutil
import subprocess

BATCHES_DIR = 'mock_source_batches'
INCOMING_FILE = 'data/incoming/batch.csv'


def run():
    # Find the next batch
    batches = sorted([f for f in os.listdir(BATCHES_DIR) if f.endswith('.csv')])
    if not batches:
        print("All batches processed")
        return

    next_batch = batches[0]
    source = os.path.join(BATCHES_DIR, next_batch)

    # Feed it to the pipeline's incoming folder and file
    os.makedirs(os.path.dirname(INCOMING_FILE), exist_ok=True)
    shutil.copy(source, INCOMING_FILE)
    os.remove(source)

    print(f"Ingesting {next_batch}")

    # Trigger the whole DataOps pipeline via DVC
    subprocess.run("dvc repro", shell=True)

if __name__ == '__main__':
    run()