# MLOps-task

## Execution flow
1. Run the split_data.py which splits train.csv into 5 batches.
2. Batches are stored in the mock_source_batches folder.
3. run_pipeline.py or manual (run_pipeline_manual.py) moves one batch at a time into data/incoming/.
4. dvc repro is triggered.
5. DVC executes the pipeline in order:
   1. Bronze: Appends raw data.
   2. Silver: Cleans the data, handles duplicates and adds the missing days.
   3. Test: Validates the data quality.
   4. Gold: Creates ML-ready dataset.
6. If validation fails, the pipeline stops.
7. If it's successful, a new Gold dataset version is produced.

## Assumptions
- Pipeline is executed in a local environment.
- The original train.csv should be in the root directory for split_data.py to generate batches.
- CSV batches will have a date column.
- Test validation will assume the temperatures are realistic.

