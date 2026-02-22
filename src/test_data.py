import pandas as pd
import sys

SILVER_FILE = 'data/silver/cleaned_data.csv'


def run_pipeline_tests():

    print("\n\nAutomated tests:")

    try:
        df = pd.read_csv(SILVER_FILE)
        df['date'] = pd.to_datetime(df['date'])

        # Absence of duplicated days
        duplicates = df['date'].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicated days!"
        print("No duplicated days detected.")

        # Continuity of the time
        expected_days = (df['date'].max() - df['date'].min()).days + 1
        actual_days = len(df)
        assert expected_days == actual_days, f"Expected {expected_days} continuous days, but found {actual_days}."
        print("No missing days")

        # Expected value ranges for climate variables
        assert df['meantemp'].between(0,50).all(), "meantemp in range 0-50"
        assert df['humidity'].between(0,100).all(), "humidity values in range 0-100"
        print("Climate variables are within the range.")


        print("All data quality tests passed")


    except AssertionError as e:
        print("Data validation failed")
        print(f"Reason: {e}")
        sys.exit(1)  # Tells DVC test failed and stops the pipeline

if __name__ == "__main__":
    run_pipeline_tests()