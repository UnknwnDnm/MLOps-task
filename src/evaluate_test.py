import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from sklearn.metrics import mean_squared_error

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_FILE = os.path.join(BASE_DIR, 'data', 'test.csv')

db_path = os.path.join(BASE_DIR, "mlflow.db")
mlflow.set_tracking_uri(f"sqlite:///{db_path}")

def evaluate_production_data():
    print("Loading test data")
    df = pd.read_csv(TEST_FILE)
    df = df.sort_values('date')

    # Create same features used during training
    df['yesterday_meantemp'] = df['meantemp'].shift(1)
    df['tomorrow_meantemp'] = df['meantemp'].shift(-1)  # target we want to predict
    df = df.dropna()

    X_test = df[['yesterday_meantemp', 'meantemp']]
    y_test = df['tomorrow_meantemp']

    # fetch best model form the mlflow (lowest RMSE)
    print("Fetching best model")
    runs = mlflow.search_runs(experiment_names=["Meantemp_forecasting"], order_by=["metrics.rmse ASC"])

    if runs.empty:
        print("No models found in mlflow. Run training pipeline first.")
        return

    best_run_id = runs.iloc[0].run_id

    model_uri = f"runs:/{best_run_id}/xgboost_model"
    model = mlflow.xgboost.load_model(model_uri)

    # predict and evaluate
    preds = model.predict(X_test)
    final_rmse = np.sqrt(mean_squared_error(y_test, preds))


    print(f"\nProduction evaluation")
    print(f"Model run ID: {best_run_id}")
    print(f"Production RMSE: {final_rmse:.4f}")

if __name__ == "__main__":
    evaluate_production_data()