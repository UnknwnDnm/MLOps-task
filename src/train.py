import pandas as pd
import numpy as np
import os
import hashlib
import mlflow
import mlflow.xgboost
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error

GOLD_FILE = 'data/gold/ml_ready.csv'


def get_data_version(file_path):
    hasher = hashlib.md5()
    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()
    return "No file"


def calculate_stability_kpi(current_rmse):

    # Calculate performance stability across past batches.
    try:
        # Get past runs from mlflow
        past_runs = mlflow.search_runs(filter_string="metrics.rmse > 0")
        if past_runs.empty:
            return 0.0  # if first run, stability is perfect

        past_rmses = past_runs['metrics.rmse'].tolist()
        past_rmses.append(current_rmse)

        # Stability is standard deviation of RMSE values
        return np.std(past_rmses)
    except Exception:
        return 0.0


def train_model():
    print("Loading Gold dataset for training")
    df = pd.read_csv(GOLD_FILE)

    # input features and target
    X = df[['yesterday_meantemp', 'meantemp']]
    y = df['tomorrow_meantemp']

    # 80% train, 20% validation
    split_index = int(len(df) * 0.8)
    X_train, X_val = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_val = y.iloc[:split_index], y.iloc[split_index:]

    data_version = get_data_version(GOLD_FILE)

    mlflow.set_experiment("Meantemp_forecasting")

    # fetch previous runs
    previous_rmse = None
    try:
        previous_runs = mlflow.search_runs(
            experiment_names=["Meantemp_forecasting"],
            filter_string="status = 'FINISHED'",
            order_by=["start_time DESC"]
        )
        if not previous_runs.empty and 'metrics.rmse' in previous_runs.columns:
            previous_rmse = previous_runs.iloc[0]["metrics.rmse"]

    except Exception as e:
        print(f"Could not fetch previous run. {e}")

    with mlflow.start_run():
        print(f"Data version: {data_version}")

        # XGBoost hyperparameters
        params = {
            "n_estimators": 150,
            "learning_rate": 0.05,
            "max_depth": 4,
            "random_state": 42,
            "objective": "reg:squarederror"
        }
        mlflow.log_params(params)

        # train XGBoost model
        model = XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        # Predict and calculate first KPI (RMSE)
        preds = model.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, preds))

        # Calculate second KPI (Stability across batches)
        stability_score = calculate_stability_kpi(rmse)

        # Comparison output:
        print("\nModel version comparison.")
        print("----------------------------")
        if previous_rmse is not None and not np.isnan(previous_rmse):

            rmse_change_pct = ((rmse - previous_rmse) / previous_rmse) * 100
            print(f"Previous model RMSE: {previous_rmse:.4f}")
            print(f"Current model RMSE: {rmse:.4f}")
            print(f"Change (%): {rmse_change_pct:.2f}%")

            mlflow.log_metric("rmse_change_pct", rmse_change_pct)

        else:
            print("First run, no previous model to compare against.")
            print(f"Current Model RMSE  : {rmse:.4f}")

        print("\n")

        # Log metrics
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("stability_std_dev", stability_score)
        mlflow.log_metric("training_data_rows", len(X_train))

        # Log the linkage between data and model
        mlflow.set_tag("data_version_hash", data_version)
        mlflow.set_tag("model_type", "xgboost")

        # Log the actual model
        mlflow.xgboost.log_model(model, "xgboost_model")

        print(f"Training complete. Final RMSE: {rmse:.4f} and stability score: {stability_score:.4f}")


if __name__ == "__main__":
    train_model()