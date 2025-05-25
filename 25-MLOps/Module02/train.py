import os
import pickle
import mlflow
import click

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error  # Updated...


def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


@click.command()
@click.option(
    "--data_path",
    default="../data_preprocessed",
    help="Location where the processed NYC taxi trip data was saved"
)
def run_train(data_path: str):

    X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
    X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))

    mlflow.sklearn.autolog()

    with mlflow.start_run():

        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_val)

        # Newer version of sk-learn so needs *root*_mean_squared_error
        rmse = root_mean_squared_error(y_val, y_pred)


if __name__ == '__main__':
    run_train()
