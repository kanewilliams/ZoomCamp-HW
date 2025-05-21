import marimo

__generated_with = "0.13.10"
app = marimo.App(
    width="medium",
    app_title="MLOps Homework 01",
    auto_download=["html"],
)


@app.cell
def _(mo):
    mo.md(
        r"""
    #MLOps Zoomcamp Module01 Homework

    - Kane Williams (https://github.com/kanewilliams)
    - Module 01 is available [here](github.com/DataTalksClub/mlops-zoomcamp/blob/main/01-intro).
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import pandas as pd
    import numpy as np
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error
    import matplotlib.pyplot as plt
    return DictVectorizer, LinearRegression, mean_squared_error, np, pd


@app.cell
def _(pd):
    jan_data = pd.read_parquet('../data/yellow_tripdata_2023-01.parquet')
    return (jan_data,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q1. Downloading the data

    We'll use [the same NYC taxi dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
    but instead of \"**Green** Taxi Trip Records\", we'll use \"**Yellow** Taxi Trip Records\".

    Download the data for January and February 2023.

    Read the data for January. How many columns are there?

    * 16
    * 17
    * 18
    * **19**
    """
    )
    return


@app.cell
def _(jan_data):
    jan_data.shape
    return


@app.cell
def q1(jan_data, mo):

    # Answer to Q1
    mo.md("Number of columns is " + str(jan_data.shape[1]))
    return


@app.cell
def _(mo):
    mo.md("""### Preview of January data:""")
    return


@app.cell
def _(jan_data):
    jan_data
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q2. Computing duration
    Now let's compute the duration variable. It should contain the duration of a ride in minutes.

    What's the standard deviation of the trips duration in January?

    - 32.59
    - **42.59**
    - 52.59
    - 62.59
    """
    )
    return


@app.cell
def q2(jan_data, mo):
    df = jan_data.copy()
    df['duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    duration_std = df['duration'].std()
    mo.md(f"Standard deviation of trip duration: {duration_std:.2f} minutes")
    return (df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q3. Dropping outliers

    Next, we need to check the distribution of the `duration` variable. There are some outliers. Let's remove them and keep only the records where the duration was between 1 and 60 minutes (inclusive).

    What fraction of the records left after you dropped the outliers?

    * 90%
    * 92%
    * 95%
    * **98%**
    """
    )
    return


@app.cell
def q3(df, mo):
    total_records = len(df)
    df_filtered = df[(df['duration'] >= 1) & (df['duration'] <= 60)]
    filtered_records = len(df_filtered)
    fraction_kept = filtered_records / total_records
    mo.md(f"Fraction of records kept: {fraction_kept:.2%}")
    return (df_filtered,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q4. One-hot encoding

    Let's apply one-hot encoding to the pickup and dropoff location IDs. We'll use only these two features for our model.

    * Turn the dataframe into a list of dictionaries (remember to re-cast the ids to strings - otherwise it will
      label encode them)
    * Fit a dictionary vectorizer
    * Get a feature matrix from it

    What's the dimensionality of this matrix (number of columns)?

    * 2
    * 155
    * 345
    * **515**
    * 715
    """
    )
    return


@app.cell
def q4(DictVectorizer, df_filtered, mo):
    dict_list = df_filtered[['PULocationID', 'DOLocationID']].to_dict(orient='records')
    for di in dict_list:
        di['PULocationID'] = str(di['PULocationID'])
        di['DOLocationID'] = str(di['DOLocationID'])
    dv = DictVectorizer()
    X_train = dv.fit_transform(dict_list)
    feature_dim = X_train.shape[1]
    mo.md(f"Dimensionality of feature matrix: {feature_dim} columns")
    return X_train, dv


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q5. Training a model

    Now let's use the feature matrix from the previous step to train a model.

    * Train a plain linear regression model with default parameters, where duration is the response variable
    * Calculate the RMSE of the model on the training data

    What's the RMSE on train?

    * 3.64
    * **7.64**
    * 11.64
    * 16.64
    """
    )
    return


@app.cell
def q5(LinearRegression, X_train, df_filtered, mean_squared_error, mo, np):
    y_train = df_filtered['duration'].values
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_pred))
    mo.md(f"RMSE on training data: {train_rmse:.2f}")
    return (lr,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Q6. Evaluating the model

    Now let's apply this model to the validation dataset (February 2023).

    What's the RMSE on validation?

    * 3.81
    * **7.81**
    * 11.81
    * 16.81
    """
    )
    return


@app.cell
def q6(dv, lr, mean_squared_error, mo, np, pd):
    feb_data = pd.read_parquet('../data/yellow_tripdata_2023-02.parquet')
    feb_data['duration'] = (feb_data['tpep_dropoff_datetime'] - feb_data['tpep_pickup_datetime']).dt.total_seconds() / 60
    feb_data_filtered = feb_data[(feb_data['duration'] >= 1) & (feb_data['duration'] <= 60)]

    val_dict_list = feb_data_filtered[['PULocationID', 'DOLocationID']].to_dict(orient='records')
    for d in val_dict_list:
        d['PULocationID'] = str(d['PULocationID'])
        d['DOLocationID'] = str(d['DOLocationID'])

    X_val = dv.transform(val_dict_list)
    y_val = feb_data_filtered['duration'].values
    y_val_pred = lr.predict(X_val)
    val_rmse = np.sqrt(mean_squared_error(y_val, y_val_pred))
    mo.md(f"RMSE on validation data: {val_rmse:.2f}")
    return


if __name__ == "__main__":
    app.run()
