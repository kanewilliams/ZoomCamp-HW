#!/usr/bin/env python
"""Run Q1 and Q2 from the homework"""

import pickle
import pandas as pd
import numpy as np
import os

# Load the model
with open('homework_files/model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

# Read March 2023 data
print("Reading March 2023 data...")
df = read_data('/home/kanewsl/repos/ZoomCamp-HW/25-MLOps/data/yellow/yellow_tripdata_2023-03.parquet')
print(f"Loaded {len(df)} records")

# Prepare features and make predictions
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

# Q1: Calculate standard deviation
std_dev = np.std(y_pred)
print(f"\nQ1 Answer - Standard deviation: {std_dev:.2f}")

# Q2: Prepare output with ride_id
year = 2023
month = 3

df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

# Create dataframe with results
df_result = pd.DataFrame()
df_result['ride_id'] = df['ride_id']
df_result['predicted_duration'] = y_pred

# Save as parquet
output_file = 'output.parquet'
df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)

# Check file size
file_size = os.path.getsize(output_file) / (1024 * 1024)  # Convert to MB
print(f"\nQ2 Answer - Output file size: {file_size:.0f}M")