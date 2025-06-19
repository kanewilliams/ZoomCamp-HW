#!/usr/bin/env python
# coding: utf-8

import pickle
import pandas as pd
import numpy as np
import sys
import os

def read_data(filename):
    categorical = ['PULocationID', 'DOLocationID']
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def main(year, month):
    # Load the model
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)
    
    # Read data for specified year and month
    input_file = f'/home/kanewsl/repos/ZoomCamp-HW/25-MLOps/data/yellow/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    df = read_data(input_file)
    
    # Prepare features
    categorical = ['PULocationID', 'DOLocationID']
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    
    # Make predictions
    y_pred = model.predict(X_val)
    
    # Calculate mean predicted duration
    mean_duration = np.mean(y_pred)
    print(f"Mean predicted duration: {mean_duration:.2f}")
    
    # Prepare output with ride_id
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    # Create dataframe with results
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    
    # Save as parquet
    output_file = f'output_{year:04d}-{month:02d}.parquet'
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )
    
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python starter.py <year> <month>")
        sys.exit(1)
    
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    
    main(year, month)