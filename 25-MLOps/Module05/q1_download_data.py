#!/usr/bin/env python3
"""
Q1: Download March 2024 Green Taxi data and determine shape
"""

import pandas as pd
import requests
from tqdm import tqdm
import os

def download_file(url, filename, data_dir):
    """Download file from URL with progress bar"""
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    file_path = os.path.join(data_dir, filename)
    
    if os.path.exists(file_path):
        print(f"{filename} already exists, skipping download.")
        return file_path
    
    print(f"Downloading {filename}...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    
    total_size = int(resp.headers.get('content-length', 0))
    
    with open(file_path, "wb") as handle:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
            for data in resp.iter_content(chunk_size=8192):
                handle.write(data)
                pbar.update(len(data))
    
    return file_path

def main():
    # Download March 2024 Green Taxi data
    filename = 'green_tripdata_2024-03.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    data_dir = '../data/green'  # Use the existing data directory
    
    file_path = download_file(url, filename, data_dir)
    
    # Load the data and check its shape
    print(f"\nLoading {filename}...")
    df = pd.read_parquet(file_path)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Number of columns: {df.shape[1]}")
    
    # Display first few rows and basic info
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nDataset info:")
    print(df.info())
    
    return df.shape[0]  # Return number of rows for answer

if __name__ == "__main__":
    num_rows = main()
    print(f"\nAnswer for Q1: {num_rows} rows")