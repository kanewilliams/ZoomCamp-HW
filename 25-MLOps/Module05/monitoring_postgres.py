#!/usr/bin/env python3
"""
Monitoring script with PostgreSQL integration (without Prefect)
"""

import datetime
import pandas as pd
import joblib
import psycopg
from evidently.report import Report
from evidently import ColumnMapping
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric, DatasetMissingValuesMetric, ColumnQuantileMetric

print("=== Setting up PostgreSQL Database ===")

# Database setup
create_table_statement = """
DROP TABLE IF EXISTS dummy_metrics;
CREATE TABLE dummy_metrics(
    timestamp timestamp,
    prediction_drift float,
    num_drifted_columns integer,
    share_missing_values float,
    fare_amount_quantile_50 float
);
"""

# Initialize database
print("Connecting to PostgreSQL...")
try:
    with psycopg.connect("host=localhost port=5432 user=postgres password=example", autocommit=True) as conn:
        # Create database if not exists
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
        if len(res.fetchall()) == 0:
            conn.execute("CREATE DATABASE test;")
            print("Created database 'test'")
        
        # Create table
        with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example") as test_conn:
            test_conn.execute(create_table_statement)
            test_conn.commit()
            print("Created metrics table")

except Exception as e:
    print(f"Database setup error: {e}")
    print("Make sure PostgreSQL container is running!")
    exit(1)

# Load data and model
print("Loading reference data and model...")
reference_data = pd.read_parquet('../data/reference.parquet')
with open('../models/lin_reg.bin', 'rb') as f_in:
    model = joblib.load(f_in)

raw_data = pd.read_parquet('../data/green/green_tripdata_2024-03.parquet')

# Setup monitoring
num_features = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount']
cat_features = ['PULocationID', 'DOLocationID']
column_mapping = ColumnMapping(
    prediction='prediction',
    numerical_features=num_features,
    categorical_features=cat_features,
    target=None
)

report = Report(metrics=[
    ColumnDriftMetric(column_name='prediction'),
    DatasetDriftMetric(),
    DatasetMissingValuesMetric(),
    ColumnQuantileMetric(column_name='fare_amount', quantile=0.5)
])

# Process daily batches
print("Processing daily batches and storing in PostgreSQL...")
begin = datetime.datetime(2024, 3, 1, 0, 0)
quantile_values = []

with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
    for i in range(31):  # March has 31 days
        # Get data for day i
        current_data = raw_data[
            (raw_data.lpep_pickup_datetime >= (begin + datetime.timedelta(i))) &
            (raw_data.lpep_pickup_datetime < (begin + datetime.timedelta(i + 1)))
        ].copy()
        
        if len(current_data) == 0:
            print(f"Day {i+1}: No data")
            continue
        
        # Add predictions
        current_data['prediction'] = model.predict(current_data[num_features + cat_features].fillna(0))
        
        # Run evidently report
        report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)
        result = report.as_dict()
        
        # Extract metrics
        prediction_drift = result['metrics'][0]['result']['drift_score']
        num_drifted_columns = result['metrics'][1]['result']['number_of_drifted_columns']
        share_missing_values = result['metrics'][2]['result']['current']['share_of_missing_values']  
        fare_quantile_50 = result['metrics'][3]['result']['current']['value']
        
        # Insert into database
        with conn.cursor() as curr:
            curr.execute(
                "INSERT INTO dummy_metrics(timestamp, prediction_drift, num_drifted_columns, share_missing_values, fare_amount_quantile_50) VALUES (%s, %s, %s, %s, %s)",
                (begin + datetime.timedelta(i), prediction_drift, num_drifted_columns, share_missing_values, fare_quantile_50)
            )
        
        quantile_values.append(fare_quantile_50)
        print(f"Day {i+1} ({(begin + datetime.timedelta(i)).strftime('%Y-%m-%d')}): quantile 0.5 = {fare_quantile_50:.2f}")

# Verify data in database
print("\n=== Database Verification ===")
with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example") as conn:
    with conn.cursor() as curr:
        curr.execute("SELECT COUNT(*) FROM dummy_metrics")
        count = curr.fetchone()[0]
        print(f"Total records in database: {count}")
        
        curr.execute("SELECT MAX(fare_amount_quantile_50) FROM dummy_metrics")
        max_quantile = curr.fetchone()[0]
        print(f"Maximum fare_amount quantile 0.5: {max_quantile}")

print("\n=== Setup Complete ===")
print("Database is ready for Grafana visualization!")
print("Access Grafana at: http://localhost:3000 (admin/admin)")
print("PostgreSQL is available at: localhost:5432 (postgres/example)")