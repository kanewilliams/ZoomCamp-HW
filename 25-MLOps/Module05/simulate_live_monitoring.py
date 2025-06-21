#!/usr/bin/env python3
"""
Simulate continuous monitoring by adding new data points every 30 seconds
"""

import datetime
import time
import pandas as pd
import joblib
import psycopg
import random
from evidently.report import Report
from evidently import ColumnMapping
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric, DatasetMissingValuesMetric, ColumnQuantileMetric

print("🔄 Starting Live Monitoring Simulation...")
print("This will add new data points every 30 seconds")
print("Watch your Grafana dashboard update in real-time!")
print("Press Ctrl+C to stop")

# Load existing data and model
reference_data = pd.read_parquet('../data/reference.parquet')
with open('../models/lin_reg.bin', 'rb') as f_in:
    model = joblib.load(f_in)

raw_data = pd.read_parquet('../data/green/green_tripdata_2024-03.parquet')

# Setup
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

def simulate_batch():
    """Simulate a new batch of data by sampling and slightly modifying existing data"""
    # Take a random sample of 1000 rides
    sample_data = raw_data.sample(n=1000, random_state=random.randint(1, 10000)).copy()
    
    # Simulate realistic variations
    # Weekend vs weekday pricing
    fare_multiplier = random.uniform(0.9, 1.1)  # ±10% variation
    sample_data['fare_amount'] *= fare_multiplier
    sample_data['total_amount'] *= fare_multiplier
    
    # Add some noise to trip distance (traffic, routes)
    sample_data['trip_distance'] *= random.uniform(0.95, 1.05)
    
    return sample_data

# Continuous monitoring loop
try:
    with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
        simulation_start = datetime.datetime.now()
        
        for hour in range(24 * 7):  # Simulate a week of hourly data
            print(f"\n⏰ Hour {hour + 1}: {datetime.datetime.now().strftime('%H:%M:%S')}")
            
            # Simulate new batch
            current_data = simulate_batch()
            current_data['prediction'] = model.predict(current_data[num_features + cat_features].fillna(0))
            
            # Calculate metrics
            report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)
            result = report.as_dict()
            
            # Extract metrics
            prediction_drift = result['metrics'][0]['result']['drift_score']
            num_drifted_columns = result['metrics'][1]['result']['number_of_drifted_columns']
            share_missing_values = result['metrics'][2]['result']['current']['share_of_missing_values']
            fare_quantile_50 = result['metrics'][3]['result']['current']['value']
            
            # Insert with current timestamp
            current_time = simulation_start + datetime.timedelta(hours=hour)
            
            with conn.cursor() as curr:
                curr.execute(
                    "INSERT INTO dummy_metrics(timestamp, prediction_drift, num_drifted_columns, share_missing_values, fare_amount_quantile_50) VALUES (%s, %s, %s, %s, %s)",
                    (current_time, prediction_drift, num_drifted_columns, share_missing_values, fare_quantile_50)
                )
            
            print(f"📊 Fare quantile 0.5: {fare_quantile_50:.2f}")
            print(f"🎯 Prediction drift: {prediction_drift:.4f}")
            print("✅ Data point added - check Grafana!")
            
            # Wait 30 seconds (or adjust for faster/slower updates)
            time.sleep(30)
            
except KeyboardInterrupt:
    print("\n🛑 Monitoring stopped by user")
except Exception as e:
    print(f"❌ Error: {e}")

print("📈 Live monitoring simulation complete!")
print("Your Grafana dashboard now shows the new data points")