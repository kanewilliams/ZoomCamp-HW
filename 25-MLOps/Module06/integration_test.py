import pandas as pd
from datetime import datetime
import os


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


# Create the same test dataframe from Q3
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

# Save this as January 2023 data
input_file = 's3://nyc-duration/in/2023-01.parquet'

# Configure S3 endpoint for Localstack
S3_ENDPOINT_URL = 'http://localhost:4566'
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

# Set AWS credentials for Localstack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Save the dataframe to S3
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

print(f"Test data saved to {input_file}")
print(f"Data shape: {df_input.shape}")
print("Data preview:")
print(df_input)

# Set environment variables for batch script
os.environ['INPUT_FILE_PATTERN'] = 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'
os.environ['OUTPUT_FILE_PATTERN'] = 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet' 
os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'

# Run the batch script for January 2023
print("\nRunning batch script...")
exit_code = os.system('python batch.py 2023 1')

if exit_code == 0:
    print("Batch script completed successfully")
    
    # Read the results
    output_file = 's3://nyc-duration/out/2023-01.parquet'
    
    df_result = pd.read_parquet(output_file, storage_options=options)
    print(f"\nResults shape: {df_result.shape}")
    print("Results preview:")
    print(df_result)
    
    # Calculate sum of predicted durations
    sum_predicted_durations = df_result['predicted_duration'].sum()
    print(f"\nSum of predicted durations: {sum_predicted_durations}")
    
else:
    print(f"Batch script failed with exit code: {exit_code}")