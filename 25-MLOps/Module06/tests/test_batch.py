import pandas as pd
from datetime import datetime
import sys
import os

# Add parent directory to path to import batch
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import batch


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)
    
    categorical = ['PULocationID', 'DOLocationID']
    
    actual_result = batch.prepare_data(df, categorical)
    
    # Expected result should have 2 rows:
    # Row 0: (None, None, dt(1, 1), dt(1, 10)) -> duration = 9 minutes (valid: >= 1 and <= 60)
    # Row 1: (1, 1, dt(1, 2), dt(1, 10)) -> duration = 8 minutes (valid: >= 1 and <= 60)
    # Row 2: (1, None, dt(1, 2, 0), dt(1, 2, 59)) -> duration = 59/60 ≈ 0.98 minutes (invalid: < 1)
    # Row 3: (3, 4, dt(1, 2, 0), dt(2, 2, 1)) -> duration = 1441/60 ≈ 24 minutes (valid but wait, that's wrong)
    
    # Let me recalculate row 3: dt(1, 2, 0) to dt(2, 2, 1) = 1 hour + 1 second = 3661 seconds = 61+ minutes (invalid: > 60)
    
    expected_data = [
        ('-1', '-1', dt(1, 1), dt(1, 10), 9.0),
        ('1', '1', dt(1, 2), dt(1, 10), 8.0),
    ]
    
    expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    expected_result = pd.DataFrame(expected_data, columns=expected_columns)
    
    # Compare the number of rows
    assert len(actual_result) == 2
    
    # Check that duration column is calculated correctly
    assert 'duration' in actual_result.columns
    
    # Check that categorical columns are strings and handle NaN properly
    assert actual_result['PULocationID'].dtype == 'object'
    assert actual_result['DOLocationID'].dtype == 'object'
    
    # Convert to dictionaries for easier comparison
    actual_dict = actual_result.to_dict('records')
    expected_dict = expected_result.to_dict('records')
    
    # Compare values (allowing for small floating point differences)
    for i in range(len(expected_dict)):
        assert actual_dict[i]['PULocationID'] == expected_dict[i]['PULocationID']
        assert actual_dict[i]['DOLocationID'] == expected_dict[i]['DOLocationID']
        assert abs(actual_dict[i]['duration'] - expected_dict[i]['duration']) < 0.01