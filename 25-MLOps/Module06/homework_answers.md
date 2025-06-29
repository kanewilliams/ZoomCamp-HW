# Module 06 Homework Answers - Best Practices

## Overview
This homework focused on improving code reliability through unit and integration testing for a ride duration prediction model. We refactored existing batch inference code, added comprehensive testing, and set up local S3 simulation using Localstack.

## Questions and Answers

### Q1. Refactoring
**Question**: How does the `if` statement that we use for the main block look like?

**Answer**: `if __name__ == '__main__':`

**Implementation**:
- Created a `main(year, month)` function with parameters
- Moved all code (except `read_data`) inside `main`
- Made `categorical` a parameter for `read_data`
- Added proper main block execution

### Q2. Installing pytest
**Question**: What should be the other file in the tests folder (besides `test_batch.py`)?

**Answer**: `__init__.py`

**Implementation**:
- Installed pytest using `uv add --dev pytest`
- Created `tests/` folder with `test_batch.py` and `__init__.py`
- The `__init__.py` file makes the tests directory a Python package, enabling imports

### Q3. Writing first unit test
**Question**: How many rows should be there in the expected dataframe?

**Answer**: **2**

**Implementation**:
- Split `read_data()` into separate reading (I/O) and transformation functions
- Created `prepare_data()` function for data transformations
- Wrote unit test with sample data:
  - Row 0: 9 minutes duration (valid: >= 1 and <= 60)
  - Row 1: 8 minutes duration (valid: >= 1 and <= 60)
  - Row 2: ~0.98 minutes duration (invalid: < 1) - filtered out
  - Row 3: ~61 minutes duration (invalid: > 60) - filtered out

### Q4. Mocking S3 with Localstack
**Question**: What option do we need to use with AWS CLI for Localstack?

**Answer**: `--endpoint-url`

**Implementation**:
- Created `docker-compose.yaml` with Localstack service
- Started Localstack with S3 service enabled
- Created S3 bucket: `aws s3 mb s3://nyc-duration --endpoint-url=http://localhost:4566`
- Verified bucket creation: `aws s3 ls --endpoint-url=http://localhost:4566`

### Q5. Creating test data
**Question**: What's the size of the test file?

**Answer**: **3620** (closest to actual 3185 bytes)

**Implementation**:
- Created `integration_test.py` with test dataframe from Q3
- Saved test data to S3 using exact snippet with specific parameters
- Used environment variables for configurable paths
- File saved to `s3://nyc-duration/in/2023-01.parquet`

### Q6. Finish the integration test
**Question**: What's the sum of predicted durations for the test dataframe?

**Answer**: **36.28**

**Implementation**:
- Created `save_data()` function for S3-compatible output
- Added environment variable support for S3 endpoints
- Ran batch script with test data: `python batch.py 2023 1`
- Read results and calculated sum: 23.197149 + 13.080101 = 36.277250

## Technical Implementation Details

### Code Refactoring
- **Modular Functions**: Split monolithic code into testable functions
- **Environment Configuration**: Added support for `INPUT_FILE_PATTERN`, `OUTPUT_FILE_PATTERN`, and `S3_ENDPOINT_URL`
- **S3 Compatibility**: Implemented conditional S3 endpoint handling for both reading and writing

### Testing Architecture
- **Unit Tests**: Focused on individual function behavior (data transformations)
- **Integration Tests**: End-to-end pipeline testing with simulated cloud services
- **Mocking Strategy**: Used Localstack for realistic S3 simulation without external dependencies

### Key Files Created
- `batch.py` - Refactored with modular functions and S3 support
- `tests/test_batch.py` - Unit tests for data preparation logic
- `integration_test.py` - End-to-end integration testing
- `docker-compose.yaml` - Localstack S3 simulation setup

### Dependencies Added
- `pytest` - Testing framework
- `s3fs` - S3 filesystem interface for pandas

## Summary
Successfully implemented comprehensive testing strategy that improves code reliability while maintaining production compatibility. The combination of unit tests for business logic and integration tests with cloud service simulation provides confidence in code quality and deployment readiness.