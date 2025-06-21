# Module 05 Homework - ML Monitoring

## Overview
This homework focuses on monitoring ML batch services using PostgreSQL, Grafana, and Evidently AI. We implemented a complete monitoring solution for a taxi trip duration prediction service.

## Answers

### Q1. Prepare the dataset
**Question:** Download March 2024 Green Taxi data and determine the shape.

**Answer:** **57457**

The downloaded March 2024 Green Taxi data has 57,457 rows and 20 columns.

### Q2. Metric
**Question:** Add one metric of your choice and a quantile value for the "fare_amount" column (quantile=0.5).

**Answer:** **ColumnQuantileMetric**

I added the `ColumnQuantileMetric` from `evidently.metrics` to monitor the 0.5 quantile (median) of the fare_amount column. This metric helps detect changes in the central tendency of fare amounts over time.

**Why ColumnQuantileMetric is Important:**
- Detects shifts in fare amount distribution
- Monitors pricing changes or data quality issues
- Provides early warning for model input drift
- Helps maintain model performance reliability

### Q3. Monitoring
**Question:** What is the maximum value of metric quantile = 0.5 on the "fare_amount" column during March 2024?

**Answer:** **14.2**

After running batch monitoring for all 31 days of March 2024, the maximum value of the fare_amount quantile 0.5 metric was 14.2, which occurred on multiple days (March 3, 10, 14, 16, 24, and 30).

### Q4. Dashboard
**Question:** Where to place a dashboard config file?

**Answer:** **project_folder/dashboards (05-monitoring/dashboards)**

According to the MLOps Zoomcamp structure and Grafana best practices, dashboard configuration files should be stored in the `dashboards` directory within the project folder. This allows for:
- Version control of dashboard configurations
- Easy sharing and deployment
- Consistent dashboard provisioning across environments

## Implementation Summary

### Files Created:
- `q1_download_data.py` - Downloads and analyzes March 2024 data
- `simple_monitoring.py` - Runs monitoring without PostgreSQL dependencies  
- `monitoring_postgres.py` - Full monitoring with PostgreSQL integration
- `dashboards/monitoring_dashboard.json` - Grafana dashboard configuration
- `config/grafana_datasources.yaml` - PostgreSQL datasource configuration
- `config/grafana_dashboards.yaml` - Dashboard provisioning configuration

### Infrastructure:
- PostgreSQL database for storing metrics
- Grafana for visualization dashboards
- Evidently AI for metric calculation
- Docker Compose for service orchestration

### Key Metrics Monitored:
1. **Prediction Drift** - Model output distribution changes
2. **Dataset Drift** - Number of drifted feature columns  
3. **Missing Values** - Data quality metric
4. **Fare Amount Quantile 0.5** - Median fare amount tracking (new metric)

## Database Schema:
```sql
CREATE TABLE dummy_metrics(
    timestamp timestamp,
    prediction_drift float,
    num_drifted_columns integer,
    share_missing_values float,
    fare_amount_quantile_50 float
);
```

## Access Information:
- **Grafana Dashboard:** http://localhost:3000 (admin/admin)
- **PostgreSQL:** localhost:5432 (postgres/example)
- **Adminer:** http://localhost:8080

## Submission Link:
https://courses.datatalks.club/mlops-zoomcamp-2025/homework/hw5