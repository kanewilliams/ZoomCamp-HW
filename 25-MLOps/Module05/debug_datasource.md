# PostgreSQL Datasource Debug

## Test Connection with Raw SQL

If the table picker doesn't work, use these raw SQL queries in Grafana:

### 1. Test Basic Connection:
```sql
SELECT NOW() as time, 1 as value;
```

### 2. Test Table Access:
```sql
SELECT COUNT(*) as count FROM dummy_metrics;
```

### 3. Main Dashboard Queries:

#### Fare Amount Quantile Panel:
```sql
SELECT
  timestamp AS "time",
  fare_amount_quantile_50 AS "Fare Amount Quantile 0.5"
FROM dummy_metrics
ORDER BY timestamp;
```

#### Prediction Drift Panel:
```sql
SELECT
  timestamp AS "time",
  prediction_drift AS "Prediction Drift"
FROM dummy_metrics
ORDER BY timestamp;
```

#### Number of Drifted Columns Panel:
```sql
SELECT
  timestamp AS "time",
  num_drifted_columns AS "Number of Drifted Columns"
FROM dummy_metrics
ORDER BY timestamp;
```

#### Share of Missing Values Panel:
```sql
SELECT
  timestamp AS "time",
  share_missing_values AS "Share of Missing Values"
FROM dummy_metrics
ORDER BY timestamp;
```

## Datasource Settings Checklist:
- ✅ Host: `db:5432` (not localhost)
- ✅ Database: `test`
- ✅ User: `postgres`
- ✅ Password: `example`
- ✅ SSL Mode: `disable`
- ✅ Test connection shows green checkmark