# Module 05: ML Monitoring with Evidently + Grafana + PostgreSQL

## 🎯 Learning Objectives
- Understand ML monitoring concepts (data drift, model performance)
- Use Evidently AI for metric calculation
- Set up PostgreSQL for metrics storage
- Create Grafana dashboards for visualization
- Implement production-ready monitoring pipeline

## 📁 Key Files
- `q1_download_data.py` - Downloads March 2024 Green Taxi data
- `monitoring_postgres.py` - **Main monitoring script** (run this!)
- `docker-compose.yml` - Infrastructure setup (PostgreSQL + Grafana)
- `config/` - Grafana configuration files
- `dashboards/monitoring_dashboard.json` - Pre-built dashboard
- `homework_answers.md` - Final answers and explanations

## 🚀 Interactive Learning Path

### Step 1: Explore the Data
```bash
# See what data we downloaded
uv run python q1_download_data.py
```
**Learn:** Understand the NYC taxi dataset structure and data quality

### Step 2: Run the Monitoring Pipeline
```bash
# Process all March 2024 data and store metrics in PostgreSQL
uv run python monitoring_postgres.py
```
**Learn:** How Evidently calculates metrics, PostgreSQL integration

### Step 3: Explore Grafana Dashboard
1. Open browser: http://localhost:3000
2. Login: admin/admin
3. Go to: Dashboards → Browse → "ML Monitoring Dashboard - Module 05"

**Learn:** How monitoring metrics look over time, dashboard design

### Step 4: Explore PostgreSQL Data
1. Open browser: http://localhost:8080 (Adminer)
2. Login: Server=db, Username=postgres, Password=example, Database=test
3. Browse table: `dummy_metrics`

**Learn:** Raw monitoring data structure, SQL queries

### Step 5: Experiment with Evidently
Open the notebook and run sections to see:
```bash
uv run jupyter notebook baseline_model_nyc_taxi_data.ipynb
```
**Learn:** Interactive Evidently reports, metric configuration

## 🔍 Key Concepts to Explore

### 1. ColumnQuantileMetric (Your Addition)
- **What:** Tracks the 50th percentile (median) of fare_amount over time
- **Why:** Detects pricing changes, data quality issues, economic shifts
- **In Grafana:** See the "Fare Amount Quantile 0.5" panel

### 2. Data Drift Detection
- **Column Drift:** Individual feature distribution changes
- **Dataset Drift:** Overall dataset distribution changes
- **In Grafana:** "Prediction Drift" and "Number of Drifted Columns" panels

### 3. Data Quality Monitoring
- **Missing Values:** Percentage of missing data points
- **In Grafana:** "Share of Missing Values" panel

## 🎛️ Hands-On Experiments

### Experiment 1: Modify the Dashboard
1. In Grafana, click "Edit" on any panel
2. Modify the SQL query to show different metrics
3. Change visualization types (line → bar → stat)
4. Save as new dashboard

### Experiment 2: Add New Metrics
Edit `monitoring_postgres.py`:
```python
# Add this to the Report metrics list:
ColumnQuantileMetric(column_name='trip_distance', quantile=0.95)
```
- Rerun the script
- Update dashboard to show the new metric

### Experiment 3: Time Range Analysis
In Grafana:
- Change time range to specific weeks
- Identify patterns (weekends vs weekdays)
- Look for anomalies in fare amounts

### Experiment 4: Database Queries
In Adminer (http://localhost:8080):
```sql
-- Find days with highest fare amounts
SELECT timestamp, fare_amount_quantile_50 
FROM dummy_metrics 
ORDER BY fare_amount_quantile_50 DESC;

-- Calculate weekly averages
SELECT 
    DATE_TRUNC('week', timestamp) as week,
    AVG(fare_amount_quantile_50) as avg_fare
FROM dummy_metrics 
GROUP BY week;
```

## 🤔 Discussion Questions
1. **Why did fare amounts vary between 12.8 and 14.2?** (weekdays vs weekends?)
2. **What would happen if we used quantile=0.95 instead of 0.5?** (detect outliers)
3. **How would you set up alerts for when drift exceeds thresholds?**
4. **What other metrics would be valuable for taxi prediction models?**

## 🛠️ Next Steps for Learning
1. **Read Evidently docs:** https://docs.evidentlyai.com/
2. **Explore Grafana features:** Alerts, annotations, template variables
3. **Try different ML models:** See how metrics change with RandomForest vs LinearRegression
4. **Production deployment:** How would you run this in AWS/GCP?

## 🐛 Troubleshooting
- **Grafana not loading?** Check `docker ps` - containers should be running
- **No data in dashboard?** Ensure `monitoring_postgres.py` completed successfully
- **Connection errors?** Restart containers: `docker compose restart`

## 🎪 Fun Challenges
1. Create a "Model Health Score" combining all metrics
2. Set up email alerts when fare amounts exceed $20
3. Compare March 2024 vs other months for seasonality
4. Build a real-time monitoring system with streaming data

Remember: **The best learning happens by breaking things and fixing them!** 🚀