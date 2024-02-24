## Week 4 Homework ([Link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/04-analytics-engineering/homework.md?plain=1))
### Set-up
1. Use a modified version of **web_to_gcs.py** to upload fhv .csv data into a GCS Bucket.

2. Ingest the GCS Bucket data into a BigQuery table using the following SQL:

```SQL
LOAD DATA OVERWRITE trips_data_all.fhv_tripdata
( index_ INT64,
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,
  PUlocationID STRING,
  DOlocationID STRING,
  SR_Flag STRING,
  Affiliated_base_number STRING )
FROM FILES (
  format = 'CSV',
  uris = (['gs://mage_zoomcamp_dezc2/fhv/fhv_tripdata_2019-*.csv']),
  skip_leading_rows = 1
);
```
Now we are ready to rock and roll!

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 
- It's the same as running *dbt build*
- It applies a _limit 100_ to all of our models
- **It applies a _limit 100_ only to our staging models**
- Nothing

**Explanation**: Because only the staging models have the line:
```SQL
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

### Question 2: 

**What is the code that our CI job will run? Where is this code coming from?**  

- The code that has been merged into the main branch
- The code that is behind the creation object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- **The code from the development branch we are requesting to merge to main**

This is primarily a guess... It is either (1) or (4). Production branch only worked for me after merging, so practically should be (1). However, (4) makes more sense theoretically.


### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

- 12998722
- 22998722
- 32998722
- 42998722

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- Yellow
- FHV and Green
- 
## Submitting the solutions

* Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3](https://courses.datatalks.club/de-zoomcamp-2024/)https://courses.datatalks.club/de-zoomcamp-2024/
