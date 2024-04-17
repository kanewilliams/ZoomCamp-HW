## Week 3 Homework
### Set-up
(Following these Instructions: https://drive.google.com/file/d/1GIi6xnS4070a8MUlIg-ozITt485_-ePB/view)
<img align="right" width="200" height="200" src="https://github.com/kanewilliams/ZoomCamp2024-HW/assets/5062932/b895c510-5cb8-442f-8b42-c75351f4581d"></img>

1. Use Mage to upload 2022 Green Taxi parquet files to my GCS Bucket.

2. Create an **External table** using "Add" on explorer on left hand side.

3. Create the **Materialized Table** using:

```SQL
# Creating "Materialized Table"
CREATE OR REPLACE TABLE proven-mercury-412123.ny_taxi.g_taxi_2022_m AS
SELECT * FROM proven-mercury-412123.ny_taxi.g_taxi_2022;
```

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- **840,402**
- 1,936,423
- 253,647

```SQL
SELECT COUNT(1) FROM ny_taxi.g_taxi_2022;
```

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- **0 MB for the External Table and 6.41MB for the Materialized Table**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

```SQL
#- External (0B)
SELECT COUNT(DISTINCT(PULocationID)) FROM ny_taxi.g_taxi_2022;
#- Material (6.41MB)
SELECT COUNT(DISTINCT(PULocationID)) FROM ny_taxi.g_taxi_2022_m;
```


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- **1,622**

```SQL
SELECT COUNT(1) FROM ny_taxi.g_taxi_2022 WHERE fare_amount = 0.0;
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- **Partition by lpep_pickup_datetime  Cluster on PUlocationID**
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

**Explanation**: Clustering is basically sorting a list. If you want to return an ordered list, as we want to do for PULocationID, then PULocationID should be sorted.
Furthermore, partitioning benefits filtering and aggregating functions, so we should partition lpep_pickup_datetime.

```SQL
# (Creating Partitioned Table - note lpep_pickup_datetime is in INT64 UNIX Epoch format. It must be converted.)
CREATE OR REPLACE TABLE proven-mercury-412123.ny_taxi.g_taxi_2022_m_partitioned
PARTITION BY DATE(lpep_date)
CLUSTER BY PULocationID AS
SELECT *, TIMESTAMP_MICROS(DIV(lpep_pickup_datetime, 1000)) AS lpep_date FROM proven-mercury-412123.ny_taxi.g_taxi_2022_m;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- **12.82 MB for non-partitioned table and 1.12 MB for the partitioned table**
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

```SQL
- Non-partitioned - 12.82MB
SELECT DISTINCT(PULocationID)
FROM proven-mercury-412123.ny_taxi.g_taxi_2022_m
WHERE DATE(TIMESTAMP_MICROS(
    DIV(lpep_pickup_datetime, 1000)
)) BETWEEN '2022-06-01' AND '2022-06-30';

- Partitioned - 1.12MB
SELECT DISTINCT(PULocationID)
FROM proven-mercury-412123.ny_taxi.g_taxi_2022_m_partitioned
WHERE lpep_date BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- **Big Table**
- Container Registry

**Source**: https://cloud.google.com/bigquery/docs/external-tables#bigtable-location-considerations


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- **False**

**Explanation**: If you're not going to do much to make use of the clustered data, then it could be a waste of time. Clustering a large amount of data will take time.


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer**: 0B are estimated. I am not sure why, but I can make a guess... _Perhaps it is because the number of rows is stored as MetaData_? So it only needs to retrieve that one piece of information, instead of counting the number of entries in a column.

```SQL
SELECT count(*) FROM ny_taxi.g_taxi_2022_m;
```
 
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3

