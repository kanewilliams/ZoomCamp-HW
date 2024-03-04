## Week 5 Homework ([Link]([https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/04-analytics-engineering/homework.md?plain=1](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/05-batch/homework.md)))
### Set-up
1. Ensure all appropriate files (hadoop/jdk/spark) are extracted to C:/tools as per the tutorial.

2. Run the following to set environmental variables:

```console
export JAVA_HOME="/c/tools/jdk-11.0.13"
export PATH="${JAVA_HOME}/bin:${PATH}"
export HADOOP_HOME="/c/tools/hadoop-3.2.0"
export PATH="${HADOOP_HOME}/bin:${PATH}"
export SPARK_HOME="/c/tools/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
);
```

3. Run the following to instantiated spark explicitly:

```console
cd /c/tools/spark-3.3.2-bin-hadoop3
./bin/spark-shell.cmd
```

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
```

### Question 2: 

**FHV October 2019**

<img align="right" width="600" height="600" src=https://github.com/kanewilliams/ZoomCamp2024-HW/assets/5062932/40e703ae-c892-4c8f-9c96-a9e8db8fb647)></img>

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- **6MB**
- 25MB
- 87MB

Around 6MB.

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- **62,610**

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhv_tripdata_2019-10.csv')

df.registerTempTable('df')
```

```SQL
spark.sql("""
SELECT
    count(1)
FROM
    df
WHERE
    DAY(pickup_datetime) = 15
""").show()
```


### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- **7.68 Hours**
- 3.32 Hours

```sql
spark.sql("""
SELECT
    (dropoff_datetime - pickup_datetime)/3600 as trip_length
FROM
    df
ORDER BY trip_length DESC
""").show()
```

returns:

```
+--------------------+
|         trip_length|
+--------------------+
|INTERVAL '7 07:19...|
|INTERVAL '7 07:19...|
|INTERVAL '1 00:21...|
|INTERVAL '0 19:28...|
...
```

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- **4040**
- 8080

### Question 6: 

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North

```SQL
spark.sql("""
SELECT
    PULocationID,
    COUNT(PULocationID)
FROM
    df
GROUP BY PULocationID
ORDER BY COUNT(PULocationID) ASC
""").show()
```
Didn't have time to do a proper join with the tables, but here's the shortcut ^^^ ;) (I'm overworked as is, with 5 Uni courses + this!!)

```
+------------+-------------------+
|PULocationID|count(PULocationID)|
+------------+-------------------+
|        null|                  0|
|           2|                  1|
|         105|                  2|
|         111|                  5|
```
Manually looking for what PULocationID number 2 is ... I get: **Jamaica Bay.**

![image](https://github.com/kanewilliams/ZoomCamp2024-HW/assets/5062932/c5d8629e-e003-4674-aaa1-5db1c62b77e9)




## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5
- Deadline: See the website
My guess is that Yello taxis will nevertheless have more trips.

## Submitting the solutions

* Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3](https://courses.datatalks.club/de-zoomcamp-2024/)https://courses.datatalks.club/de-zoomcamp-2024/
