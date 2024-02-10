
## Setup
1. Download Green Taxi 2022 Parquet files from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Upload files in GCS 
3. Create an external table using the Green Taxi Trip Records Data for 2022.

CREATE EXTERNAL TABLE ny_taxi.green_taxi_data_2022
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_green_taxi/green_tripdata_2022-*.parquet']
)

4. Create a table in BQ using the Green Taxi Trip Records for 2022

CREATE TABLE ny_taxi.green_taxi_data_2022_not_partitioned
AS SELECT *
FROM ny_taxi.green_taxi_data_2022;


## Assignment Questions
1. Total records in table - 
SELECT count(*) FROM ny_taxi.green_taxi_data_2022; 


2. Bytes processed for external and BQ tables - 
SELECT COUNT(distinct PULocationID) FROM ny_taxi.green_taxi_data_2022_not_partitioned; 
SELECT COUNT(distinct PULocationID) FROM ny_taxi.green_taxi_data_2022; 


3. Records with fare amount = 0 -
SELECT count(*) FROM ny_taxi.green_taxi_data_2022 WHERE fare_amount = 0;


4. Best Startegy to make table -
CREATE TABLE ny_taxi.green_taxi_data_2022_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS 
SELECT * FROM ny_taxi.green_taxi_data_2022; 

5. Difference in Non - Partitioned and Partitioned tables

CREATE TABLE ny_taxi.green_taxi_data_2022_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
AS 
SELECT * FROM ny_taxi.green_taxi_data_2022; 


