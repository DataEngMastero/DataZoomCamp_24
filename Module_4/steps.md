CREATE EXTERNAL TABLE taxi_dbt.green_tripdata
OPTIONS (
  format = 'CSV',
  uris = ['gs://taxi_dbt_zc/green_tripdata/green_tripdata_*.csv']
);

SELECT count(*) FROM taxi_dbt.green_tripdata; 
SELECT * FROM taxi_dbt.green_tripdata LIMIT 10;

CREATE TABLE taxi_dbt.green_tripdata_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
AS 
SELECT * FROM taxi_dbt.green_tripdata; 


CREATE EXTERNAL TABLE taxi_dbt.yellow_tripdata
OPTIONS (
  format = 'CSV',
  uris = ['gs://taxi_dbt_zc/yellow_tripdata/yellow_tripdata*.csv']
);

SELECT count(*) FROM taxi_dbt.yellow_tripdata; 
SELECT * FROM taxi_dbt.yellow_tripdata LIMIT 10;

CREATE TABLE taxi_dbt.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_pickup_datetime)
AS 
SELECT * FROM taxi_dbt.yellow_tripdata; 


// For Empty Columns > Tried adding column and data types.
// Using allow_jagged_rows Options worked
CREATE EXTERNAL TABLE taxi_dbt.fhv_tripdata
OPTIONS (
  format = 'CSV',
  allow_jagged_rows=True,
  uris = ['gs://taxi_dbt_zc/fhv_tripdata/fhv_tripdata_*.csv']
);

SELECT count(*) FROM taxi_dbt.fhv_tripdata LIMIT 10;

SELECT * FROM taxi_dbt.fhv_tripdata LIMIT 10;


CREATE TABLE taxi_dbt.fhv_tripdata_partitioned
PARTITION BY DATE(pickup_datetime)
AS 
SELECT * FROM taxi_dbt.fhv_tripdata;
