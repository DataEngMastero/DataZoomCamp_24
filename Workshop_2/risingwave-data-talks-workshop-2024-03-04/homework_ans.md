Columns:
vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | 
ratecodeid | store_and_fwd_flag | pulocationid | dolocationid | payment_type | 
fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | 
total_amount | congestion_surcharge | airport_fee 

Question 0 : What are the dropoff taxi zones at the latest dropoff times?
SELECT MAX(tpep_dropoff_datetime) FROM trip_data;   // Latest Dropoff time :  2022-01-03 17:24:54
SELECT * FROM trip_data WHERE tpep_dropoff_datetime = '2022-01-03 17:24:54';

CREATE MATERIALIZED VIEW latest_dropoff_time AS
SELECT dolocationid, zone, tpep_dropoff_datetime
FROM trip_data td 
JOIN taxi_zone tz ON td.dolocationid = location_id
WHERE tpep_dropoff_datetime = (
    SELECT MAX(tpep_dropoff_datetime) FROM trip_data
);


Question 1: Create a materialized view to compute the average, min and max trip time between each taxi zone.

CREATE MATERIALIZED VIEW trip_distance_time AS
SELECT 
    tz2.zone pu_zone, tz1.zone do_zone, 
    avg(tpep_dropoff_datetime - tpep_pickup_datetime) avg_trip_time,
    min(tpep_dropoff_datetime - tpep_pickup_datetime) min_trip_time,
    max(tpep_dropoff_datetime - tpep_pickup_datetime) max_trip_time
FROM trip_data td 
JOIN taxi_zone tz1 ON td.dolocationid = tz1.location_id
JOIN taxi_zone tz2 ON td.pulocationid = tz2.location_id
GROUP BY 1,2
ORDER BY 3 DESC;

SELECT * FROM trip_distance_time ORDER BY avg_trip_time DESC LIMIT 5;


Question 2: Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

CREATE MATERIALIZED VIEW zone_trip_details AS
SELECT 
    tz2.zone pu_zone, tz1.zone do_zone, 
    avg(tpep_dropoff_datetime - tpep_pickup_datetime) avg_trip_time,
    min(tpep_dropoff_datetime - tpep_pickup_datetime) min_trip_time,
    max(tpep_dropoff_datetime - tpep_pickup_datetime) max_trip_time,
    count(*) num_of_trips
FROM trip_data td 
JOIN taxi_zone tz1 ON td.dolocationid = tz1.location_id
JOIN taxi_zone tz2 ON td.pulocationid = tz2.location_id
GROUP BY 1,2
ORDER BY 3 DESC;

SELECT * FROM zone_trip_details ORDER BY avg_trip_time DESC LIMIT 5;

Question 3: From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

SELECT MAX(tpep_pickup_datetime) FROM trip_data; // 2022-01-03 10:53:33

17 hours before :  (NOW() - INTERVAL '1' MINUTE)


CREATE MATERIALIZED VIEW busiest_zones AS 
WITH max_putime AS (SELECT MAX(tpep_pickup_datetime) max_pu FROM trip_data) 
SELECT
    tz.zone AS pu_zone,
    count(*) AS num_of_pickups
FROM trip_data td
JOIN taxi_zone tz ON td.pulocationid = tz.location_id
, max_putime
WHERE td.tpep_pickup_datetime > (max_pu - INTERVAL '17' HOUR)
GROUP BY tz.Zone 
ORDER BY num_of_pickups DESC;

