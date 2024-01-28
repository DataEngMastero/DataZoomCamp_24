SELECT * FROM green_taxi_trips LIMIT 10


SELECT COUNT(*) FROM green_taxi_trips 
WHERE DATE(lpep_pickup_datetime) = DATE('2019-09-18')
    AND DATE(lpep_dropoff_datetime) = DATE('2019-09-18')


SELECT DATE(lpep_pickup_datetime), MAX(trip_distance)
FROM green_taxi_trips
GROUP BY 1 
LIMIT 5

SELECT 
    Borough, 
    SUM(total_amount)
FROM green_taxi_trips gt 
JOIN zones z ON gt.PULocationID = z.LocationID
WHERE DATE(lpep_pickup_datetime) = DATE('2019-09-18')
GROUP BY 1 
HAVING SUM(total_amount) > 50000
ORDER BY 2 DESC 


SELECT 
    zd."zone", MAX(tip_amount)
FROM green_taxi_trips 
JOIN zones zp ON gt."PULocationID" = zp."LocationID"
JOIN zones zd ON gt."DOLocationID" = zd."LocationID"
WHERE zp.zone = "Astoria"
GROUP BY 1 
ORDER BY 2 DESC 