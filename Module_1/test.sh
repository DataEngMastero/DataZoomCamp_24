URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
python3 ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --database=ny_taxi \
    --table=yellow_taxi_trips\
    --file_url=${URL}


ASSIGN_URL='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet'
python3 ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --database=ny_taxi \
    --table=green_taxi_trips\
    --file_url=${ASSIGN_URL}




docker build -t taxi_ingest:v001 .

docker run -it  \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --database=ny_taxi \
    --table=yellow_taxi_trips \
    --file_url=${URL}


python3 zone_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --database=ny_taxi \
    --table=zones