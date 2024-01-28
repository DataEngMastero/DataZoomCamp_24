docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /Users/poojasingh/Documents/Git_Reposit/DataZoomCamp/Module_1/ny_taxi_dataset:/app/postgres/data \
    -p 5432:5432 \
    # --network=pg-network
    # --name pg-database
    postgres:13

docker rename 619dd4a819b3 pg-database

docker network connect pg-network pg-database

pgcli -h localhost -p 5432 -u root -d ny_taxi