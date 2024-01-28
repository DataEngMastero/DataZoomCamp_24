docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /Users/poojasingh/Documents/Git_Reposit/DataZoomCamp/Module_1/ny_taxi_dataset:/app/postgres/data \
    -p 5432:5432 \
    postgres:13