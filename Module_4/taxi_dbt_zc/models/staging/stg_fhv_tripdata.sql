{{
    config(
        materialized='view'
    )
}}


select 
    -- identifiers
    dispatching_base_num,
    affiliated_base_number,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
  
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime
   
from {{ source('staging','fhv_tripdata') }}
where FORMAT_TIMESTAMP('%G', pickup_datetime) = '2019'