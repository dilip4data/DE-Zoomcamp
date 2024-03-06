{{
    config(
        materialized='view'
    )
}}

    select
        dispatching_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        cast(pulocationid as integer) as pickup_locationid,
        cast(dolocationid as integer) as dropoff_locationid,

        -- trip info
        sr_flag,
        affiliated_base_number

    from {{ source('staging', 'fhv_tripdata_2019') }}
    where extract(year from pickup_datetime) = 2019


