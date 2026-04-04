with source as (
    {{ union_yellow_trips() }}
),

renamed as (
    select
        VendorID                as vendor_id,
        tpep_pickup_datetime    as pickup_datetime,
        tpep_dropoff_datetime   as dropoff_datetime,
        PULocationID            as pickup_location_id,
        DOLocationID            as dropoff_location_id,
        passenger_count,
        trip_distance,
        RatecodeID              as rate_code_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee             as airport_fee,
        cbd_congestion_fee,
        payment_type,
        store_and_fwd_flag      as store_and_fwd_flag,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
        date_trunc('month', tpep_pickup_datetime)                        as pickup_month

    from source
),

filtered as (
    select * from renamed
    where
        fare_amount          >= 0
        and trip_distance     > 0
        and passenger_count   > 0
        and pickup_datetime   < dropoff_datetime
        and trip_duration_minutes < 360
)

select * from filtered