with enriched_trips as (
    select * from {{ ref('int_trips_enriched') }}
),

final as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        rate_code_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        payment_type,
        store_and_fwd_flag,
        trip_duration_minutes,
        pickup_month,
        pickup_zone,
        pickup_borough,
        pickup_service_zone,
        dropoff_zone,
        dropoff_borough,
        dropoff_service_zone,
        case
            when pickup_zone ilike '%airport%'
              or dropoff_zone ilike '%airport%'
            then true else false
        end as is_airport_trip,

        case
            when fare_amount > 0
            then round(tip_amount / fare_amount * 100, 2)
            else 0
        end as tip_percentage,

        case
            when pickup_service_zone = 'Yellow Zone'
            then true else false
        end as is_congestion_zone

    from enriched_trips
)

select * from final