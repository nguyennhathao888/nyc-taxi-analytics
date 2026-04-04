with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),
zones as (
    select * from {{ ref('stg_taxi_zones') }}
),
enriched as (
    select
        t.*,
        z_pickup.zone as pickup_zone,
        z_pickup.borough as pickup_borough,
        z_pickup.service_zone as pickup_service_zone,

        z_dropoff.zone as dropoff_zone,
        z_dropoff.borough as dropoff_borough,
        z_dropoff.service_zone as dropoff_service_zone
        
    from trips t
    left join zones z_pickup on t.pickup_location_id = z_pickup.location_id
    left join zones z_dropoff on t.dropoff_location_id = z_dropoff.location_id
)
select * from enriched
