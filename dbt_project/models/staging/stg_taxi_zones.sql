with source as (
    select * from {{ source('raw', 'taxi_zones') }}
),

renamed as (
    select
        LocationID as location_id,
        Borough as borough, 
        Zone as zone,
        service_zone as service_zone
    from source
)

select * from renamed