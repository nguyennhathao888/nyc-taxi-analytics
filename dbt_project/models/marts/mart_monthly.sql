with monthly_trips as (
    select * from {{ ref('fct_trips') }}
),
aggregated as (
    select
        pickup_month,
        count(*) as total_trips,
        sum(fare_amount) as total_fare_amount,
        sum(tip_amount) as total_tip_amount,
        avg(trip_distance) as avg_trip_distance,
        avg(trip_duration_minutes) as avg_trip_duration_minutes
    from monthly_trips
    group by pickup_month
)
select * from aggregated