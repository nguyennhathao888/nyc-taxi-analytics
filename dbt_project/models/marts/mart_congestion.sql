with congestion_trips as (
    select * from {{ ref('fct_trips') }}
),
congestion as (
    select
        case 
            when year(pickup_month)<2025 then 'before_2025'
            else 'after_2025'
        end as time_period,       
        count(*) as congestion_trips,
        sum(fare_amount) as congestion_fare_amount,
        sum(tip_amount) as congestion_tip_amount,
        avg(trip_distance) as congestion_avg_trip_distance,
        avg(trip_duration_minutes) as congestion_avg_trip_duration_minutes,
        avg(cbd_congestion_fee) as avg_congestion_fee
    from congestion_trips
    group by time_period
)
select * from congestion