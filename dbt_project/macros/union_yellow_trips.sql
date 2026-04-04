{% macro union_yellow_trips() %}

{% set year_months_old = [
    ('2024', '04'), ('2024', '05'), ('2024', '06'), ('2024', '07'),
    ('2024', '08'), ('2024', '09'), ('2024', '10'), ('2024', '11'),
    ('2024', '12')
] %}

{% set year_months_new = [
    ('2025', '01'), ('2025', '02'), ('2025', '03'), ('2025', '04'),
    ('2025', '05'), ('2025', '06'), ('2025', '07'), ('2025', '08'),
    ('2025', '09'), ('2025', '10'), ('2025', '11'), ('2025', '12'),
    ('2026', '01'), ('2026', '02')
] %}

{% for year, month in year_months_old %}
    select
        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
        PULocationID, DOLocationID, payment_type, fare_amount, extra,
        mta_tax, tip_amount, tolls_amount, improvement_surcharge,
        total_amount, congestion_surcharge, Airport_fee,
        0.0 as cbd_congestion_fee,
        'yellow_tripdata_{{ year }}_{{ month }}' as _source_table
    from main.yellow_tripdata_{{ year }}_{{ month }}
    union all
{% endfor %}

{% for year, month in year_months_new %}
    select
        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
        PULocationID, DOLocationID, payment_type, fare_amount, extra,
        mta_tax, tip_amount, tolls_amount, improvement_surcharge,
        total_amount, congestion_surcharge, Airport_fee,
        cbd_congestion_fee,
        'yellow_tripdata_{{ year }}_{{ month }}' as _source_table
    from main.yellow_tripdata_{{ year }}_{{ month }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}

{% endmacro %}