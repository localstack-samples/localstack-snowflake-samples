-- Revenue per pickup zone per day. Revenue is the passenger-paid total
-- (fare, surcharges, tolls and tips) of billable trips.
select
    {{ surrogate_key(['pickup_date', 'pickup_location_id']) }} as daily_zone_id,
    pickup_date,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    count(*) as trip_count,
    sum(total_amount) as revenue,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    round(avg(trip_distance_miles), 2) as avg_trip_distance_miles
from {{ ref('int_trips_enriched') }}
group by pickup_date, pickup_location_id, pickup_borough, pickup_zone
