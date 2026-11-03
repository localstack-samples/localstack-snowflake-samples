{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_id',
        on_schema_change='append_new_columns'
    )
}}

-- Billable trips in the reporting month, keyed and enriched with zone names.
-- Incremental: each run re-merges a lookback window, so a corrected trip
-- updates its row (matched on trip_id) instead of being appended twice.

with trips as (

    select *
    from {{ ref('stg_trips') }}
    where pickup_at >= '{{ var("reporting_start") }}'
      and pickup_at < '{{ var("reporting_end") }}'
      -- Negative totals are voids and refunds of an earlier trip, not revenue.
      and total_amount >= 0
    {% if is_incremental() %}
      and pickup_at >= (
          select dateadd(day, -{{ var('incremental_lookback_days') }}, max(pickup_at))
          from {{ this }}
      )
    {% endif %}

),

keyed as (

    select
        *,
        -- The TLC feed has no trip id, and the same vendor, timestamps and
        -- zones can appear on more than one record. Number the repeats so the
        -- surrogate key stays unique.
        row_number() over (
            partition by vendor_id, pickup_at, dropoff_at, pickup_location_id, dropoff_location_id
            order by _source_file, _source_row
        ) as natural_key_seq
    from trips

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

)

select
    {{ surrogate_key([
        'keyed.vendor_id', 'keyed.pickup_at', 'keyed.dropoff_at',
        'keyed.pickup_location_id', 'keyed.dropoff_location_id', 'keyed.natural_key_seq'
    ]) }} as trip_id,
    keyed.natural_key_seq,
    keyed.vendor_id,
    keyed.pickup_at,
    cast(keyed.pickup_at as date) as pickup_date,
    keyed.dropoff_at,
    datediff('second', keyed.pickup_at, keyed.dropoff_at) as trip_duration_seconds,
    keyed.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone_name as pickup_zone,
    keyed.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone_name as dropoff_zone,
    keyed.passenger_count,
    keyed.trip_distance_miles,
    keyed.payment_type_id,
    keyed.fare_amount,
    keyed.tip_amount,
    keyed.tolls_amount,
    keyed.total_amount,
    keyed._source_file,
    keyed._loaded_at
from keyed
left join zones as pickup_zone
    on keyed.pickup_location_id = pickup_zone.location_id
left join zones as dropoff_zone
    on keyed.dropoff_location_id = dropoff_zone.location_id
