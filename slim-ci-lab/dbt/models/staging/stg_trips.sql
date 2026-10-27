-- One row per raw trip record: renamed, typed, no business rules yet.
with source as (

    select * from {{ source('tlc', 'trips') }}

),

renamed as (

    select
        vendorid as vendor_id,
        tpep_pickup_datetime as pickup_at,
        tpep_dropoff_datetime as dropoff_at,
        passenger_count,
        trip_distance as trip_distance_miles,
        ratecodeid as rate_code_id,
        store_and_fwd_flag = 'Y' as is_store_and_forward,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type as payment_type_id,
        -- Money as fixed-point so sums are exact.
        cast(fare_amount as number(10, 2)) as fare_amount,
        cast(extra as number(10, 2)) as extra_amount,
        cast(mta_tax as number(10, 2)) as mta_tax_amount,
        cast(tip_amount as number(10, 2)) as tip_amount,
        cast(tolls_amount as number(10, 2)) as tolls_amount,
        cast(improvement_surcharge as number(10, 2)) as improvement_surcharge_amount,
        cast(congestion_surcharge as number(10, 2)) as congestion_surcharge_amount,
        cast(airport_fee as number(10, 2)) as airport_fee_amount,
        cast(total_amount as number(10, 2)) as total_amount,
        _source_file,
        _source_row,
        _loaded_at
    from source

)

select * from renamed
