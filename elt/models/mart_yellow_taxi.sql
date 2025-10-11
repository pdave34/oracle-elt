{{
  config(
    materialized='table'
  )
}}

SELECT
    TRUNC(pickup_datetime) AS trip_date,
    COUNT(trip_id) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_trip_distance,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue,
    CURRENT_TIMESTAMP AS refresh_date
FROM {{ ref('stg_yellow_taxi') }}
GROUP BY
    TRUNC(pickup_datetime)
