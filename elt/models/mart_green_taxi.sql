{{
  config(
    materialized='table'
  )
}}

SELECT
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM') AS trip_month,
    COUNT(1) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_trip_distance,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue,
    CURRENT_TIMESTAMP AS refresh_date
FROM {{ ref('stg_green_taxi') }}
GROUP BY
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM')
ORDER BY 1 ASC