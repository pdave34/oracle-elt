{{
  config(
    materialized='table'
  )
}}

with green as (
SELECT
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM') AS trip_month,
    COUNT(1) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_trip_distance,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue
FROM {{ ref('stg_green_taxi') }}
GROUP BY
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM')),
yellow as (
    SELECT
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM') AS trip_month,
    COUNT(1) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_trip_distance,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue
FROM {{ ref('stg_yellow_taxi') }}
GROUP BY
    TO_CHAR(TRUNC(pickup_datetime),'YYYY-MM')),
RR as (
    select g.*, 'GREEN' as taxi_type
    from green g
    union all
    select y.*, 'YELLOW' as taxi_type
    from yellow y)
select RR.*, CURRENT_TIMESTAMP AS refresh_date
from RR
order by 
    RR.trip_month ASC, 
    RR.taxi_type ASC