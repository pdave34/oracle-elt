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
select
    cast(STANDARD_HASH(RR.trip_month || RR.taxi_type, 'SHA256') as varchar2(256)) AS trip_id
    , cast(RR.trip_month as varchar2(7)) AS trip_month
    , RR.total_trips
    , RR.total_passengers
    , RR.total_trip_distance
    , RR.total_fare_amount
    , RR.total_tip_amount
    , RR.total_revenue
    , RR.taxi_type
    , cast(CURRENT_TIMESTAMP as timestamp) AS refresh_date
from RR
order by 
    RR.trip_month ASC, 
    RR.taxi_type ASC