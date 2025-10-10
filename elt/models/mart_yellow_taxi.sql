
WITH yellow_taxi AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
),
calendar AS (
    SELECT * FROM {{ ref('dim_calendar') }}
)
SELECT
    y.*,
    c."year",
    c."month",
    c."day",
    c."quarter",
    c."week",
    c."isodow",
    c."doy",
    c."iso_year_num",
    (EXTRACT(DAY FROM (y.dropoff_datetime - y.pickup_datetime)) * 24 * 60) +
    (EXTRACT(HOUR FROM (y.dropoff_datetime - y.pickup_datetime)) * 60) +
    EXTRACT(MINUTE FROM (y.dropoff_datetime - y.pickup_datetime)) AS trip_duration_minutes,
    CASE
        WHEN (EXTRACT(DAY FROM (y.dropoff_datetime - y.pickup_datetime)) * 24 * 60) +
             (EXTRACT(HOUR FROM (y.dropoff_datetime - y.pickup_datetime)) * 60) +
             EXTRACT(MINUTE FROM (y.dropoff_datetime - y.pickup_datetime)) > 0
        THEN y.trip_distance / (((EXTRACT(DAY FROM (y.dropoff_datetime - y.pickup_datetime)) * 24 * 60) +
                               (EXTRACT(HOUR FROM (y.dropoff_datetime - y.pickup_datetime)) * 60) +
                               EXTRACT(MINUTE FROM (y.dropoff_datetime - y.pickup_datetime))) / 60)
        ELSE 0
    END AS average_speed_mph,
    EXTRACT(HOUR FROM y.pickup_datetime) AS pickup_hour
FROM yellow_taxi y
LEFT JOIN calendar c ON TRUNC(y.pickup_datetime) = c."date"
