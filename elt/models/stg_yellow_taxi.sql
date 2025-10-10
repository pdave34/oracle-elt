{{
  config(
    materialized='table'
) }}

WITH L1 AS (

    SELECT
        ROWNUM as rn,
        a."VENDORID",
        a."TPEP_PICKUP_DATETIME",
        a."TPEP_DROPOFF_DATETIME",
        a."PASSENGER_COUNT",
        a."TRIP_DISTANCE",
        a."RATECODEID",
        a."STORE_AND_FWD_FLAG",
        a."PULOCATIONID",
        a."DOLOCATIONID",
        a."PAYMENT_TYPE",
        a."FARE_AMOUNT",
        a."EXTRA",
        a."MTA_TAX",
        a."TIP_AMOUNT",
        a."TOLLS_AMOUNT",
        a."IMPROVEMENT_SURCHARGE",
        a."TOTAL_AMOUNT",
        a."CONGESTION_SURCHARGE",
        a."AIRPORT_FEE"
    FROM {{ source('pdave', 'RAW_YELLOW_TAXI') }} a

),

renamed AS (

    SELECT
        rn AS trip_id,
        "VENDORID" AS vendor_id,
        "TPEP_PICKUP_DATETIME" AS pickup_datetime,
        "TPEP_DROPOFF_DATETIME" AS dropoff_datetime,
        NVL("PASSENGER_COUNT", 0) AS passenger_count,
        CASE WHEN "TRIP_DISTANCE" < 0 THEN 0 ELSE "TRIP_DISTANCE" END AS trip_distance,
        "RATECODEID" AS rate_code_id,
        CASE WHEN "STORE_AND_FWD_FLAG" = 'Y' THEN 1 ELSE 0 END AS store_and_fwd_flag,
        "PULOCATIONID" AS pickup_location_id,
        "DOLOCATIONID" AS dropoff_location_id,
        "PAYMENT_TYPE" AS payment_type,
        CASE WHEN "FARE_AMOUNT" < 0 THEN 0 ELSE "FARE_AMOUNT" END AS fare_amount,
        CASE WHEN "EXTRA" < 0 THEN 0 ELSE "EXTRA" END AS extra,
        CASE WHEN "MTA_TAX" < 0 THEN 0 ELSE "MTA_TAX" END AS mta_tax,
        CASE WHEN "TIP_AMOUNT" < 0 THEN 0 ELSE "TIP_AMOUNT" END AS tip_amount,
        CASE WHEN "TOLLS_AMOUNT" < 0 THEN 0 ELSE "TOLLS_AMOUNT" END AS tolls_amount,
        CASE WHEN "IMPROVEMENT_SURCHARGE" < 0 THEN 0 ELSE "IMPROVEMENT_SURCHARGE" END AS improvement_surcharge,
        CASE WHEN "TOTAL_AMOUNT" < 0 THEN 0 ELSE "TOTAL_AMOUNT" END AS total_amount,
        CASE WHEN "CONGESTION_SURCHARGE" < 0 THEN 0 ELSE "CONGESTION_SURCHARGE" END AS congestion_surcharge,
        CASE WHEN "AIRPORT_FEE" < 0 THEN 0 ELSE "AIRPORT_FEE" END AS airport_fee
    FROM L1
    WHERE "PULOCATIONID" IS NOT NULL AND "DOLOCATIONID" IS NOT NULL

)

SELECT RR.*, pickup_borough.Borough AS pickup_borough, pickup_borough.Zone AS pickup_zone, pickup_borough.service_zone AS pickup_service_zone,
       dropoff_borough.Borough AS dropoff_borough, dropoff_borough.Zone AS dropoff_zone, dropoff_borough.service_zone AS dropoff_service_zone
FROM renamed RR
LEFT JOIN {{ref('RAW_TAXI_ZONES')}} pickup_borough
    ON RR.pickup_location_id = pickup_borough.LOCATIONID
LEFT JOIN {{ref('RAW_TAXI_ZONES')}} dropoff_borough
    ON RR.dropoff_location_id = dropoff_borough.LOCATIONID
