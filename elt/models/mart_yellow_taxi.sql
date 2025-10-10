{{
  config(
    materialized='table'
) }}

SELECT
    y.*,
    c."YEAR",
    c."MONTH",
    c."DAY",
    c."QUARTER",
    c."WEEK",
    c."ISODOW",
    c."DOY",
    c."ISO_YEAR_NUM"
FROM {{ ref('stg_yellow_taxi') }} y
LEFT JOIN {{ ref('dim_calendar') }} c ON TRUNC(y.pickup_datetime) = c."DATE"
