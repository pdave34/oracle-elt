
{{
  config(
    materialized='table'
) }}

with date_series as (
  select
    (to_date('{{ var("start_date", "2000-01-01") }}', 'YYYY-MM-DD') + level - 1) as calendar_date
  from
    dual
  connect by level <= to_date('{{ var("end_date", "2099-12-31") }}', 'YYYY-MM-DD') - to_date('{{ var("start_date", "2000-01-01") }}', 'YYYY-MM-DD') + 1
)
select
  calendar_date as "DATE",
  to_number(to_char(calendar_date, 'YYYY')) as "YEAR",
  to_number(to_char(calendar_date, 'MM')) as "MONTH",
  to_number(to_char(calendar_date, 'DD')) as "DAY",
  to_number(to_char(calendar_date, 'Q')) as "QUARTER",
  to_number(to_char(calendar_date, 'WW')) as "WEEK",
  to_number(to_char(calendar_date, 'ID')) as "ISODOW",
  to_number(to_char(calendar_date, 'DDD')) as "DOY",
  to_number(to_char(calendar_date, 'IYYY')) as "ISO_YEAR_NUM"
from
  date_series
