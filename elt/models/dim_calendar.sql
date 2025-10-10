
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
  calendar_date as "date",
  to_number(to_char(calendar_date, 'YYYY')) as "year",
  to_number(to_char(calendar_date, 'MM')) as "month",
  to_number(to_char(calendar_date, 'DD')) as "day",
  to_number(to_char(calendar_date, 'Q')) as "quarter",
  to_number(to_char(calendar_date, 'WW')) as "week",
  to_number(to_char(calendar_date, 'ID')) as "isodow",
  to_number(to_char(calendar_date, 'DDD')) as "doy",
  to_number(to_char(calendar_date, 'IYYY')) as "iso_year_num"
from
  date_series
