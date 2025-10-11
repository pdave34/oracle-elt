{{
  config(
    materialized = 'incremental',
    unique_key = 'result_id',
    incremental_strategy = 'delete+insert'
  )
}}

with empty_table as (
    select
        cast('A' as nvarchar2(250)) as result_id,
        cast('A' as nvarchar2(250)) as invocation_id,
        cast('A' as nvarchar2(250)) as unique_id,
        cast('A' as nvarchar2(250)) as database_name,
        cast('A' as nvarchar2(250)) as schema_name,
        cast('A' as nvarchar2(250)) as name,
        cast('A' as nvarchar2(250)) as resource_type,
        cast('A' as nvarchar2(250)) as status,
        0.332 as execution_time,
        10000000 as rows_affected,
        cast(current_timestamp as timestamp) as generated_at
    from dual
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0