{{
    config(
        materialized='table'
    )
}}

-- This query is written using nested subqueries to avoid an Oracle parsing bug (ORA-32039)
-- where the WITH clause was being incorrectly interpreted as recursive.
with stg_replen as (
    select
        LOC_ID,
        ITEM_FAMILY,
        TARGET,
        FAMILY_SUBTOTAL,
        row_number() over (order by LOC_ID, ITEM_FAMILY) as process_id
    from {{ ref('INIT_REPLEN') }}
)
select
    loc_id,
    item_number,
    item_family,
    order_qty
from (
    -- Step 6: Calculate the final order quantity
    select
        loc_id,
        item_number,
        item_family,
        priority,
        case
            when priority = max_priority then greatest(0, target - fulfilled_by_higher_priority)
            else greatest(0, least(potential_fulfillment, target - fulfilled_by_higher_priority))
        end as order_qty
    from (
        -- Step 5: Apply the cascading fulfillment logic
        select
            process_id,
            loc_id,
            item_family,
            target,
            item_number,
            priority,
            potential_fulfillment,
            coalesce(sum(potential_fulfillment) over (partition by process_id order by priority rows between unbounded preceding and 1 preceding), 0) as fulfilled_by_higher_priority,
            max(priority) over (partition by process_id) as max_priority
        from (
            -- Step 4: Determine the actual supply available
            select
                process_id,
                loc_id,
                item_family,
                target,
                item_number,
                priority,
                least(target, greatest(0, supply_qty - (cumulative_demand_for_item - target))) as potential_fulfillment
            from (
                -- Step 3: Calculate the cumulative demand for each item
                select
                    d.process_id,
                    d.loc_id,
                    d.item_family,
                    d.target,
                    d.item_number,
                    d.priority,
                    coalesce(s.supply_qty, 0) as supply_qty,
                    sum(d.target) over (partition by d.item_number order by d.process_id) as cumulative_demand_for_item
                from (
                    -- Step 2b: Calculate priority
                    select
                        process_id,
                        loc_id,
                        item_family,
                        target,
                        item_number,
                        row_number() over (partition by process_id order by preference_level desc) as priority
                    from (
                        -- Step 2: Unpack the ITEM_FAMILY string
                        select process_id, loc_id, item_family, target, trim(regexp_substr(item_family, '[^/]+', 1, 1)) as item_number, 1 as preference_level from stg_replen
                        union all
                        select process_id, loc_id, item_family, target, trim(regexp_substr(item_family, '[^/]+', 1, 2)) as item_number, 2 as preference_level from stg_replen
                        union all
                        select process_id, loc_id, item_family, target, trim(regexp_substr(item_family, '[^/]+', 1, 3)) as item_number, 3 as preference_level from stg_replen
                        union all
                        select process_id, loc_id, item_family, target, trim(regexp_substr(item_family, '[^/]+', 1, 4)) as item_number, 4 as preference_level from stg_replen
                        union all
                        select process_id, loc_id, item_family, target, trim(regexp_substr(item_family, '[^/]+', 1, 5)) as item_number, 5 as preference_level from stg_replen
                    ) unpacked_base
                    where unpacked_base.item_number is not null
                ) d
                left join {{ ref('SUPPLY') }} s on d.item_number = s.item_number
            )
        )
    )
)
where order_qty > 0
order by loc_id, item_family, priority