WITH
allocation_cte (
    loc_id,
    item_family,
    target,
    remaining_demand,
    item_number,
    preference_level,
    assigned_quantity
) AS (
    -- Anchor member: start from INIT_REPLEN and take the first preference level from the parsed item_family
    SELECT
        d.loc_id,
        d.item_family,
        d.target,
        d.target AS remaining_demand,
        h.item_number,
        h.preference_level,
        0 AS assigned_quantity
    FROM (
        SELECT loc_id, item_family, target FROM {{ ref('INIT_REPLEN') }}
    ) d
    JOIN (
        SELECT
            item_family,
            regexp_substr(item_family, '[^/]+', 1, level) AS item_number,
            level AS preference_level
        FROM (
            SELECT DISTINCT item_family FROM {{ ref('INIT_REPLEN') }}
        )
        CONNECT BY level <= regexp_count(item_family, '/') + 1
            AND PRIOR item_family = item_family
            AND PRIOR sys_guid() IS NOT NULL
    ) h ON d.item_family = h.item_family
    WHERE h.preference_level = 1

    UNION ALL

    -- Recursive member: walk to the next preference level and assign from SUPPLY
    SELECT
        prev.loc_id,
        prev.item_family,
        prev.target,
        prev.remaining_demand - LEAST(prev.remaining_demand, s.supply_qty) AS remaining_demand,
        h.item_number,
        h.preference_level,
        LEAST(prev.remaining_demand, s.supply_qty) AS assigned_quantity
    FROM allocation_cte prev
    JOIN (
        SELECT
            item_family,
            regexp_substr(item_family, '[^/]+', 1, level) AS item_number,
            level AS preference_level
        FROM (
            SELECT DISTINCT item_family FROM {{ ref('INIT_REPLEN') }}
        )
        CONNECT BY level <= regexp_count(item_family, '/') + 1
            AND PRIOR item_family = item_family
            AND PRIOR sys_guid() IS NOT NULL
    ) h ON prev.item_family = h.item_family AND h.preference_level = prev.preference_level + 1
    LEFT JOIN (
        SELECT item_pos, item_family, item_number, supply_qty FROM {{ ref('SUPPLY') }}
    ) s ON h.item_number = s.item_number
)

SELECT
    loc_id,
    item_family,
    item_number,
    SUM(assigned_quantity) AS assigned_quantity,
    target AS requested_quantity,
    CASE
        WHEN SUM(assigned_quantity) < target THEN target - SUM(assigned_quantity)
        ELSE 0
    END AS ordered_quantity
FROM allocation_cte
GROUP BY loc_id, item_family, item_number, target