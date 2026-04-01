{{
  config(
    unique_key=['operation_date', 'sku_code', 'warehouse_name'],
  )
}}

-- =====================================================
-- Intermediate Model: Daily Inventory Changes (Aggregated)
-- =====================================================
-- Purpose: Aggregate quantities from each operation type separately
-- Pattern: 10 CTEs for 10 operation types → UNION keys → LEFT JOIN all
-- =====================================================

with po_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as po_quantity
    from {{ ref('int_inventory_po') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

return_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as return_quantity
    from {{ ref('int_inventory_return') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

cancel_outbound_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as cancel_outbound_quantity
    from {{ ref('int_inventory_cancel_outbound') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

move_in_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as move_in_quantity
    from {{ ref('int_inventory_move_in') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

adjust_in_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as adjust_in_quantity
    from {{ ref('int_inventory_adjust_in') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

outbound_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as outbound_quantity
    from {{ ref('int_inventory_outbound') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

move_out_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as move_out_quantity
    from {{ ref('int_inventory_move_out') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

adjust_out_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as adjust_out_quantity
    from {{ ref('int_inventory_adjust_out') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

other_in_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as other_in_quantity
    from {{ ref('int_inventory_other_in') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

other_out_agg as (
    select
        operation_date, sku_code, warehouse_name,
        sum(quantity) as other_out_quantity
    from {{ ref('int_inventory_other_out') }}
    {% if is_incremental() %}
    WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
    {% endif %}
    group by operation_date, sku_code, warehouse_name
),

-- Get ALL unique date/sku/warehouse combinations across all operation types
all_keys as (
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_po') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_return') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_cancel_outbound') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_move_in') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_adjust_in') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_outbound') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_move_out') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_adjust_out') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_other_in') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
    union
    select distinct operation_date, sku_code, warehouse_name from {{ ref('int_inventory_other_out') }}
    {% if is_incremental() %} WHERE operation_date >= DATEADD(day, -90, CAST(GETDATE() AS date)) {% endif %}
)

-- Join all operation types on composite key (date + sku + warehouse)
select
    k.operation_date,
    k.sku_code,
    k.warehouse_name,

    -- Separate columns for each operation type
    coalesce(po.po_quantity, 0) as po_quantity,
    coalesce(ret.return_quantity, 0) as return_quantity,
    coalesce(co.cancel_outbound_quantity, 0) as cancel_outbound_quantity,
    coalesce(mi.move_in_quantity, 0) as move_in_quantity,
    coalesce(ai.adjust_in_quantity, 0) as adjust_in_quantity,
    coalesce(ob.outbound_quantity, 0) as outbound_quantity,
    coalesce(mo.move_out_quantity, 0) as move_out_quantity,
    coalesce(ao.adjust_out_quantity, 0) as adjust_out_quantity,
    coalesce(oi.other_in_quantity, 0) as other_in_quantity,
    coalesce(oo.other_out_quantity, 0) as other_out_quantity,

    -- Total net change (sum of all operations)
    coalesce(po.po_quantity, 0) + coalesce(ret.return_quantity, 0)
    + coalesce(co.cancel_outbound_quantity, 0) + coalesce(mi.move_in_quantity, 0)
    + coalesce(ai.adjust_in_quantity, 0) + coalesce(ob.outbound_quantity, 0)
    + coalesce(mo.move_out_quantity, 0) + coalesce(ao.adjust_out_quantity, 0)
    + coalesce(oi.other_in_quantity, 0) + coalesce(oo.other_out_quantity, 0)
    as daily_quantity_change

from all_keys k
left join po_agg po               on k.operation_date = po.operation_date and k.sku_code = po.sku_code and k.warehouse_name = po.warehouse_name
left join return_agg ret           on k.operation_date = ret.operation_date and k.sku_code = ret.sku_code and k.warehouse_name = ret.warehouse_name
left join cancel_outbound_agg co   on k.operation_date = co.operation_date and k.sku_code = co.sku_code and k.warehouse_name = co.warehouse_name
left join move_in_agg mi           on k.operation_date = mi.operation_date and k.sku_code = mi.sku_code and k.warehouse_name = mi.warehouse_name
left join adjust_in_agg ai         on k.operation_date = ai.operation_date and k.sku_code = ai.sku_code and k.warehouse_name = ai.warehouse_name
left join outbound_agg ob          on k.operation_date = ob.operation_date and k.sku_code = ob.sku_code and k.warehouse_name = ob.warehouse_name
left join move_out_agg mo          on k.operation_date = mo.operation_date and k.sku_code = mo.sku_code and k.warehouse_name = mo.warehouse_name
left join adjust_out_agg ao        on k.operation_date = ao.operation_date and k.sku_code = ao.sku_code and k.warehouse_name = ao.warehouse_name
left join other_in_agg oi          on k.operation_date = oi.operation_date and k.sku_code = oi.sku_code and k.warehouse_name = oi.warehouse_name
left join other_out_agg oo         on k.operation_date = oo.operation_date and k.sku_code = oo.sku_code and k.warehouse_name = oo.warehouse_name
