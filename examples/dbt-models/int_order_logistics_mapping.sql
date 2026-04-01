{{
  config(
    schema='intermediate',
    unique_key='internal_order_number',
  )
}}

-- =====================================================
-- Intermediate Model: Main Order (Combined Orders)
-- =====================================================
-- Purpose: Combine normal orders and child orders with logistics type mapping
-- Grain: 1 row per tracking number
-- =====================================================

WITH combined_orders AS (
  -- Normal orders
  SELECT
    internal_order_number,
    internal_order_number AS original_internal_order_number,
    online_order_number,
    tracking_number AS track_no,
    order_time,
    order_date,
    order_time_only,
    'Normal' AS order_type,
    order_status,
    shipping_status,
    tags,
    store,
    platform,
    outbound_warehouse,
    shipping_company,
    products
  FROM {{ ref('int_fulfillment_order') }}

  UNION ALL

  -- Child orders (inherit order_date/order_time_only from parent)
  SELECT
    odc.internal_order_number,
    parent.internal_order_number AS original_internal_order_number,
    odc.online_order_number,
    odc.child_track AS track_no,
    odc.order_time,
    parent.order_date,
    parent.order_time_only,
    'Child Order' AS order_type,
    odc.order_status,
    odc.shipping_status,
    odc.tags,
    odc.store,
    odc.platform,
    odc.outbound_warehouse,
    odc.shipping_company,
    odc.products
  FROM {{ ref('int_fulfillment_order_child') }} odc
  LEFT JOIN {{ ref('int_fulfillment_order') }} parent
    ON odc.parent_track = parent.tracking_number
)

-- Map logistics types (standardize carrier names)
SELECT
  internal_order_number,
  original_internal_order_number,
  online_order_number,
  track_no,
  order_time,
  order_date,
  order_time_only,
  order_type,
  order_status,
  shipping_status,
  tags,
  store,
  platform,
  outbound_warehouse,
  shipping_company,

  -- Standardized Shipping Company Mapping
  CASE
    WHEN UPPER(shipping_company) LIKE '%KEX%' OR UPPER(shipping_company) LIKE '%KERRY%' THEN 'KEX'
    WHEN UPPER(shipping_company) LIKE '%LEX%' THEN 'LEX'
    WHEN UPPER(shipping_company) LIKE '%FLASH%' THEN 'FLASH'
    WHEN UPPER(shipping_company) LIKE '%J&T%' THEN 'J&T'
    WHEN UPPER(shipping_company) LIKE '%DHL%' THEN 'DHL'
    WHEN UPPER(shipping_company) LIKE '%SPX%' THEN 'SPX'
    WHEN UPPER(shipping_company) LIKE '%STANDARD DELIVERY%' THEN 'SPX'
    WHEN UPPER(shipping_company) LIKE '%SAME DAY%' THEN 'SPX Same Day'
    WHEN UPPER(shipping_company) LIKE '%INSTANT DELIVERY%' THEN 'SPX Same Day'
    WHEN UPPER(shipping_company) LIKE '%EXPRESS DELIVERY%' THEN 'SPX Same Day'
    WHEN UPPER(shipping_company) LIKE '%SELF%' THEN 'Self Pickup'
    WHEN UPPER(shipping_company) LIKE '%WHOLESALE%' THEN 'In-House'
    ELSE 'Unknown'
  END AS standardized_shipping_company,

  products

FROM combined_orders
WHERE order_status <> 'Split Order'
{% if is_incremental() %}
  AND order_time >= DATEADD(day, -90, CAST(GETDATE() AS date))
{% endif %}
