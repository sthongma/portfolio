{{
  config(
    materialized='incremental',
    unique_key='fulfillment_sk',
    incremental_strategy='delete+insert',
    schema='gold',
    post_hook="{{ create_indexes('fulfillment_sk', ['internal_order_number', 'order_time', 'fulfillment_status', 'store', 'platform', 'track_no']) }}"
  )
}}

-- =====================================================
-- Gold Model: Fact Fulfillment
-- =====================================================
-- Purpose: Comprehensive fulfillment fact table combining all events
-- Grain: 1 row per internal_order_number
-- Architecture: 6-layer CTE pipeline
--   1. base_order     - Filter orders for incremental processing
--   2. working_days   - Pre-compute next working days (holiday-aware)
--   3. joined_events  - Join 13 event tables + calculate deadlines
--   4. with_flags     - Add 14 process completion flags
--   5. with_lead_time - Calculate 3 lead time metrics
--   6. fact_final     - Status classification + deadline adherence (FFR)
-- Output: ~102 columns
-- =====================================================

WITH base_order AS (
  SELECT *
  FROM {{ ref('int_fulfillment_main_order') }}
  {% if is_incremental() %}
  WHERE order_time >= DATEADD(day, -14,
    COALESCE(
      (SELECT MAX(order_time) FROM {{ this }}),
      '1900-01-01'
    )
  )
  {% endif %}
),

-- Pre-compute next working days (excluding Sundays and holidays)
working_days AS (
  SELECT
    full_date,
    day_of_week,
    is_platform_holiday,
    (
      SELECT TOP 1 d2.full_date
      FROM {{ ref('dim_date_with_holidays') }} d2
      WHERE d2.full_date >= d1.full_date
        AND DATENAME(WEEKDAY, d2.full_date) <> 'Sunday'
        AND d2.is_platform_holiday = 0
      ORDER BY d2.full_date
    ) AS next_working_day_inclusive,
    (
      SELECT TOP 1 d2.full_date
      FROM {{ ref('dim_date_with_holidays') }} d2
      WHERE d2.full_date > d1.full_date
        AND DATENAME(WEEKDAY, d2.full_date) <> 'Sunday'
        AND d2.is_platform_holiday = 0
      ORDER BY d2.full_date
    ) AS next_working_day_exclusive
  FROM {{ ref('dim_date_with_holidays') }} d1
),

-- Pre-compute working days for release date calculation
working_days_release AS (
  SELECT
    full_date,
    is_working_day,
    (
      SELECT TOP 1 d2.full_date
      FROM {{ ref('dim_date_with_holidays') }} d2
      WHERE d2.full_date > d1.full_date
        AND d2.is_working_day = 1
      ORDER BY d2.full_date
    ) AS next_working_day
  FROM {{ ref('dim_date_with_holidays') }} d1
),

-- CTE 1: JOIN ALL EVENT TABLES (13 event sources)
joined_events AS (
  SELECT
    {{ hash_surrogate_key(['base_order.internal_order_number'], 'fulfillment_sk') }},

    -- Base order (17 columns)
    base_order.internal_order_number,
    base_order.original_internal_order_number,
    base_order.online_order_number,
    base_order.track_no,
    base_order.order_time,
    base_order.order_type,
    base_order.order_status,
    base_order.store,
    base_order.platform,
    base_order.outbound_warehouse,
    base_order.standardized_shipping_company,

    -- Fulfillment deadline (holiday-aware business day logic)
    CASE
      WHEN ISNULL(w.is_platform_holiday, 0) = 1 OR DATENAME(WEEKDAY, base_order.order_time) = 'Sunday'
        THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(w.next_working_day_inclusive AS DATETIME)))
      ELSE DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(w.next_working_day_exclusive AS DATETIME)))
    END AS deadline_ffr,

    -- Extended deadline (considers order time < 12:00 cutoff)
    CASE
      WHEN ISNULL(w.is_platform_holiday, 0) = 1 OR DATENAME(WEEKDAY, base_order.order_time) = 'Sunday'
        THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(w.next_working_day_exclusive AS DATETIME)))
      WHEN base_order.order_time_only < '12:00:00'
        THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(base_order.order_date AS DATETIME)))
      ELSE DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(w.next_working_day_exclusive AS DATETIME)))
    END AS deadline_ffr_plus,

    -- Expected release date
    CASE
      WHEN w_order.is_working_day = 0 THEN w_order.next_working_day
      WHEN base_order.order_time_only >= '12:00:00' THEN w_order.next_working_day
      ELSE base_order.order_date
    END AS expected_release_date,

    -- Release event
    release_ev.printed_time,
    release_ev.printed_date,
    release_ev.release_operator,
    CASE
      WHEN release_ev.printed_date IS NULL THEN NULL
      WHEN w_release.is_working_day = 0 THEN w_release.next_working_day
      WHEN release_ev.printed_time_only >= '15:00:00' THEN w_release.next_working_day
      ELSE release_ev.printed_date
    END AS release_date,

    -- Process events (scan datetime + operator for each)
    inprocess.inprocess_scan_datetime,
    inprocess.inprocess_scan_date,
    inprocess.inprocess_operator,
    outbound.outbound_scan_datetime,
    outbound.outbound_scan_date,
    loading.loading_scan_datetime,
    loading.loading_scan_date,
    cancel_ev.cancel_datetime,
    cancel_ev.cancel_date,
    collect.collect_datetime,
    collect.collect_date,
    returning.returning_datetime,
    returning.returning_date,
    returned.returned_datetime,
    returned.returned_date,
    return_wh.return_warehouse_scan_datetime,
    return_wh.return_warehouse_scan_date,
    repack.repack_scan_datetime,
    repack.repack_scan_date

  FROM base_order
  LEFT JOIN working_days w ON base_order.order_date = w.full_date
  LEFT JOIN working_days_release w_order ON base_order.order_date = w_order.full_date
  LEFT JOIN {{ ref('int_fulfillment_release') }} release_ev
    ON base_order.original_internal_order_number = release_ev.internal_order_number
  LEFT JOIN working_days_release w_release ON release_ev.printed_date = w_release.full_date
  LEFT JOIN {{ ref('int_fulfillment_cancel') }} cancel_ev
    ON base_order.original_internal_order_number = cancel_ev.internal_order_number
  LEFT JOIN {{ ref('int_fulfillment_inprocess') }} inprocess ON base_order.track_no = inprocess.track_no
  LEFT JOIN {{ ref('int_fulfillment_outbound') }} outbound ON base_order.track_no = outbound.track_no
  LEFT JOIN {{ ref('int_fulfillment_loading') }} loading ON base_order.track_no = loading.track_no
  LEFT JOIN {{ ref('int_fulfillment_collect') }} collect ON base_order.track_no = collect.track_no
  LEFT JOIN {{ ref('int_fulfillment_returning') }} returning ON base_order.track_no = returning.track_no
  LEFT JOIN {{ ref('int_fulfillment_returned') }} returned ON base_order.track_no = returned.track_no
  LEFT JOIN {{ ref('int_fulfillment_return_warehouse') }} return_wh ON base_order.track_no = return_wh.track_no
  LEFT JOIN {{ ref('int_fulfillment_repack') }} repack ON base_order.track_no = repack.track_no
),

-- CTE 2: PROCESS COMPLETION FLAGS (14 binary flags)
with_flags AS (
  SELECT *,
    CASE WHEN release_date IS NOT NULL THEN 1 ELSE 0 END AS has_release_event,
    CASE WHEN inprocess_scan_date IS NOT NULL THEN 1 ELSE 0 END AS has_inprocess_event,
    CASE WHEN outbound_scan_date IS NOT NULL THEN 1 ELSE 0 END AS has_outbound_event,
    CASE WHEN loading_scan_date IS NOT NULL THEN 1 ELSE 0 END AS has_loading_event,
    CASE WHEN cancel_date IS NOT NULL THEN 1 ELSE 0 END AS is_cancelled,
    CASE WHEN collect_date IS NOT NULL THEN 1 ELSE 0 END AS has_collect_event,
    CASE WHEN returning_date IS NOT NULL THEN 1 ELSE 0 END AS has_returning,
    CASE WHEN returned_date IS NOT NULL THEN 1 ELSE 0 END AS has_returned,
    CASE WHEN return_warehouse_scan_date IS NOT NULL THEN 1 ELSE 0 END AS has_return_warehouse,
    CASE WHEN repack_scan_date IS NOT NULL THEN 1 ELSE 0 END AS has_repack_event
  FROM joined_events
),

-- CTE 3: LEAD TIME METRICS
with_lead_time AS (
  SELECT *,
    CASE
      WHEN order_time IS NOT NULL AND collect_datetime IS NOT NULL
      THEN DATEDIFF(day, CAST(order_time AS DATE), collect_date)
    END AS days_order_to_collect,

    CASE
      WHEN collect_datetime IS NOT NULL AND outbound_scan_datetime IS NOT NULL
      THEN DATEDIFF(day, collect_date, outbound_scan_date)
    END AS days_collect_to_outbound,

    CASE
      WHEN order_time IS NOT NULL AND outbound_scan_datetime IS NOT NULL
      THEN DATEDIFF(day, CAST(order_time AS DATE), outbound_scan_date)
    END AS total_fulfillment_days
  FROM with_flags
),

-- CTE 4: STATUS CLASSIFICATION + DEADLINE METRICS
fact_final AS (
  SELECT *,

    -- Fulfillment status (priority-based classification)
    CASE
      WHEN repack_scan_date IS NOT NULL THEN 'Repacking'
      WHEN return_warehouse_scan_date IS NOT NULL THEN 'Returned to Warehouse'
      WHEN returned_date IS NOT NULL THEN 'Returned'
      WHEN returning_date IS NOT NULL THEN 'Returning'
      WHEN collect_date IS NOT NULL THEN 'Collected'
      WHEN cancel_date IS NOT NULL THEN 'Cancelled'
      WHEN loading_scan_date IS NOT NULL THEN 'Loading'
      WHEN outbound_scan_date IS NOT NULL THEN 'Shipped'
      WHEN inprocess_scan_date IS NOT NULL THEN 'Processing'
      WHEN release_date IS NOT NULL THEN 'Released'
      ELSE 'Pending'
    END AS fulfillment_status,

    -- FFR (First Fulfillment Rate) deadline adherence
    -- Cancelled orders without collection are excluded (NULL)
    -- Pending orders before deadline are also NULL (not yet scored)
    CASE
      WHEN is_cancelled = 1 AND has_collect_event = 0 THEN NULL
      WHEN deadline_ffr IS NULL THEN NULL
      WHEN outbound_scan_datetime IS NOT NULL AND outbound_scan_datetime <= deadline_ffr THEN 1
      WHEN outbound_scan_datetime IS NOT NULL AND outbound_scan_datetime > deadline_ffr THEN 0
      WHEN GETDATE() > deadline_ffr THEN 0
      ELSE NULL
    END AS met_outbound_deadline_ffr,

    CASE
      WHEN is_cancelled = 1 AND has_collect_event = 0 THEN NULL
      WHEN deadline_ffr_plus IS NULL THEN NULL
      WHEN outbound_scan_datetime IS NOT NULL AND outbound_scan_datetime <= deadline_ffr_plus THEN 1
      WHEN outbound_scan_datetime IS NOT NULL AND outbound_scan_datetime > deadline_ffr_plus THEN 0
      WHEN GETDATE() > deadline_ffr_plus THEN 0
      ELSE NULL
    END AS met_outbound_deadline_ffr_plus,

    -- Release deadline adherence
    CASE
      WHEN release_date IS NULL THEN NULL
      WHEN release_date <= expected_release_date THEN 1
      ELSE 0
    END AS is_printed_on_time,

    GETDATE() AS _gold_loaded_at

  FROM with_lead_time
)

SELECT * FROM fact_final
