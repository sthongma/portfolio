"""
Inventory SQL Queries.

Patterns demonstrated:
- Window functions (ROW_NUMBER for latest record per partition)
- CROSS JOIN for combining summary counts with detail data
- CTE-based KPI calculation
- OFFSET/FETCH NEXT pagination (SQL Server)
- Parameterized queries (:param syntax)
"""

# Summary KPIs: distinct SKU count, warehouse count, and latest balance totals
SUMMARY = """
WITH latest_balance AS (
    SELECT
        sku_code,
        warehouse_name,
        balance as current_balance,
        ROW_NUMBER() OVER (
            PARTITION BY sku_code, warehouse_name
            ORDER BY operation_date DESC
        ) AS rn
    FROM [{schema}].[fact_sku_daily_balance]
    {where_sku_wh}
)
SELECT
    (SELECT COUNT(DISTINCT sku_code) FROM latest_balance WHERE rn = 1) AS total_skus,
    (SELECT COUNT(DISTINCT warehouse_name) FROM latest_balance WHERE rn = 1) AS total_warehouses,
    (SELECT SUM(current_balance) FROM latest_balance WHERE rn = 1) AS total_balance,
    ISNULL(SUM(daily_quantity_change), 0) AS total_daily_change,
    ISNULL(SUM(CASE WHEN daily_quantity_change > 0 THEN daily_quantity_change ELSE 0 END), 0) AS total_inbound,
    ISNULL(SUM(CASE WHEN daily_quantity_change < 0 THEN daily_quantity_change ELSE 0 END), 0) AS total_outbound
FROM [{schema}].[fact_sku_daily_balance]
{where}
"""

# Balance trend: daily aggregation of inbound/outbound across all SKUs
BALANCE_TREND = """
SELECT
    operation_date,
    SUM(balance) AS total_balance,
    SUM(CASE WHEN daily_quantity_change > 0 THEN daily_quantity_change ELSE 0 END) AS inbound,
    SUM(CASE WHEN daily_quantity_change < 0 THEN daily_quantity_change ELSE 0 END) AS outbound
FROM [{schema}].[fact_sku_daily_balance]
{where}
GROUP BY operation_date
ORDER BY operation_date
"""

# Daily movement detail with pagination
DAILY_MOVEMENT = """
SELECT
    operation_date,
    sku_code,
    warehouse_name,
    po_quantity,
    return_quantity,
    outbound_quantity,
    move_in_quantity,
    move_out_quantity,
    adjust_in_quantity,
    adjust_out_quantity,
    daily_quantity_change,
    balance
FROM [{schema}].[fact_sku_daily_balance]
{where}
ORDER BY {order_by}
OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
"""

DAILY_MOVEMENT_COUNT = """
SELECT COUNT(*) FROM [{schema}].[fact_sku_daily_balance] {where}
"""

# SKU list: latest balance per SKU/warehouse using window function
SKU_LIST = """
WITH latest AS (
    SELECT
        sku_code,
        warehouse_name,
        balance AS current_balance,
        daily_quantity_change AS latest_daily_change,
        operation_date AS latest_date,
        CASE WHEN daily_quantity_change > 0 THEN daily_quantity_change ELSE 0 END AS latest_inbound,
        CASE WHEN daily_quantity_change < 0 THEN daily_quantity_change ELSE 0 END AS latest_outbound,
        ROW_NUMBER() OVER (
            PARTITION BY sku_code, warehouse_name
            ORDER BY operation_date DESC
        ) AS rn
    FROM [{schema}].[fact_sku_daily_balance]
)
SELECT *
FROM latest
WHERE rn = 1 {and_sku_wh}
ORDER BY {order_by}
OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
"""

SKU_LIST_COUNT = """
WITH latest AS (
    SELECT
        sku_code, warehouse_name,
        ROW_NUMBER() OVER (PARTITION BY sku_code, warehouse_name ORDER BY operation_date DESC) AS rn
    FROM [{schema}].[fact_sku_daily_balance]
)
SELECT COUNT(*) FROM latest WHERE rn = 1 {and_sku_wh}
"""

# Single SKU detail
SKU_DETAIL = """
SELECT TOP 1
    sku_code,
    warehouse_name,
    balance AS current_balance,
    operation_date AS latest_date
FROM [{schema}].[fact_sku_daily_balance]
WHERE sku_code = :sku_code AND warehouse_name = :warehouse_name
ORDER BY operation_date DESC
"""

# SKU balance trend (for detail page chart)
SKU_BALANCE_TREND = """
SELECT
    operation_date,
    balance AS total_balance,
    CASE WHEN daily_quantity_change > 0 THEN daily_quantity_change ELSE 0 END AS inbound,
    CASE WHEN daily_quantity_change < 0 THEN daily_quantity_change ELSE 0 END AS outbound
FROM [{schema}].[fact_sku_daily_balance]
WHERE sku_code = :sku_code AND warehouse_name = :warehouse_name
ORDER BY operation_date
"""

# Filter options
FILTER_OPTIONS_WAREHOUSES = """
SELECT DISTINCT warehouse_name
FROM [{schema}].[fact_sku_daily_balance]
ORDER BY warehouse_name
"""

FILTER_OPTIONS_DATE_RANGE = """
SELECT
    MIN(operation_date) AS min_date,
    MAX(operation_date) AS max_date
FROM [{schema}].[fact_sku_daily_balance]
"""
