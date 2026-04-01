# dbt Models — Code Examples

Sanitized examples from a production data warehouse with 193 dbt models across a medallion architecture (Bronze → Staging → Silver → Intermediate → Gold) on SQL Server.

## Files

| File | Layer | What it demonstrates |
|------|-------|---------------------|
| `staging_orders.sql` | Staging | Macro-based type casting, NULL standardization, cast failure tracking |
| `silver_product_bom.sql` | Silver | Incremental with delete+insert, ROW_NUMBER dedup, surrogate keys, post-hook indexes |
| `int_order_logistics_mapping.sql` | Intermediate | UNION ALL, LEFT JOIN inheritance, 13-condition CASE mapping, incremental filter |
| `int_inventory_daily_changes.sql` | Intermediate | 10 CTEs, multi-source aggregation, composite key joins, COALESCE patterns |
| `fact_fulfillment.sql` | Gold/Fact | 6-layer CTE, 13-table join, correlated subqueries, business day logic, deadline metrics |
| `macros/hash_surrogate_key.sql` | Macro | Jinja2 templating, MD5 hashing, edge case handling |
| `macros/std_cast.sql` | Macro | Type-aware casting, null-like value detection, TRY_CAST patterns |
| `staging_orders.yml` | Schema | Model documentation, data quality tests, severity configuration |
