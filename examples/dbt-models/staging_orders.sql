-- =====================================================
-- Staging Model: stg_platform_order
-- =====================================================
-- Purpose: Clean and type-cast raw order data (1:1 with Bronze)
-- Source: bronze.platform_order
-- Layer: Staging (cleaning only, no business logic)
-- =====================================================

with source as (
    select *
    from {{ source('bronze', 'platform_order') }}
),

final as (
    select
        -- === Metadata (Bronze) ===
        _source_file_path,
        _file_sha256,
        _batch_id,
        _imported_by,
        _imported_at,
        _loaded_at,
        _row_hash,

        -- === Key Columns (macro-based type casting) ===
        {{ std_cast('[internal_order_number]', 'nvarchar(100)') }} as internal_order_number,
        {{ std_cast('[product_code]', 'nvarchar(100)') }} as product_code,
        {{ std_cast('[unit_price]', 'decimal(18,2)') }} as unit_price,
        {{ std_cast('[regular_price]', 'decimal(18,2)') }} as regular_price,
        {{ std_cast('[platform_discount_item]', 'decimal(18,2)') }} as platform_discount_item,
        {{ std_cast('[shop_discount_item]', 'decimal(18,2)') }} as shop_discount_item,
        {{ std_cast('[net_amount]', 'decimal(18,2)') }} as net_amount,
        {{ std_cast('[platform_sku]', 'nvarchar(100)') }} as platform_sku,

        -- === Order Details ===
        {{ std_cast('[amount_payable]', 'decimal(18,2)') }} as amount_payable,
        {{ std_cast('[amount_paid]', 'decimal(18,2)') }} as amount_paid,
        {{ std_cast('[shipping_fee]', 'decimal(18,2)') }} as shipping_fee,
        {{ std_cast('[total_quantity]', 'int') }} as total_quantity,
        {{ std_cast('[order_status]', 'nvarchar(100)') }} as order_status,
        {{ std_cast('[shipping_company]', 'nvarchar(200)') }} as shipping_company,
        {{ std_cast('[tracking_number]', 'nvarchar(200)') }} as tracking_number,

        -- === Timestamps ===
        {{ std_cast('[order_time]', 'datetime') }} as order_time,
        {{ std_cast('[payment_time]', 'datetime') }} as payment_time,
        {{ std_cast('[shipping_date]', 'date') }} as shipping_date,
        {{ std_cast('[pickup_deadline]', 'datetime') }} as pickup_deadline,

        -- === Customer & Address ===
        {{ std_cast('[store]', 'nvarchar(200)') }} as store,
        {{ std_cast('[platform]', 'nvarchar(100)') }} as platform,
        {{ std_cast('[buyer_name]', 'nvarchar(200)') }} as buyer_name,
        {{ std_cast('[province]', 'nvarchar(100)') }} as province,
        {{ std_cast('[postal_code]', 'nvarchar(20)') }} as postal_code,
        {{ std_cast('[outbound_warehouse]', 'nvarchar(200)') }} as outbound_warehouse,

        -- === Financial ===
        {{ std_cast('[platform_discount]', 'decimal(18,2)') }} as platform_discount,
        {{ std_cast('[shop_discount]', 'decimal(18,2)') }} as shop_discount,
        {{ std_cast('[cod_amount]', 'decimal(18,2)') }} as cod_amount,
        {{ std_cast('[outstanding_amount]', 'decimal(18,2)') }} as outstanding_amount,
        {{ std_cast('[tax_amount]', 'decimal(18,2)') }} as tax_amount,
        {{ std_cast('[refund_amount]', 'decimal(18,2)') }} as refund_amount,

        -- === Product ===
        {{ std_cast('[product_name]', 'nvarchar(500)') }} as product_name,
        {{ std_cast('[quantity_item]', 'int') }} as quantity_item,

        -- === Cast Failure Tracking ===
        -- Counts how many rows failed type casting (data quality monitoring)
        {{ cast_failure_summary([
            {'raw': '[unit_price]', 'type': 'decimal(18,2)', 'name': 'unit_price'},
            {'raw': '[regular_price]', 'type': 'decimal(18,2)', 'name': 'regular_price'},
            {'raw': '[platform_discount_item]', 'type': 'decimal(18,2)', 'name': 'platform_discount_item'},
            {'raw': '[total_quantity]', 'type': 'int', 'name': 'total_quantity'},
            {'raw': '[order_time]', 'type': 'datetime', 'name': 'order_time'},
            {'raw': '[payment_time]', 'type': 'datetime', 'name': 'payment_time'},
            {'raw': '[shipping_fee]', 'type': 'decimal(18,2)', 'name': 'shipping_fee'},
            {'raw': '[tax_amount]', 'type': 'decimal(18,2)', 'name': 'tax_amount'},
            {'raw': '[refund_amount]', 'type': 'decimal(18,2)', 'name': 'refund_amount'},
            {'raw': '[quantity_item]', 'type': 'int', 'name': 'quantity_item'},
        ]) }}

    from source
)

select * from final
