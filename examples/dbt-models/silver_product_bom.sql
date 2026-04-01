{{
  config(
    materialized='incremental',
    unique_key='product_bom_sk',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    post_hook="{{ create_indexes('product_bom_sk', ['combo_sku_code', '_loaded_at']) }}"
  )
}}

-- =====================================================
-- Silver Model: Product BOM (Bill of Materials) — Latest Only
-- =====================================================
-- Purpose: Latest BOM version for combo/set products
-- Source: stg_inventory_sku_set (cleaned from staging layer)
-- Strategy: Keep LATEST version only (no history)
-- Unique Key: combo_sku_code (single column)
-- =====================================================

with source as (
    select *
    from {{ ref('stg_inventory_sku_set') }}

    -- Macro: incremental filter based on _loaded_at
    {{ build_incremental_filter() }}
),

-- Step 1: Generate Surrogate Key & Add Metadata
transformed as (
    select
        -- === Metadata (Bronze) ===
        _source_file_path,
        _file_sha256,
        _batch_id,
        _imported_by,
        _imported_at,
        _loaded_at,
        _row_hash,

        -- === Surrogate Key (MD5 hash of combo_sku_code) ===
        {{ hash_surrogate_key(['combo_sku_code'], 'product_bom_sk') }},

        -- === Business Columns (Already Typed from Staging) ===
        combo_style_code,
        combo_sku_code,
        combo_product_name,
        sku_code,
        quantity,
        product_short_name,
        brand,
        category,
        product_type,
        unit,
        weight_kg,
        sale_price,
        purchase_price,
        barcode,
        note,
        is_active,
        creator,
        create_time,
        modifier,
        modified_time,

        -- === Data Quality Flag ===
        case
            when combo_sku_code is null then cast(0 as bit)
            when sku_code is null then cast(0 as bit)
            else cast(1 as bit)
        end as is_valid_record,

        -- === Silver Metadata ===
        current_timestamp as _dbt_processed_at

    from source
),

-- Step 2: Deduplication — Keep LATEST version only
deduplicated as (
    select
        *,
        row_number() over (
            partition by product_bom_sk
            order by modified_time desc
        ) as row_num
    from transformed
)

-- Final: Return Only Latest BOM Version
select
    product_bom_sk,
    combo_style_code,
    combo_sku_code,
    combo_product_name,
    sku_code,
    quantity,
    product_short_name,
    brand,
    category,
    product_type,
    unit,
    weight_kg,
    sale_price,
    purchase_price,
    barcode,
    note,
    is_active,
    creator,
    create_time,
    modifier,
    modified_time,
    is_valid_record,
    _source_file_path,
    _file_sha256,
    _batch_id,
    _imported_by,
    _imported_at,
    _loaded_at,
    _row_hash,
    _dbt_processed_at

from deduplicated
where row_num = 1
