{%- macro std_cast(column_name, target_type) -%}
    {#
        Standardize NULL + Type Cast in one step.

        - Numeric: REPLACE comma + TRY_CAST (null-like values handled automatically)
        - Date/Time: TRY_CAST (invalid values become NULL)
        - String: TRIM + null-like value check + CAST

        Usage:
            {{ std_cast('[unit_price]', 'decimal(18,2)') }} as unit_price
            {{ std_cast('[order_time]', 'datetime') }} as order_time
            {{ std_cast('[product_name]', 'nvarchar(500)') }} as product_name
    #}
    {%- set t = target_type.lower() -%}

    {%- if t.startswith('decimal') or t.startswith('numeric') or t.startswith('int')
        or t.startswith('bigint') or t.startswith('smallint') or t.startswith('tinyint')
        or t.startswith('float') or t.startswith('real') or t.startswith('money') -%}
        {# Numeric: TRY_CAST handles '-', 'null', 'N/A' as NULL automatically #}
        try_cast(REPLACE({{ column_name }}, ',', '') as {{ target_type }})

    {%- elif t.startswith('date') or t.startswith('time') -%}
        {# Date/Time: TRY_CAST handles invalid values as NULL #}
        try_cast({{ column_name }} as {{ target_type }})

    {%- elif t.startswith('nvarchar') or t.startswith('varchar') -%}
        {# String: TRIM + null-like value check #}
        {%- set cleaned = "trim(cast(" ~ column_name ~ " as " ~ target_type ~ "))" -%}
        case
            when {{ column_name }} is null then null
            when {{ cleaned }} = '' then null
            when {{ cleaned }} in ('-', 'null', 'NULL', 'N/A', 'n/a', '#N/A', 'NA', 'None', 'none', 'nil', 'NIL') then null
            else {{ cleaned }}
        end

    {%- else -%}
        {# Fallback #}
        try_cast({{ column_name }} as {{ target_type }})
    {%- endif -%}
{%- endmacro %}
