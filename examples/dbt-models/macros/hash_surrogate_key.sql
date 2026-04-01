{% macro hash_surrogate_key(key_columns, sk_name) %}
    {#
        Generate surrogate key using MD5 hash.

        Args:
            key_columns: List of column names to include in key
            sk_name: Name of the surrogate key column

        Example:
            {{ hash_surrogate_key(['order_id', 'sku'], 'order_item_sk') }}

        Output:
            convert(varchar(32), hashbytes('MD5',
                concat(
                    coalesce(cast(order_id as nvarchar(500)), ''), '|',
                    coalesce(cast(sku as nvarchar(500)), '')
                )
            ), 2) as order_item_sk
    #}
    {%- if key_columns|length == 0 %}
        {# Empty: Use CHECKSUM of all columns #}
        convert(varchar(32), hashbytes('MD5',
            cast(checksum(*) as nvarchar(40))
        ), 2) as {{ sk_name }}
    {%- elif key_columns|length == 1 %}
        {# Single column #}
        convert(varchar(32), hashbytes('MD5',
            concat(
                coalesce(cast({{ key_columns[0] }} as nvarchar(500)), ''),
                ''
            )
        ), 2) as {{ sk_name }}
    {%- else %}
        {# Multiple columns: CONCAT with pipe separator #}
        convert(varchar(32), hashbytes('MD5',
            concat(
                {%- for col in key_columns %}
                coalesce(cast({{ col }} as nvarchar(500)), '')
                {%- if not loop.last %}, '|', {% endif %}
                {%- endfor %}
            )
        ), 2) as {{ sk_name }}
    {%- endif %}
{%- endmacro %}
