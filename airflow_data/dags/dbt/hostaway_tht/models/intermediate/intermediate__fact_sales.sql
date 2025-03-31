{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert'
) }}

select
    id,
    product_id,
    retailer_id,
    channel,
    location,
    price,
    quantity,
    price*quantity as total_amount,
    sale_date
from {{ ref('staging__sales') }}

{% if is_incremental() %}
where sale_date >= coalesce(
    (
        select
           max(sale_date)
        from
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}
