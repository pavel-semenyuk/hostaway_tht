{{ config(
    materialized = 'incremental',
    unique_key = 'sale_date',
    incremental_strategy = 'delete+insert'
) }}

select
    sale_date,
    sum(total_amount) as total_amount,
    count(id) as count_sales,
    round(avg(total_amount), 2) as average_sale
from
    {{ ref('intermediate__fact_sales') }}

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

group by 1