{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert'
) }}

with inc_products as (
    select distinct product_id, product_name, brand, category
    from {{ ref('staging__sales') }}
)
select
    product_id as id,
    product_name,
    brand,
    category
from inc_products