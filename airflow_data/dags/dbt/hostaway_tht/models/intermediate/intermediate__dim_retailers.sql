{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert'
) }}

with inc_retailers as (
    select distinct retailer_id, retailer_name
    from {{ ref('staging__sales') }}
)
select
    retailer_id as id,
    retailer_name
from inc_retailers