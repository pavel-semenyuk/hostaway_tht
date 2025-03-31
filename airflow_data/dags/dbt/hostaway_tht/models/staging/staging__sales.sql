{{ config(
    materialized = 'view'
) }}

select
    "SaleID" as id,
    "ProductID" as product_id,
    "ProductName" as product_name,
    "Brand" as brand,
    "Category" as category,
    "RetailerID" as retailer_id,
    "RetailerName" as retailer_name,
    "Channel" as channel,
    "Location" as location,
    "Quantity" as quantity,
    "Price" as price,
    "Date" as sale_date
from
    {{ source('raw', 'sales') }}