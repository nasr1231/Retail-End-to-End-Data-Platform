{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY product_key) AS product_sk,
    product_key,
    product_name,
    brand,
    unit_cost AS unit_cost_usd,
    unit_price AS unit_price_usd,
    subcategory_key,
    subcategory,
    category_key,
    category
FROM {{ ref('silver_products') }}

