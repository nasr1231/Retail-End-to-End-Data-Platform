{{ config(materialized='table', schema='silver') }}

SELECT
    productkey AS product_key,
    product_name,
    Brand AS brand,
    UPPER(Color) AS color,

    -- Clean unit cost
    REPLACE(REPLACE(TRIM(unit_cost_usd), '$', ''), ',', '') :: numeric(15,2) AS unit_cost,

    -- Clean unit price
    REPLACE(REPLACE(TRIM(unit_price_usd), '$', ''), ',', '') :: numeric(15,2) AS unit_price,

    -- Expensive flag
    CASE WHEN 
        (REPLACE(REPLACE(TRIM(unit_price_usd), '$', ''), ',', '') :: numeric(15,2)) > 100
    THEN 'Yes' ELSE 'No' END AS expensive_flag,

    SubcategoryKey AS subcategory_key,
    Subcategory AS subcategory,
    CategoryKey AS category_key,
    Category AS category

FROM {{ source('s3_sources', 'products_ext') }}
