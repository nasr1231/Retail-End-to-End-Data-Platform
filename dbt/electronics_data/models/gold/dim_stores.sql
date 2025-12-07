{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY store_key) AS store_sk,
    store_key,
    state,
    country,
    square_meters,
    open_date
FROM {{ ref('silver_stores') }}

