{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_key) AS customer_sk,
    customer_key,
    name,
    gender,
    city,
    state,
    country,
    continent,
    birthday
FROM {{ ref('silver_customers') }}

