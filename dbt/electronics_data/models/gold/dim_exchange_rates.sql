{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY rate_date, currency) AS exchange_sk,
    currency AS currency_code,
    currency AS currency_name,
    TO_DATE(rate_date, 'DD-MM-YY') AS date_key,
    exchange_rate_usd AS rate
FROM {{ ref('silver_exchange_rates') }}

