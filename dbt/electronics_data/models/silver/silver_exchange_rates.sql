{{ config(materialized='table', schema='silver') }}

SELECT
TO_CHAR(TO_DATE(rate_date, 'MM/DD/YYYY'), 'DD-MM-YY') AS rate_date,
  UPPER(currency) AS currency,
  CAST(Exchange AS FLOAT) AS exchange_rate_usd
FROM {{ source('s3_sources', 'exchange_rates_ext') }}
