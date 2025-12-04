{{ config(materialized='table', schema='silver') }}

SELECT
 customerkey AS customer_key,
  UPPER(Gender) AS gender,
  Name AS name,
  City AS city,
  state_code,
  State AS state,
  zip_code,
  Country AS country,
  UPPER(Continent) AS continent,
  to_date(birthday) as birthday,
FROM {{ source('s3_sources', 'customers_ext') }}
