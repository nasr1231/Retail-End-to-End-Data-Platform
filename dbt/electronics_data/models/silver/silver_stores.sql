{{ config(materialized='table', schema='silver') }}

SELECT
  storekey AS store_key,
  Country AS country,
  State AS state,
  square_meters,
  to_varchar(open_date, 'DD-MM-YY') as open_date,
FROM {{ source('s3_sources', 'stores_ext') }}
