{{ config(materialized='table', schema='silver') }}

SELECT
  order_number,
  line_item,
  TO_DATE(order_date) AS order_date,
  TO_DATE(delivery_date) AS delivery_date,
  customerkey AS customer_key,
  storekey AS store_key,
  productkey AS product_key,
  quantity,
  currency_code
FROM {{ source('s3_sources', 'sales_ext') }}
