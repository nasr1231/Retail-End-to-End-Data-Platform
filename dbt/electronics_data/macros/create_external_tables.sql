{% macro create_external_tables() %}

  {{ log("Creating ALL external tables from S3", info=True) }}

  {% set create_sql %}

------------------------------------------------------------------------------------
-- 1) FILE FORMAT FOR NDJSON (Newline Delimited JSON)
------------------------------------------------------------------------------------

CREATE FILE FORMAT IF NOT EXISTS json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = 'AUTO';

------------------------------------------------------------------------------------
-- 2) STAGES
------------------------------------------------------------------------------------

CREATE OR REPLACE STAGE customers_stage
  URL='s3://grad-s3-batch-bucket/customers/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

CREATE OR REPLACE STAGE data_dictionary_stage
  URL='s3://grad-s3-batch-bucket/data_dictionary/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

CREATE OR REPLACE STAGE exchange_rates_stage
  URL='s3://grad-s3-batch-bucket/exchange_rates/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

CREATE OR REPLACE STAGE sales_stage
  URL='s3://grad-s3-batch-bucket/sales/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

CREATE OR REPLACE STAGE stores_stage
  URL='s3://grad-s3-batch-bucket/stores/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

CREATE OR REPLACE STAGE products_stage
  URL='s3://grad-s3-batch-bucket/products/'
  CREDENTIALS = (
    AWS_KEY_ID='{{ env_var("AWS_KEY_ID") }}'
    AWS_SECRET_KEY='{{ env_var("AWS_SECRET_KEY") }}'
  )
  FILE_FORMAT = json_format;

------------------------------------------------------------------------------------
-- 3) EXTERNAL TABLES
------------------------------------------------------------------------------------

-- CUSTOMERS
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.customers_ext
(
  customerkey BIGINT        AS (value:customerkey::BIGINT),
  gender VARCHAR(20)        AS (value:gender::STRING),
  name VARCHAR(255)         AS (value:name::STRING),
  city VARCHAR(255)         AS (value:city::STRING),
  state_code VARCHAR(100)   AS (value:state_code::STRING),
  state VARCHAR(255)        AS (value:state::STRING),
  zip_code VARCHAR(20)      AS (value:zip_code::STRING),
  country VARCHAR(255)      AS (value:country::STRING),
  continent VARCHAR(50)     AS (value:continent::STRING),
  birthday DATE             AS (TO_DATE(TO_TIMESTAMP_NTZ(TO_NUMBER(value:birthday) / 1000)))
)
LOCATION=@customers_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

-- DATA DICTIONARY
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.data_dictionary_ext
(
  id BIGINT                   AS (value:id::BIGINT),
  table_name VARCHAR(255)     AS (value:table_name::STRING),
  field_name VARCHAR(255)     AS (value:field_name::STRING),
  description TEXT            AS (value:description::STRING)
)
LOCATION=@data_dictionary_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

-- EXCHANGE RATES
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.exchange_rates_ext
(
  id BIGINT                     AS (value:id::BIGINT),
  rate_date VARCHAR(30)         AS (value:rate_date::STRING),
  currency VARCHAR(10)          AS (value:currency::STRING),
  exchange NUMERIC(18,6)        AS (value:exchange::NUMBER(18,6))
)
LOCATION=@exchange_rates_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

-- SALES
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.sales_ext
(
  order_number INT            AS (value:order_number::INT),
  line_item INT               AS (value:line_item::INT),
  order_date DATE             AS (value:order_date::DATE),
  delivery_date DATE          AS (value:delivery_date::DATE),
  customerkey INT             AS (value:customerkey::INT),
  storekey INT                AS (value:storekey::INT),
  productkey INT              AS (value:productkey::INT),
  quantity INT                AS (value:quantity::INT),
  currency_code VARCHAR(10)   AS (value:currency_code::STRING)
)
LOCATION=@sales_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

-- STORES
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.stores_ext
(
  storekey INT                AS (value:storekey::INT),
  country VARCHAR(100)        AS (value:country::STRING),
  state VARCHAR(100)          AS (value:state::STRING),
  square_meters NUMBER(18,2)  AS (value:square_meters::NUMBER(18,2)),
  open_date DATE              AS (value:open_date::DATE)
)
LOCATION=@stores_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

-- PRODUCTS
CREATE OR REPLACE EXTERNAL TABLE DATAWAREHOUSE_TEST.TEST_SCH.products_ext
(
  productkey INT              AS (value:productkey::INT),
  product_name VARCHAR(255)   AS (value:product_name::STRING),
  brand VARCHAR(255)          AS (value:brand::STRING),
  color VARCHAR(100)          AS (value:color::STRING),
  unit_cost_usd VARCHAR(15)   AS (value:Unit_Cost_USD::STRING),
  unit_price_usd VARCHAR(15)  AS (value:Unit_Price_USD::STRING),
  subcategorykey INT          AS (value:subcategorykey::INT),
  subcategory VARCHAR(255)    AS (value:subcategory::STRING),
  categorykey INT             AS (value:categorykey::INT),
  category VARCHAR(255)       AS (value:category::STRING)
)
LOCATION=@products_stage
FILE_FORMAT=json_format
AUTO_REFRESH = FALSE;

  {% endset %}

  {% do run_query(create_sql) %}

{% endmacro %}

