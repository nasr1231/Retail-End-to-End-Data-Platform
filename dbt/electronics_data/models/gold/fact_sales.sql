{{ config(materialized='table', schema='gold') }}

WITH Sales_cte AS (
    SELECT 		
        s.order_number,
        MAX(s.line_item) AS max_line_item_count,
        s.CUSTOMER_KEY,
        s.order_date,
        st.Store_sk,
        s.delivery_date,
        
        -- Calculate the Order Totals here
        SUM(s.quantity) AS order_total_quantity,
        SUM(p.UNIT_COST_USD * s.quantity) AS order_total_cost_usd,
        SUM(p.UNIT_PRICE_USD * s.quantity) AS order_total_price_usd,
        (SUM(p.UNIT_COST_USD * s.quantity) * ex.rate) AS order_total_cost_local,
        (SUM(p.UNIT_PRICE_USD * s.quantity) * ex.rate) AS order_total_price_local,
        s.currency_code
    FROM {{ ref('silver_sales') }} AS S
    JOIN {{ ref('dim_product_info') }} AS p
        ON S.PRODUCT_KEY = p.PRODUCT_KEY
    JOIN {{ ref('dim_stores') }} AS st
        ON S.STORE_KEY = st.STORE_KEY
    LEFT JOIN {{ ref('dim_exchange_rates') }} AS ex
        ON S.CURRENCY_CODE = ex.currency_code 
       AND S.order_date = ex.date_key
    GROUP BY 
        s.order_number, 
        s.CUSTOMER_KEY, 
        s.order_date, 
        s.delivery_date, 
        st.Store_sk,
        s.currency_code,
        ex.rate
)

SELECT
    ROW_NUMBER() OVER (ORDER BY S.order_number) AS order_sk,
    S.order_number AS order_number_bk,
    MAX(S.max_line_item_count) AS number_of_items,
    MAX(od.date_sk) AS order_date_fk,
    MAX(dd.date_sk) AS delivery_date_fk,
    S.Store_sk AS Store_fk,
    c.CUSTOMER_KEY AS customer_fk,
    exr.exchange_sk AS exchange_rate_fk,
    MAX(S.order_total_quantity) AS total_quantity,

    MAX(S.order_total_cost_usd) AS total_cost_usd,
    MAX(S.order_total_price_usd) AS total_price_usd,
    MAX(S.order_total_price_usd) - MAX(S.order_total_cost_usd) AS total_profit_usd,

    MAX(S.order_total_cost_local) AS total_cost_local,
    MAX(S.order_total_price_local) AS total_price_local,
    MAX(S.order_total_price_local) - MAX(S.order_total_cost_local) AS total_profit_local,

    S.currency_code
FROM Sales_cte AS S
JOIN {{ ref('dim_customer') }} AS c
    ON S.CUSTOMER_KEY = c.CUSTOMER_KEY
JOIN {{ ref('dim_date') }} AS od
    ON S.order_date = od.Date_key
LEFT JOIN {{ ref('dim_date') }} AS dd
    ON S.delivery_date = dd.Date_key
LEFT JOIN {{ ref('dim_exchange_rates') }} AS exr
    ON S.CURRENCY_CODE = exr.currency_code 
   AND S.order_date = exr.date_key
GROUP BY 
    S.order_number,
    S.Store_sk,
    c.CUSTOMER_KEY,
    exr.exchange_sk,
    S.currency_code

