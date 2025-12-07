{{ config(materialized='table', schema='gold') }}

WITH Sales_cte AS (
    SELECT 		
        s.order_number,
        COUNT(*) OVER (PARTITION BY s.order_number) as line_item_count,
        s.product_key , 
        s.CUSTOMER_KEY,
        s.order_date,
        s.delivery_date,
        st.Store_sk,
        p.unit_cost_usd,
        p.unit_price_usd,
        s.quantity as p_quantity,
        (P.UNIT_COST_USD * s.quantity) AS total_cost_usd,
        (P.UNIT_PRICE_USD * s.quantity) AS total_price_usd,
        ((P.UNIT_COST_USD * s.quantity)*ex.rate) as total_cost_local,
        ((P.UNIT_PRICE_USD * s.quantity)*ex.rate) as total_price_local,
        s.currency_code,
        ((P.UNIT_PRICE_USD ) - (P.UNIT_COST_USD ))/p.unit_price_usd as profit_margin
     FROM {{ ref('silver_sales') }} as S
    JOIN {{ ref('dim_product_info') }} AS P
        ON S.PRODUCT_KEY = P.PRODUCT_KEY
    JOIN {{ ref('dim_stores') }} AS st
        ON S.STORE_KEY = st.STORE_KEY
    JOIN {{ ref('dim_exchange_rates') }} as ex
        ON S.CURRENCY_CODE = ex.currency_code AND s.order_date = ex.date_key
    GROUP BY 
        s.order_number, 
        s.CUSTOMER_KEY, 
        s.order_date, 
        s.delivery_date, 
        st.Store_sk,
        p.unit_cost_usd,
        p.unit_price_usd,
        s.currency_code,
        s.quantity,
        ex.rate,
        s.product_key
) 
	
SELECT
    ROW_NUMBER() OVER (ORDER BY S.order_number ) AS product_sale_sk,
    S.order_number as order_number_bk,
    S.line_item_count as total_line_items,
    MAX(od.date_sk) as order_date_fk,
    MAX(dd.date_sk) as delivery_date_fk,
    S.Store_sk as Store_fk,
    c.CUSTOMER_KEY as customer_fk,
    pin.product_sk as product_fk,
    exr.exchange_sk as exchange_fk,
    MAX(S.p_quantity) as p_quantity,
    MAX(S.total_cost_usd) as total_cost_usd,
    MAX(S.total_price_usd) as total_price_usd,
    MAX(S.total_price_usd) - MAX(S.total_cost_usd) AS total_profit_usd,
    MAX(S.total_cost_local) as total_cost_local,
    MAX(S.total_price_local) as total_price_local,
    MAX(S.total_price_local) - MAX(S.total_cost_local) AS total_profit_local,
    MAX(S.profit_margin) as profit_margin,
    MAX(s.currency_code) as local_currency
    
FROM Sales_cte AS S
JOIN {{ ref('dim_customer') }} AS c
    ON S.CUSTOMER_KEY = c.CUSTOMER_KEY
JOIN {{ ref('dim_date') }} as od	
     ON S.order_date = od.Date_key
LEFT JOIN {{ ref('dim_date') }} AS dd	
     ON S.delivery_date = dd.Date_key
LEFT JOIN {{ ref('dim_product_info') }} as pin
     ON S.product_key = pin.product_key
LEFT JOIN {{ ref('dim_exchange_rates') }} as exr
     ON S.CURRENCY_CODE = exr.currency_code AND S.order_date = exr.date_key
GROUP BY 
    S.order_number,
    S.line_item_count,
    S.Store_sk,
    c.CUSTOMER_KEY,
    pin.product_sk,
    exr.exchange_sk
