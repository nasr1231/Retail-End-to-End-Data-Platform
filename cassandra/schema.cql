-- create this file in your main directory then these 2 commands in powershell
-- Get-Content schema.cql | docker exec -i cassandra cqlsh
-- docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE sales_realtime;"

-- 1. Create Keyspace
CREATE KEYSPACE IF NOT EXISTS sales_realtime 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 2. Create Items Table (Stream A)
CREATE TABLE IF NOT EXISTS sales_realtime.sales_items_realtime (
    store_country text,
    store_city text,
    product_name text,
    category text,
    event_ts timestamp,
    order_serial text,
    quantity int,
    total_sales_usd double,
    line_total_amount double,
    customerkey bigint,
    payment_method text,
    is_return int,
    is_cancel int,
    sales_channel text,
    processing_lag int,
    PRIMARY KEY ((store_country), event_ts, order_serial)
) WITH CLUSTERING ORDER BY (event_ts DESC);

-- 3. Create Orders Table (Stream B)
CREATE TABLE IF NOT EXISTS sales_realtime.orders_financial_realtime (
    currency_code text,
    order_date text,
    event_ts timestamp,
    order_group_id text,
    customerkey bigint,
    store_country text,
    payment_method text,
    total_items_count bigint,
    total_order_local double,
    total_order_usd double,
    is_return_flag int,
    is_cancel_flag int,
    max_lag_seconds int,
    PRIMARY KEY ((store_country), event_ts, order_group_id)
) WITH CLUSTERING ORDER BY (event_ts DESC);


-- 4. Create index on store_city and sales_channel columns in sales_items_realtime table
CREATE INDEX IF NOT EXISTS idx_store_city ON sales_realtime.sales_items_realtime (store_city);

CREATE INDEX IF NOT EXISTS channel_idx ON sales_realtime.sales_items_realtime (sales_channel);
-- CREATE INDEX IF NOT EXISTS order_country_idx ON sales_realtime.orders_financial_realtime (store_country);

-- how to delete onld schema.cql from cassandra container: 
-- docker exec -it cassandra cqlsh -e "DROP KEYSPACE sales_realtime;"
-- then run the 2 commands at the top again to recreate it
-- to check data in the tables use:
-- docker exec -it cassandra cqlsh -e "SELECT * FROM sales_realtime.sales_items_realtime LIMIT 10;"
-- docker exec -it cassandra cqlsh -e "SELECT * FROM sales_realtime.orders_financial_realtime LIMIT 10;"