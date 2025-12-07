import json
import logging
import time
import sys
from datetime import datetime
from cassandra.cluster import Cluster
from confluent_kafka import Consumer, KafkaError

# ==========================================
# 1. CONFIGURATION (DOCKER)
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', stream=sys.stdout)
logger = logging.getLogger("sink-service")

KAFKA_BROKER = 'broker:29092'       
CASSANDRA_HOST = ['cassandra']      

TOPIC_ITEMS = "processed_sales_items"
TOPIC_ORDERS = "processed_orders_agg"

# ==========================================
# 2. CONNECT TO CASSANDRA
# ==========================================
session = None
while not session:
    try:
        logger.info(f"Attempting connection to Cassandra at {CASSANDRA_HOST}...")
        cluster = Cluster(CASSANDRA_HOST)
        session = cluster.connect()
        logger.info("✅ Connected to Cassandra!")
    except Exception as e:
        logger.warning(f"⏳ Cassandra not ready. Retrying in 5 seconds... Error: {e}")
        time.sleep(5)

# ==========================================
# 3. PREPARE SQL STATEMENTS 
# ==========================================
logger.info("Preparing CQL Statements...")

# STREAM A: ITEMS
# Added: customerkey, payment_method
insert_item_stmt = session.prepare("""
    INSERT INTO sales_realtime.sales_items_realtime 
    (store_country, store_city, product_name, category, event_ts, order_serial, 
     quantity, total_sales_usd, line_total_amount, is_return, is_cancel, 
     processing_lag, customerkey, payment_method, sales_channel)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# STREAM B: ORDERS
insert_order_stmt = session.prepare("""
    INSERT INTO sales_realtime.orders_financial_realtime 
    (currency_code, event_ts, order_group_id, total_items_count, 
     total_order_local, total_order_usd, is_return_flag, is_cancel_flag, 
     max_lag_seconds, order_date, customerkey, payment_method , store_country)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# ==========================================
# 4. HELPER: TIMESTAMP PARSER
# ==========================================
def parse_ts(ts_string):
    if not ts_string: return None
    try:
        if ts_string.endswith('Z'):
            ts_string = ts_string[:-1] + '+00:00'
        return datetime.fromisoformat(ts_string)
    except ValueError:
        return datetime.now()

# ==========================================
# 5. CONNECT TO KAFKA
# ==========================================
logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'cassandra_sink_group_docker_v2_fraud', # New Group ID for V2
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC_ITEMS, TOPIC_ORDERS])
logger.info(f"✅ Subscribed to topics: {TOPIC_ITEMS}, {TOPIC_ORDERS}")

# ==========================================
# 6. PROCESSING LOGIC
# ==========================================
def process_msg(msg):
    try:
        topic = msg.topic()
        val = msg.value()
        if val is None: return

        data = json.loads(val)
        
        if topic == TOPIC_ITEMS:
            session.execute_async(insert_item_stmt, [
                data.get('store_country'),
                data.get('store_city'),
                data.get('product_name'),
                data.get('category'),
                parse_ts(data.get('event_ts')), 
                data.get('order_serial'),
                data.get('quantity'),
                data.get('total_sales_usd'),
                data.get('line_total_amount'),
                data.get('is_return'),
                data.get('is_cancel'),
                data.get('processing_lag', 0),
                data.get('customerkey'),
                data.get('payment_method'),
                data.get('sales_channel')                
            ])
            
        elif topic == TOPIC_ORDERS:
            session.execute_async(insert_order_stmt, [
                data.get('currency_code'),
                parse_ts(data.get('last_update')), 
                data.get('order_group_id'),
                data.get('total_items_count'),
                data.get('total_order_local'),
                data.get('total_order_usd'),
                data.get('is_return_flag'),
                data.get('is_cancel_flag'),
                data.get('max_lag_seconds', 0),
                data.get('order_date'),
                data.get('customerkey'),
                data.get('payment_method'),
                data.get('store_country')
            ])
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# ==========================================
# 7. MAIN LOOP
# ==========================================
try:
    logger.info("🚀 Sink Service V2 Started. Waiting for messages...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            logger.error(f"Kafka Error: {msg.error()}")
            continue
        process_msg(msg)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cluster.shutdown()