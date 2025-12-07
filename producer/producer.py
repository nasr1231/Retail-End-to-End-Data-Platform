# producer.py
from __future__ import annotations

import os
import random
import time
import logging
import re
from datetime import datetime, timedelta
from datetime import date

from multiprocessing import Process
import psycopg2
import redis
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
import threading
import requests
import time as _time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from config import (
    KAFKA_TOPIC,
    NUM_WORKERS,
    Country,
    COUNTRY_WEIGHTS,
    COUNTRY_CURRENCY,
    ONLINE_ORDER_PROBABILITY,
    CANCEL_PROBABILITY,
    RETURN_PROBABILITY,
)
from schema_registry import avro_serializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("sales-producer")

# ==================== CONNECTIONS ====================
redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "user": "airflow",
    "password": "airflow",
    "dbname": "data_source",
}

CURRENT_RATES_DATE = None
CURRENT_RATES = {
    'USD': 1.0,
    'EUR': 0.9512,
    'GBP': 0.8234,
    'CAD': 1.3871,
    'AUD': 1.5345
}

def update_exchange_rates():
    global CURRENT_RATES_DATE, CURRENT_RATES
    today = date.today()
        
    if CURRENT_RATES_DATE == today:
        return
    
    logger.info("Today's price list %s", today)
    try:
        resp = requests.get("https://api.exchangerate.host/latest?base=USD", timeout=8)
        data = resp.json()
        if data.get('success'):
            r = data['rates']
            CURRENT_RATES = {
                'USD': 1.0,
                'EUR': r.get('EUR', 0.95),
                'GBP': r.get('GBP', 0.82),
                'CAD': r.get('CAD', 1.38),
                'AUD': r.get('AUD', 1.53)
            }
            CURRENT_RATES_DATE = today
            logger.info("تم تحديث أسعار الصرف → EUR=%.4f | GBP=%.4f | CAD=%.4f | AUD=%.4f",
                        CURRENT_RATES['EUR'], CURRENT_RATES['GBP'], CURRENT_RATES['CAD'], CURRENT_RATES['AUD'])
        else:
            logger.warning("API رجع فشل، هنستخدم آخر قيم")
    except Exception as e:
        logger.error("فشل جلب سعر الصرف: %s", e)

# Fallback local order counter when Redis is unavailable
_local_order_counter = 0
_redis_available = False
_last_redis_warning = 0.0
_redis_warning_interval = 60.0  # seconds between repeated warnings


def _monitor_redis_availability(poll_interval: float = 5.0) -> None:
    """Background thread that monitors Redis availability and attempts to sync local counter when Redis returns.

    This will set `_redis_available` to True when Redis responds to `ping()` and will, if possible,
    ensure Redis' `sales_last_order_number` is at least the local counter to avoid overlapping order numbers.
    """
    global _redis_available, _local_order_counter
    while True:
        try:
            # ping is cheap; will raise on DNS resolution failure
            if redis_client.ping():
                if not _redis_available:
                    logger.info("Redis is now available. Syncing local counter if needed.")
                _redis_available = True

                # Attempt to sync local counter into Redis if our local counter is ahead
                try:
                    remote_val = redis_client.get("sales_last_order_number")
                    remote_val_int = int(remote_val) if remote_val is not None else 0
                    if _local_order_counter > remote_val_int:
                        # bump redis to local counter by setting to local counter
                        redis_client.set("sales_last_order_number", _local_order_counter)
                        logger.info("Synchronized Redis counter to local value %d", _local_order_counter)
                except Exception:
                    # If syncing fails, ignore and keep trying
                    logger.debug("Failed to sync local counter to Redis; will retry")
        except Exception:
            if _redis_available:
                logger.warning("Redis became unavailable; falling back to local counter")
            _redis_available = False
        _time.sleep(poll_interval)


# Start background monitor thread (daemon so it doesn't prevent shutdown)
try:
    t = threading.Thread(target=_monitor_redis_availability, args=(), daemon=True)
    t.start()
except Exception:
    logger.debug("Failed to start Redis monitor thread; continuing without background monitor")


def get_next_order_number() -> int:
    """Try to get the next order number from Redis; fall back to a local counter on failure.

    This prevents the producer from crashing if Redis is not available. Note that the
    local counter is process-local (not shared across workers) and only used as a fallback.
    """
    global _local_order_counter
    global _last_redis_warning, _redis_available
    # If redis is currently marked available, prefer it
    if _redis_available:
        try:
            return int(redis_client.incr("sales_last_order_number"))
        except Exception:
            # if an unexpected error occurs, mark redis unavailable and fall through to local
            _redis_available = False

    # Fallback local counter
    _local_order_counter += 1
    now = _time.time()
    if now - _last_redis_warning > _redis_warning_interval:
        logger.warning("Redis unavailable. Falling back to local order counter: %d", _local_order_counter)
        _last_redis_warning = now
    return _local_order_counter

# ==================== FAKER ====================
FAKERS = {
    Country.USA: Faker("en_US"),
    Country.CANADA: Faker("en_CA"),
    Country.UK: Faker("en_GB"),
    Country.AUSTRALIA: Faker("en_AU"),
    Country.GERMANY: Faker("de_DE"),
    Country.FRANCE: Faker("fr_FR"),
    Country.NETHERLANDS: Faker("nl_NL"),
    Country.ITALY: Faker("it_IT"),
}


def _get_state_abbr_and_name(faker_obj) -> tuple[str, str]:
    """Return a (abbr, name) pair for administrative region.

    Works across Faker locales and versions by trying several provider
    method names and falling back to sensible defaults.
    """
    name = None
    abbr = None

    # Try common methods that return full state/province name
    for name_fn in ("state", "province", "administrative_unit", "state_name", "subdivision"):
        if hasattr(faker_obj, name_fn) and callable(getattr(faker_obj, name_fn)):
            try:
                name = getattr(faker_obj, name_fn)()
                if name:
                    break
            except Exception:
                name = None

    # Fallback to city if we couldn't get a region name
    if not name:
        try:
            name = faker_obj.city()
        except Exception:
            name = "N/A"

    # Try common methods that return abbreviated form
    for abbr_fn in ("state_abbr", "province_abbr", "administrative_unit_abbr", "state_abbrev"):
        if hasattr(faker_obj, abbr_fn) and callable(getattr(faker_obj, abbr_fn)):
            try:
                abbr = getattr(faker_obj, abbr_fn)()
                if abbr:
                    break
            except Exception:
                abbr = None

    # Last-resort abbreviation: first two letters of the name
    if not abbr:
        if name and name != "N/A":
            letters = [c for c in name if c.isalpha()]
            abbr = ("".join(letters)[:2] or "N/A").upper()
        else:
            abbr = "N/A"

    return abbr, name

# ==================== LOAD PRODUCTS FROM DB ====================
def load_products():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute(f'SELECT productkey, "Unit_Price_USD" FROM products WHERE "Unit_Price_USD" IS NOT NULL')
    prods = []
    for row in cur.fetchall():
        try:
            key = int(row[0])
        except Exception:
            logger.warning("Skipping product with invalid key: %s", row[0])
            continue
        raw_price = row[1]
        
        s = str(raw_price).strip()
        s = s.replace(',', '')
        s = re.sub(r"[^0-9.\-]", "", s)
        try:
            price = float(s)
        except ValueError:
            logger.warning("Could not parse price '%s' for product %s, skipping", raw_price, key)
            continue
        prods.append((key, price))
    cur.close()
    conn.close()
    logger.info(f"Loaded {len(prods)} products from PostgreSQL")
    return prods

# ==================== LOAD STORES BY COUNTRY ====================
def load_stores_by_country():    
    DB_TO_ENUM = {
        'Australia': Country.AUSTRALIA,
        'United States': Country.USA,
        'Canada': Country.CANADA,
        'France': Country.FRANCE,
        'United Kingdom': Country.UK,
        'Germany': Country.GERMANY,
        'Italy': Country.ITALY,
        'Netherlands': Country.NETHERLANDS,
    }

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("SELECT storekey, country FROM stores WHERE storekey >= 0")
    stores_by_country = {}
    
    for sk, db_country in cur.fetchall():
        if db_country == 'Online':
            continue
        country_enum = DB_TO_ENUM.get(db_country.strip())
        if country_enum:
            stores_by_country.setdefault(country_enum, []).append(int(sk))
    
    cur.close()
    conn.close()
        
    for c in [
        Country.USA,
        Country.CANADA,
        Country.UK,
        Country.AUSTRALIA,
        Country.GERMANY,
        Country.FRANCE,
        Country.NETHERLANDS,
        Country.ITALY,
    ]:
        if c not in stores_by_country:
            stores_by_country[c] = [1]
        
    return stores_by_country


STORES_BY_COUNTRY = load_stores_by_country()
ONLINE_STORE_KEY = 0

# ==================== CUSTOMER CACHE ====================
customers_cache = {c: [] for c in Country}

def generate_new_customer(country: Country) -> int:
    faker = FAKERS[country]
    gender = random.choice(["M", "F"])
    first_name = faker.first_name_male() if gender == "M" else faker.first_name_female()
    full_name = f"{first_name} {faker.last_name()}"

    
    if country in [Country.USA, Country.CANADA, Country.AUSTRALIA]:
        state_abbr = faker.state_abbr()
        state_name = faker.state()
    else:
        state_abbr = "N/A"
        state_name = faker.city()  

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO generated_customers 
        (gender, name, city, state_code, state, zip_code, country, continent, birthday)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING customerkey
        """,
        (
            gender,
            full_name,
            faker.city(),
            state_abbr,
            state_name,
            faker.postcode(),
            country.value,
            "North America" if country in {Country.USA, Country.CANADA}
            else "Europe" if country in {Country.UK, Country.GERMANY, Country.FRANCE, Country.NETHERLANDS, Country.ITALY}
            else "Oceania",
            faker.date_of_birth(minimum_age=18, maximum_age=85),
        ),
    )

    customerkey = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    customers_cache[country].append(customerkey)
    logger.info(f"Created new customer {customerkey} from {country.value}")
    return customerkey

def get_customer(country: Country) -> int:
    if len(customers_cache[country]) < 20:
        for _ in range(30):
            generate_new_customer(country)
    return random.choice(customers_cache[country])

# ==================== SERIAL NUMBER ====================
def generate_order_serial(prefix: str, order_number: int, line_item: int) -> str:
    """
    يولد order_serial بالشكل الجديد: {O|C|R}-{order_number}-{line_item}
    مثال: O-990456-1
    """
    return f"{prefix}-{order_number}-{line_item}"

# ==================== WORKER ====================
def worker(wid: int):
    logger.info("Worker %d started (PID %d)", wid, os.getpid())
    update_exchange_rates()

    # Each worker has its own copy of products
    products = load_products()

    producer = SerializingProducer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "compression.type": "snappy",
            "linger.ms": 20,
            "batch.size": 65536,
            "acks": "all",
        }
    )

    def delivery_report(err, msg):
        if err is not None:
            logger.error("Delivery failed for key=%s topic=%s: %s", msg.key(), msg.topic(), err)
        else:
            logger.debug("Message delivered to %s [%s] at offset %s", msg.topic(), msg.partition(), msg.offset())

    while True:
        update_exchange_rates()
        # Choose a random country (weighted)
        country = random.choices(list(COUNTRY_WEIGHTS.keys()), weights=list(COUNTRY_WEIGHTS.values()), k=1)[0]

        # Online or store?
        if random.random() < ONLINE_ORDER_PROBABILITY:
            storekey = ONLINE_STORE_KEY
            customer_country = random.choice(list(Country))
        else:            
            # Physical store order
            country_stores = STORES_BY_COUNTRY.get(country)
            if not country_stores and country != Country.USA:
                country_stores = STORES_BY_COUNTRY.get(Country.USA, [1])
            storekey = random.choice(country_stores or [ONLINE_STORE_KEY])
            customer_country = country

        customerkey = get_customer(customer_country)
        order_number = get_next_order_number()
        order_date = datetime.now()
        group_id = f"ORD-{order_date.strftime('%Y%m%d')}-{order_number}"
        currency = COUNTRY_CURRENCY[country]

        #Number of Prodcuts in the Order
        n_lines = random.randint(1, 10)
        chosen_products = random.sample(products, k=n_lines)

        events = []
        for line_no, (pk, price) in enumerate(chosen_products, 1):
            qty = random.randint(1, 4)
            payment_method = random.choices(
                ["CASH", "WALLET", "DEBIT_CARD", "CREDIT_CARD"],
                weights=[70, 10, 10, 10],
                k=1
            )[0]
            
            line_total_usd = round(qty * price, 2)
            exchange_rate = CURRENT_RATES.get(currency, 1.0)
            line_total_local = round(line_total_usd * exchange_rate, 2)
            
            event = {
                "event_type": "ORDER",
                "order_number": order_number,
                "line_item": line_no,
                "order_serial": generate_order_serial("O", order_number, line_no),
                "order_date": order_date.strftime("%d-%m-%Y %H:%M:%S"),
                "delivery_date": None,
                "customerkey": customerkey,
                "storekey": storekey,
                "productkey": int(pk),             
                "quantity": qty,
                "payment_method": payment_method,
                "currency_code": currency,
                "status": "Completed",
                "order_group_id": group_id,
                "unit_price_usd": round(float(price), 2),  
                "line_total_amount": line_total_local,                    
                "event_timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            }
            events.append(event)
            producer.produce(topic=KAFKA_TOPIC, key=event["order_serial"], value=event, on_delivery=delivery_report)

        # CANCEL ?
        if random.random() < CANCEL_PROBABILITY:
            # time.sleep(random.uniform(0.1, 1.0))
            for ev in events:
                ev = ev.copy()
                ev.update({
                    "event_type": "CANCEL",
                    "order_serial": generate_order_serial("C", order_number, ev["line_item"]),
                    "order_date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "status": "Cancelled",     
                    "event_timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),               
                })
                producer.produce(topic=KAFKA_TOPIC, key=ev["order_serial"], value=ev, on_delivery=delivery_report)

        # RETURN ?
        if random.random() < RETURN_PROBABILITY:
            # time.sleep(random.uniform(10, 60))
            for ev in events:
                ev = ev.copy()
                ev.update({
                    "event_type": "RETURN",
                    "order_serial": generate_order_serial("R", order_number, ev["line_item"]),
                    "order_date": (order_date + timedelta(days=random.randint(7, 45))).strftime("%d-%m-%Y %H:%M:%S"),
                    "status": "Returned",
                    "event_timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                })
                producer.produce(topic=KAFKA_TOPIC, key=ev["order_serial"], value=ev, on_delivery=delivery_report)

        producer.poll(0)


# ==================== MAIN ====================
if __name__ == "__main__":
    logger.info("Starting Sales Producer with %d workers", NUM_WORKERS)
    processes = [Process(target=worker, args=(i+1,)) for i in range(NUM_WORKERS)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()