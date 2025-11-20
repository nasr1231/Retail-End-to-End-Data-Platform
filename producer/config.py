import os
from enum import Enum

from dotenv import load_dotenv
# from schema_registry import avro_serializer, uuid_serializer

load_dotenv()

# ==================== GENERAL CONFIG ====================
NUM_WORKERS = 2
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
EVENT_INTERVAL_SECONDS = 0.3
NEW_USER_SESSION_PROBABILITY = 0.250

# ===================== COUNTRIES ENUM =====================
class Country(Enum):
    USA = "USA"
    CANADA = "CANADA"
    UK = "UK"
    AUSTRALIA = "AUSTRALIA"
    GERMANY = "GERMANY"
    FRANCE = "FRANCE"
    NETHERLANDS = "NETHERLANDS"
    ITALY = "ITALY"

COUNTRY_WEIGHTS = {
    Country.USA: 13,
    Country.CANADA: 14,
    Country.UK: 13,
    Country.AUSTRALIA: 10,
    Country.GERMANY: 11,
    Country.FRANCE: 13,
    Country.NETHERLANDS: 14,
    Country.ITALY: 12,
}

COUNTRY_CURRENCY = {
    Country.USA: "USD",
    Country.CANADA: "CAD",
    Country.UK: "GBP",
    Country.AUSTRALIA: "AUD",
    Country.GERMANY: "EUR",
    Country.FRANCE: "EUR",
    Country.NETHERLANDS: "EUR",
    Country.ITALY: "EUR",
}

# PRODUCER_CONF = {
#     'acks': 'all',
#     'batch.size': 32768,  # 32 KB
#     'linger.ms': 20,
#     'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
#     'compression.type': 'snappy',
#     'key.serializer': uuid_serializer,
#     'value.serializer': avro_serializer,
# }

# ==================== PROBABILITIES ====================
# Online order probability
ONLINE_ORDER_PROBABILITY = 0.22

# Cancellation and return probabilities
CANCEL_PROBABILITY = 0.07
RETURN_PROBABILITY = 0.04
