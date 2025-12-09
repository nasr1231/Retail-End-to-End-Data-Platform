# End-to-End Data Platform: Tech Shop E-Commerce Analytics

An enterprise-grade Lambda architecture data platform built as an ITI Data Engineering capstone project, designed to transform fragmented e-commerce data into a unified analytics foundation supporting real-time insights and historical analysis.

## 📋 Table of Contents

- [Business Context](#business-context)
- [Architecture Overview](#architecture-overview)
- [Problem Statement](#problem-statement)
- [Solution Design](#solution-design)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Installation & Setup](#installation--setup)
- [Data Pipeline](#data-pipeline)
- [Key Features](#key-features)
- [Deliverables](#deliverables)
- [Getting Started](#getting-started)
- [Team](#team)

---

## 🎯 Business Context

**Tech Shop** is a mid-sized international electronics retailer facing critical operational challenges:

- **67 stores** across **8 countries**
- **$55M** in sales
- **15,000+ customers**
- **26,000+ orders** processed across 5 years

### Business Pain Points

The organization struggled with severe data fragmentation, leading to:

- **24-hour data latency** preventing real-time operational decisions
- **Siloed data sources** across incompatible legacy systems
- **No single source of truth**, causing data inconsistencies
- **Lack of customer insights** (no RFM/CLV segmentation) limiting personalization
- **Blind inventory management** causing stockouts during high-demand periods
- **Delayed anomaly detection** for returns/cancellations (1-day lag = massive revenue loss)

---

## 🏗️ Architecture Overview

The platform implements a **Lambda Architecture** combining batch and streaming layers for comprehensive data coverage:

```
Data Sources (PostgreSQL, CSV)
    ↓
┌─────────────────────────────────┬──────────────────────────────────┐
│      BATCH LAYER (Cold Path)    │   STREAMING LAYER (Hot Path)     │
├─────────────────────────────────┼──────────────────────────────────┤
│ • NiFi Data Ingestion           │ • Kafka Producer (Python)        │
│ • AWS S3 Data Lake (Avro/JSON)  │ • Schema Registry (Avro)         │
│ • Snowflake DW (Medallion)      │ • Kafka Connect (S3 Sink)        │
│ • dbt Transformations           │ • Apache Spark Streaming         │
│ • Fact & Dimension Tables       │ • Cassandra DB (Time-series)     │
└─────────────────────────────────┴──────────────────────────────────┘
    ↓                                   ↓
SERVING LAYER: Snowflake | Cassandra
    ↓
┌─────────────────────────────────────────┐
│     CONSUMER LAYER                      │
├──────────────────┬──────────────────────┤
│  Power BI        │  Grafana (Real-time) │
│  (Historical)    │  (Live Monitoring)   │
└──────────────────┴──────────────────────┘
```

---

## 🔍 Problem Statement

| Challenge | Impact | Solution |
|-----------|--------|----------|
| **Data Fragmentation** | Customer, order, product data scattered across systems | Unified medallion architecture in Snowflake |
| **No Real-time Visibility** | Inventory stockouts, delayed returns flagging | Apache Kafka streaming pipeline (<50ms latency) |
| **Manual Insights** | No automated reporting, slow decision-making | Power BI + Grafana dashboards with DAX measures |
| **Data Quality Issues** | Inconsistent formats, null values, duplicates | Multi-layer validation, dbt testing, data quality checks |
| **Scaling Challenges** | Legacy systems unable to handle growth | Cloud-native architecture (Snowflake + AWS S3) |

---

## 💡 Solution Design

### Batch Layer (Cold Path) - Historical Analytics

**Purpose:** Deep analysis of historical patterns for business intelligence and strategic decisions.

**Flow:**
1. **Data Ingestion (NiFi):** Extract structured data from PostgreSQL database
2. **Data Lake (AWS S3):** Store raw data in Avro format for cost efficiency
3. **Medallion Architecture:**
   - **Bronze Layer:** External tables pointing to S3 raw data
   - **Silver Layer:** Data quality transformations (type standardization, null handling, deduplication)
   - **Gold Layer:** Dimensional modeling using galaxy schema
4. **Orchestration (Airflow):** Automated DAG scheduling with dependency management
5. **Serving:** Snowflake as single source of truth

**dbt Transformations:**
- External table creation from S3
- Silver layer quality checks (data type validation, whitespace trimming)
- Gold layer fact/dimension tables with surrogate keys
- Automated documentation and unit testing

### Streaming Layer (Hot Path) - Real-time Operations

**Purpose:** Sub-second event processing for operational alerting and live dashboards.

**Flow:**
1. **Data Simulation (Python):** Generate realistic order events, customer data, exchange rates
2. **Schema Management:** Avro schema with Schema Registry for data governance
3. **Kafka Brokers:** Distributed topic storage with partitioning and retention policies
4. **Stream Processing (Spark Streaming):**
   - Ingestion & deserialization of Avro-encoded events
   - Currency normalization across markets
   - Return/cancellation flagging for anomaly detection
   - Processing lag monitoring
5. **Dual Output:**
   - **Stream A (Item-level):** Product details, quantities, pricing
   - **Stream B (Order-level):** Aggregated order metrics, totals
6. **Sinking:** Kafka Connect writes processed streams to Cassandra + S3
7. **Analytics:** Real-time dashboards in Grafana + historical archive

---

## 📁 Project Structure

```
tech-shop-data-platform/
│
├── sources/                      # Source data definitions
│   ├── __init__.py
│   └── README.md
│
├── producer/                     # Kafka data simulation
│   ├── order_producer.py
│   ├── customer_producer.py
│   ├── exchange_rate_producer.py
│   └── README.md
│
├── connectors/                   # Kafka Connect configurations
│   ├── s3-sink-config.json       # S3 Sink Connector
│   ├── cassandra-sink-config.json # Cassandra Sink
│   └── README.md
│
├── spark/                        # Stream processing jobs
│   ├── spark_streaming_items.py  # Item-level transformations
│   ├── spark_streaming_orders.py # Order-level aggregations
│   └── README.md
│
├── dbt/                          # Data transformation models
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   ├── intermediate/
│   │   │   └── silver_*.sql
│   │   └── marts/
│   │       ├── dim_customer.sql
│   │       ├── dim_product.sql
│   │       ├── fact_sales.sql
│   │       └── fact_product_sales.sql
│   ├── tests/
│   │   ├── test_unique_keys.sql
│   │   ├── test_not_null.sql
│   │   └── test_referential_integrity.sql
│   ├── macros/
│   ├── dbt_project.yml
│   └── README.md
│
├── airflow/                      # Orchestration DAGs
│   ├── dags/
│   │   ├── batch_layer_dag.py
│   │   ├── nifi_ingestion_dag.py
│   │   └── data_quality_dag.py
│   ├── docker-compose.yml
│   └── README.md
│
├── cassandra/                    # NoSQL time-series database
│   ├── schema_definition.cql
│   └── README.md
│
├── docker-compose.yml            # Full stack orchestration
├── docker-commands.sh            # Useful Docker commands
├── Dockerfile.nifi               # NiFi image
├── postgres.jar                  # PostgreSQL driver
├── s3-sink-config.json           # Cloud storage connector
│
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🛠️ Technology Stack

### Data Integration & Ingestion
- **Apache NiFi:** Data flow management and extraction from PostgreSQL
- **Apache Kafka:** Distributed event streaming (raw topics, processed topics)
- **Kafka Connect:** Source/sink connectors for seamless data movement
- **Schema Registry:** Data governance with Avro schema versioning

### Data Storage & Processing
- **AWS S3:** Data lake (cost-effective, scalable)
- **Snowflake:** Cloud data warehouse (medallion architecture)
- **PostgreSQL:** Source operational database
- **Cassandra:** High-write, time-series analytics store
- **Apache Spark:** Stream processing (<50ms latency)

### Data Transformation & Orchestration
- **dbt:** Data transformation, testing, and documentation
- **Apache Airflow:** Workflow orchestration and DAG scheduling

### Analytics & Visualization
- **Power BI:** Historical BI dashboards (sales, products, customers)
- **Grafana:** Real-time operational dashboards
- **Redis:** Optional caching layer

### Infrastructure & DevOps
- **Docker & Docker Compose:** Containerization and local development
- **Python:** Data simulation and custom scripts

---

## 🚀 Installation & Setup

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- PostgreSQL
- AWS account (S3 bucket)
- Snowflake account

### Local Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/[your-org]/tech-shop-data-platform.git
   cd tech-shop-data-platform
   ```

2. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Start the entire stack:**
   ```bash
   docker-compose up -d
   ```

   This starts:
   - Apache NiFi (port 8080)
   - Kafka Brokers (port 9092)
   - Zookeeper (port 2181)
   - Cassandra (port 9042)
   - PostgreSQL (port 5432)
   - Airflow (port 8888)
   - Grafana (port 3000)

4. **Configure Kafka Schema Registry:**
   ```bash
   docker exec kafka curl -X POST http://localhost:8081/subjects/sales-events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d @schemas/sales-event.avsc
   ```

5. **Initialize Snowflake:**
   ```bash
   # Create external tables pointing to S3
   # See dbt/models/staging/external_tables.sql
   ```

6. **Run dbt models:**
   ```bash
   cd dbt
   dbt deps
   dbt run
   dbt test
   ```

---

## 📊 Data Pipeline

### Batch Pipeline (Daily)

```
PostgreSQL Database
    ↓
[NiFi] Extract & Transform
    ↓
AWS S3 (Avro Format)
    ↓
[Airflow DAG] Scheduled at 02:00 UTC
    ↓
Snowflake External Tables (Bronze)
    ↓
[dbt] Silver Layer Transformations
    ├─ Data type standardization
    ├─ NULL handling
    ├─ Deduplication
    └─ Whitespace trimming
    ↓
[dbt] Gold Layer (Galaxy Schema)
    ├─ fact_sales (grain: order)
    ├─ fact_product_sales (grain: product)
    ├─ dim_customer (SCD Type 2)
    ├─ dim_product
    ├─ dim_store
    ├─ dim_date
    └─ dim_exchange_rates
    ↓
[Power BI] Historical Analytics Dashboards
```

**Success Criteria:**
- ✓ 100% data completeness (no missing rows)
- ✓ <2% null values post-cleansing
- ✓ All foreign keys validated
- ✓ Processing completes by 06:00 UTC

### Streaming Pipeline (Real-time)

```
Python Producer Scripts
├─ Order Events (order_producer.py)
├─ Customer Data (customer_producer.py)
└─ Exchange Rates (exchange_rate_producer.py)
    ↓
[Kafka] Raw Topics (Order, Customer, ExchangeRate)
    ↓
[Schema Registry] Avro Schema Validation
    ↓
[Kafka Connect] Multi-sink distribution
├─ S3 Sink Connector → AWS S3 (Data Lake Archive)
└─ [Spark Streaming] Processing
    ├─ Ingestion & Deserialization
    ├─ Currency Normalization
    ├─ Return/Cancellation Flagging
    ├─ Processing Lag Monitoring
    └─ Dual Stream Output
        ├─ Stream A: Item-level details
        └─ Stream B: Order-level aggregations
    ↓
[Kafka Connect] Processed Topic Sink
    ├─ Cassandra Sink → Time-series DB
    └─ S3 Sink → Historical Archive
    ↓
[Grafana] Real-time Dashboards
    ├─ Order Volume & Revenue
    ├─ Cancellation/Return Tracking
    ├─ Payment Method Analysis
    └─ Operational Risk Monitoring

Latency: <50ms (Spark processing)
Throughput: 10K+ events/second
```

---

## ✨ Key Features

### 1. Data Quality Framework

- **dbt Tests:** Unique keys, not-null constraints, referential integrity
- **Dead Letter Queue (DLQ):** Captures malformed or failed records
- **Data Validation:** Pre/post-transformation checks
- **Schema Enforcement:** Avro schema validation for streaming data

### 2. Multi-Source Integration

| Source | Format | Frequency | Volume |
|--------|--------|-----------|--------|
| PostgreSQL | Relational | Daily (batch) | 26K orders/day |
| Kafka | JSON/Avro | Real-time | 10K events/sec |
| CSV Files | Structured | On-demand | Variable |
| APIs | REST/JSON | Real-time | Exchange rates |

### 3. Galaxy Schema Design

**Fact Tables:**
- `fact_sales` (Grain: Order-level)
  - Measures: Total revenue, cost, profit (USD + local)
  - Foreign keys: customer_fk, store_fk, date_fk, exchange_rate_fk

- `fact_product_sales` (Grain: Product-level)
  - Measures: Units sold, revenue per product
  - Enables: Product mix analysis, category trends

**Dimension Tables:**
- `dim_customer` (SCD Type 2): Name, gender, city, state, country, continent, birthday
- `dim_product_info`: Name, brand, category, subcategory, pricing, attributes
- `dim_store`: Location, square meters, open date
- `dim_date`: Year, quarter, month, day, holiday flags
- `dim_exchange_rates`: Currency code, conversion rates by date

### 4. Real-time Operational Monitoring

- **Order Volume Ticker:** Live order count and revenue
- **Cancellation Alerts:** Real-time anomaly detection
- **Return Tracking:** Revenue lost pre/post-fulfillment
- **Payment Method Mix:** Customer payment preferences
- **Top Selling Products:** Item-level insights
- **Shipping Performance:** Lag analysis and optimization

### 5. Scalability & Fault Tolerance

- **Horizontal Scaling:** Kafka brokers, Spark executors, Cassandra nodes
- **Fault Tolerance:** Spark checkpointing, Kafka replication (3x)
- **Dead Letter Queue:** Error handling without data loss
- **Retention Policies:** Configurable topic and storage retention
- **Cloud-native:** AWS S3 cost optimization, Snowflake auto-scaling

---

## 📦 Deliverables

### 1. Data Warehouse
- Snowflake medallion architecture (Bronze/Silver/Gold)
- 5 normalized dimensions + 2 fact tables (galaxy schema)
- Single source of truth for all business metrics
- Enforced data integrity with referential constraints

### 2. Power BI Dashboards (Historical Analytics)
- **Sales Dashboard:** Revenue trends by quarter/category, AOV ($2.1K), profit margins
- **Product Analysis:** Top brands (Adventure Works, Contoso, WWI), category distribution
- **Customer Insights:** Demographics (15K+ customers), segmentation by age/geography
- **Store Performance:** Regional analysis, best/worst performers, expansion metrics
- **Geographic Coverage:** 27 states, 8 countries, online vs. in-store split

### 3. Kafka Streaming Pipeline
- Raw topic ingestion with Avro schema validation
- Processed topics with currency normalization and enrichment
- S3 data lake archive for long-term analytics
- Error handling with dead letter queues

### 4. Grafana Real-time Dashboard
- **Financial Performance:** Order volume, revenue ticker, trend lines
- **Customer Behavior:** Payment method distribution, top products
- **Operational Risk:** Cancellations/returns, revenue impact, top problematic SKUs
- **Latency Monitoring:** Processing lag tracking (<5 seconds baseline)

### 5. Documentation & Code Quality
- dbt auto-generated documentation with lineage
- Comprehensive README files for each module
- Unit tests for all transformations
- Docker compose for reproducible setup

---

## 🏃 Getting Started

### 1. Start Development Environment

```bash
docker-compose up -d
```

### 2. Configure Data Sources

```bash
# NiFi Data Flow (http://localhost:8080)
# - Add PostgreSQL data source
# - Configure output to AWS S3

# Schema Registry
curl -X POST http://localhost:8081/schemas/register \
  -H "Content-Type: application/json" \
  -d '{"schema":"...avro schema..."}'
```

### 3. Run Batch Pipeline

```bash
# Initialize Airflow DAGs
airflow dags trigger batch_layer_dag

# Or manually run dbt
cd dbt && dbt run --models staging.* intermediate.* marts.*
```

### 4. Start Streaming

```bash
python producer/order_producer.py
python producer/customer_producer.py
```

### 5. Monitor Dashboards

- **Grafana:** http://localhost:3000
- **Airflow:** http://localhost:8888
- **Power BI:** Connect to Snowflake data warehouse

---

## 👥 Team

This project was developed by:

- **Mohamed Nasr** - Stream Data Ingestion & Schema Management
- **Mohamed Abdelsalam** - Stream Processing & Real-time Analytics
- **Eman Mohamed** - Data Ingestion & Data Lake Management
- **Abdelzaher Mohamed** - Data Transformation & Warehouse Design
- **Omar Khaled** - Business Intelligence & Analytics


**Institution:** Information Technology Institute (ITI), Data Engineering Intensive Track, R1 2025

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Last Updated:** December 2025  
**Status:** Production-Ready (ITI Capstone Project)
