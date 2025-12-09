# Apache Airflow Custom Docker Image Documentation

## 📋 Overview

This Docker image is a customized Apache Airflow environment specifically designed for orchestrating data pipelines in a retail end-to-end data platform. It integrates Apache Airflow with DBT (Data Build Tool), AWS S3, and Snowflake to provide a comprehensive data orchestration solution.

## 🏗️ Base Image

```dockerfile
FROM apache/airflow:3.0.2-python3.11
```

- **Base Image**: Apache Airflow 3.0.2
- **Python Version**: 3.11

## 📦 Installed Components

### Core Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `apache-airflow` | ${AIRFLOW_VERSION} | Core workflow orchestration platform |
| `dbt-core` | 1.8.7 | Data transformation framework |
| `dbt-snowflake` | Latest | DBT adapter for Snowflake |
| `apache-airflow-providers-amazon` | ≥10.0.0 | AWS services integration (S3, etc.) |
| `boto3` | Latest | AWS SDK for Python |
| `s3fs` | Latest | Pythonic file interface to S3 |
| `dotenv` | 0.9.9 | Environment variables management |
| `pandas` | 2.1.4 | Data manipulation and analysis |
| `requests` | 2.32.3 | HTTP library for API calls |

### System Packages

The image includes the following system-level packages installed via APT:

| Package | Purpose |
|---------|---------|
| `build-essential` | Compilation tools for building Python packages |
| `libpq-dev` | PostgreSQL development libraries |
| `git` | Version control system |
| `openjdk-17-jre-headless` | Java Runtime Environment (required for certain connectors) |
| `wget` | File download utility |

## 👥 User Configuration

The container runs as the `airflow` user (non-root) for security purposes:

```dockerfile
USER airflow
```

This follows Docker security best practices by avoiding root execution.

## 🗂️ Directory Structure

```
/opt/airflow/
├── dags/                    # Airflow DAG definitions
├── plugins/                 # Custom Airflow plugins
├── logs/                    # Execution logs
├── config/                  # Configuration files
└── dbt/                     # DBT project files
```


## 🔐 Required Environment Variables

When deploying this image, you'll need to set the following environment variables:

### Airflow Configuration

```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@host/db
AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
AIRFLOW__WEBSERVER__SECRET_KEY=<your-secret-key>
```

### AWS Credentials

```bash
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_DEFAULT_REGION=us-east-1
```

### Snowflake Credentials

```bash
SNOWFLAKE_ACCOUNT=<account-identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_WAREHOUSE=<warehouse-name>
SNOWFLAKE_DATABASE=<database-name>
SNOWFLAKE_SCHEMA=<schema-name>
```

## 🎯 Key Features

### 1. **DBT Integration**
- Supports data transformation workflows using DBT
- Enables SQL-based data modeling
- Version-controlled data transformations

### 2. **Cloud Storage Integration**
- Native S3 connectivity via `boto3` and `s3fs`
- Seamless file operations with cloud storage
- Support for large-scale data ingestion

### 3. **Snowflake Data Warehouse**
- Direct connection to Snowflake
- Optimized for cloud data warehousing
- High-performance analytics queries

### 4. **Python Data Processing**
- Pandas for data manipulation
- Requests for API integration
- Flexible Python-based transformations

## 📊 Use Cases

This Docker image is ideal for:

- **ETL/ELT Pipelines**: Extract, Transform, Load data workflows
- **Data Warehouse Orchestration**: Snowflake-based data warehousing
- **Cloud Data Lakes**: S3-based data lake operations
- **Data Quality Checks**: Automated data validation
- **Scheduled Reporting**: Automated report generation
- **Data Transformation**: DBT-powered SQL transformations
