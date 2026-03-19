# Crypto Market Data Pipeline

A production-style end-to-end data pipeline that ingests live
cryptocurrency market data from the CoinGecko API, stores it in
Snowflake, transforms it into analytics-ready tables, and runs
automatically on a daily schedule using Apache Airflow.

---

##  Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
- [Data Layers](#data-layers)
- [Setup and Installation](#setup-and-installation)
- [Running the Pipeline](#running-the-pipeline)
- [Running with Airflow](#running-with-airflow)
- [Environment Variables](#environment-variables)
- [Sample Output](#sample-output)

---

## Project Overview

This project demonstrates a complete data engineering workflow:

- Fetches the **top 100 cryptocurrencies** by market cap from the
  CoinGecko public API
- Loads raw data into a **Snowflake data warehouse**
- Transforms raw data into **staging and analytics layers** using SQL
- Orchestrates the entire pipeline with **Apache Airflow**
- Includes production-grade **logging, error handling, and retries**

---

## Architecture
```
CoinGecko API
      │
      │  HTTP GET (JSON)
      ▼
┌─────────────────┐
│  Stage 1        │  Python — coingecko_ingest.py
│  Data Ingestion │  Fetches + cleans 100 coin records
└────────┬────────┘
         │  pandas DataFrame
         ▼
┌─────────────────┐
│  Stage 2        │  Python — snowflake_loader.py
│  Raw Storage    │  Loads into CRYPTO_DB.RAW
└────────┬────────┘
         │  SQL
         ▼
┌─────────────────┐
│  Stage 3        │  SQL — staging.sql + analytics.sql
│  Transformation │  RAW → STAGING → ANALYTICS
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  Apache Airflow                         │
│  Orchestrates all stages automatically  │
│  Schedule: every day at 08:00 UTC       │
└─────────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Data ingestion, loading, orchestration |
| Apache Airflow 2.10 | Workflow orchestration and scheduling |
| Snowflake | Cloud data warehouse |
| SQL | Data transformation |
| pandas | Data manipulation |
| PostgreSQL | Airflow metadata database |

---

## Project Structure
```
crypto_pipeline/
├── dags/
│   └── crypto_pipeline_dag.py
├── src/
│   ├── ingestion/
│   │   └── coingecko_ingest.py
│   ├── storage/
│   │   └── snowflake_loader.py
│   ├── transformation/
│   │   ├── transform_queries.py
│   │   └── sql/
│   │       ├── staging.sql       
│   │       └── analytics.sql     # STAGING → ANALYTICS
│   └── utils/
│       ├── exceptions.py
│       └── pipeline_runner.py
├── config/
│   ├── settings.py              
│   ├── logger.py                 
│   └── snowflake_schema.sql
├── logs/                       
├── tests/
│   ├── test_ingestion.py
│   └── test_storage.py
├── .env.example                
├── requirements.txt             
└── README.md
```

---

## Pipeline Stages

### Stage 1 — Data Ingestion
- Calls the CoinGecko `/coins/markets` endpoint
- Fetches top 100 cryptocurrencies by market cap
- Cleans and types the data using pandas
- Adds an audit `fetched_at` timestamp
- Implements retry logic with 3 attempts and 5 second delays

### Stage 2 — Raw Data Storage
- Automatically creates Snowflake database, schemas, warehouse and
  table if they do not exist
- Bulk loads the DataFrame using `write_pandas`
- Verifies load success with a COUNT query

### Stage 3 — SQL Transformations
**Staging layer** adds derived columns:
- `price_range_24h` — daily price volatility in USD
- `price_range_pct_24h` — daily price volatility as percentage
- `volume_to_market_cap_pct` — trading activity ratio
- `circulating_supply_pct` — supply in circulation
- `price_movement_label` — GAINER / LOSER / STABLE

**Analytics layer** aggregates to one row per day:
- Total market capitalisation
- Average price change across all coins
- Count of gaining vs losing coins
- Top coin by volume
- Most volatile coin

### Stage 4 — Orchestration
- Apache Airflow DAG with three PythonOperator tasks
- Tasks run in strict order with dependency chain
- Automatic retries: 3 attempts with 5 minute delay
- Daily schedule at 08:00 UTC
- XCom used to pass DataFrame between tasks

---

## Data Layers
```
Snowflake: CRYPTO_DB
├── RAW schema
│   └── RAW_CRYPTO_MARKET_DATA      ← untouched API data
├── STAGING schema
│   └── STG_CRYPTO_MARKET_DATA      ← cleaned + enriched
└── ANALYTICS schema
    └── ANALYTICS_DAILY_CRYPTO_SUMMARY  ← one row per day
```

---

## Setup and Installation

### Prerequisites
- Python 3.12+
- Snowflake account (free trial at snowflake.com)
- PostgreSQL (for Airflow metadata)
- Git

### 1. Clone the repository
```bash
git clone https://github.com/LEAKONO/crypto-data-pipeline.git
cd crypto_pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values:
```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN

# CoinGecko API
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_COINS_LIMIT=100
COINGECKO_VS_CURRENCY=usd

# Pipeline
PIPELINE_ENV=development
LOG_LEVEL=INFO
```

---

## Running the Pipeline

### Run the full pipeline (all stages)
```bash
python3 -m src.utils.pipeline_runner
```

### Run individual stages
```bash
# Stage 1 only — fetch from API
python3 -m src.ingestion.coingecko_ingest

# Stage 2 only — load into Snowflake
python3 -m src.storage.snowflake_loader

# Stage 3 only — run SQL transformations
python3 -m src.transformation.transform_queries
```

---

## Running with Airflow

### Start PostgreSQL
```bash
sudo service postgresql start
```

### Set environment variables
```bash
export AIRFLOW_HOME=~/path/to/crypto_pipeline/airflow
export AIRFLOW__CORE__DAGS_FOLDER=~/path/to/crypto_pipeline/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
```

### Start Airflow
```bash
airflow standalone
```

### Trigger the DAG
```bash
airflow dags unpause crypto_pipeline
airflow dags trigger crypto_pipeline
```

Open `http://localhost:8080` to view the Airflow UI.

---

## Sample Output
```
PIPELINE RUN SUMMARY  ✓ SUCCESS
============================================================
Timestamp    : 2026-03-19T08:11:28 UTC
Total time   : 11.38 seconds
Rows ingested: 100
Rows loaded  : 100

  ✓ ingestion       SUCCESS  (1.04s)
  ✓ storage         SUCCESS  (7.25s)
  ✓ transformation  SUCCESS  (3.10s)
============================================================
```

---

## Key Engineering Decisions

**Why CoinGecko?** Free public API, no authentication required,
high quality financial data, generous rate limits for learning.

**Why Snowflake?** Industry-standard cloud data warehouse used by
thousands of companies. Separates storage from compute, scales
infinitely, and has a generous free trial.

**Why Airflow?** The most widely used workflow orchestration tool
in data engineering. Allows pipelines to be defined as code,
retried automatically, and monitored visually.

**Why three data layers?** RAW preserves the original data forever.
STAGING enriches without losing history. ANALYTICS serves business
queries efficiently. This is the medallion architecture pattern
used by leading data teams.


## Author

**Emmanuel Leakono**

Data Engineer

 leakonoemmanuel3@gmail.com

[LinkedIn](https://www.linkedin.com/in/emmanuel-leakono/)

 [GitHub](https://github.com/LEAKONO)

---

*Built with ❤️ as a portfolio data engineering project.*
*Feel free to fork, star, or reach out with any questions.*