-- ============================================================
-- snowflake_schema.sql
-- ============================================================
-- Run this ONCE manually in your Snowflake worksheet
-- before running the pipeline for the first time.
-- ============================================================


-- ── Step 1: Create the database ──────────────────────────
CREATE DATABASE IF NOT EXISTS CRYPTO_DB
    COMMENT = 'Cryptocurrency market data pipeline';

USE DATABASE CRYPTO_DB;


-- ── Step 2: Create the three schemas ─────────────────────
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw unmodified data loaded directly from CoinGecko API';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and validated data — produced by SQL transformations';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Aggregated analytics-ready tables — produced by SQL transformations';


-- ── Step 3: Create the virtual warehouse ─────────────────
-- A warehouse in Snowflake = the compute engine (CPU/RAM)
-- X-SMALL is the smallest and cheapest size
-- AUTO_SUSPEND shuts it off after 60s of inactivity
-- AUTO_RESUME wakes it up automatically when needed
-- You only pay when queries are actually running

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Compute warehouse for crypto pipeline';

USE WAREHOUSE COMPUTE_WH;


-- ── Step 4: Create the RAW table ─────────────────────────
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_CRYPTO_MARKET_DATA (

    -- Identity fields
    ID                          VARCHAR(100)    NOT NULL,
    SYMBOL                      VARCHAR(20)     NOT NULL,
    NAME                        VARCHAR(200)    NOT NULL,

    -- Price fields
    CURRENT_PRICE               FLOAT,
    HIGH_24H                    FLOAT,
    LOW_24H                     FLOAT,
    PRICE_CHANGE_24H            FLOAT,
    PRICE_CHANGE_PERCENTAGE_24H FLOAT,

    -- Market fields
    MARKET_CAP                  FLOAT,
    MARKET_CAP_RANK             INTEGER,
    TOTAL_VOLUME                FLOAT,

    -- Supply fields
    CIRCULATING_SUPPLY          FLOAT,
    TOTAL_SUPPLY                FLOAT,

    -- Timestamp fields
    LAST_UPDATED                TIMESTAMP_TZ,
    FETCHED_AT                  TIMESTAMP_TZ    NOT NULL,
    LOADED_AT                   TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Raw cryptocurrency market data loaded from CoinGecko API';


-- ── Step 5: Verify everything was created ────────────────
SHOW DATABASES  LIKE 'CRYPTO_DB';
SHOW SCHEMAS    IN DATABASE CRYPTO_DB;
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW TABLES     IN SCHEMA CRYPTO_DB.RAW;

-- Preview the empty table structure
SELECT * FROM CRYPTO_DB.RAW.RAW_CRYPTO_MARKET_DATA LIMIT 10;