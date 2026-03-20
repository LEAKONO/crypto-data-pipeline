
CREATE DATABASE IF NOT EXISTS CRYPTO_DB
    COMMENT = 'Cryptocurrency market data pipeline';

USE DATABASE CRYPTO_DB;


CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw unmodified data loaded directly from CoinGecko API';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and validated data — produced by SQL transformations';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Aggregated analytics-ready tables — produced by SQL transformations';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Compute warehouse for crypto pipeline';

USE WAREHOUSE COMPUTE_WH;


USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_CRYPTO_MARKET_DATA (

    ID                          VARCHAR(100)    NOT NULL,
    SYMBOL                      VARCHAR(20)     NOT NULL,
    NAME                        VARCHAR(200)    NOT NULL,

    CURRENT_PRICE               FLOAT,
    HIGH_24H                    FLOAT,
    LOW_24H                     FLOAT,
    PRICE_CHANGE_24H            FLOAT,
    PRICE_CHANGE_PERCENTAGE_24H FLOAT,

    MARKET_CAP                  FLOAT,
    MARKET_CAP_RANK             INTEGER,
    TOTAL_VOLUME                FLOAT,

    CIRCULATING_SUPPLY          FLOAT,
    TOTAL_SUPPLY                FLOAT,

    LAST_UPDATED                TIMESTAMP_TZ,
    FETCHED_AT                  TIMESTAMP_TZ    NOT NULL,
    LOADED_AT                   TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Raw cryptocurrency market data loaded from CoinGecko API';


SHOW DATABASES  LIKE 'CRYPTO_DB';
SHOW SCHEMAS    IN DATABASE CRYPTO_DB;
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW TABLES     IN SCHEMA CRYPTO_DB.RAW;

SELECT * FROM CRYPTO_DB.RAW.RAW_CRYPTO_MARKET_DATA LIMIT 10;