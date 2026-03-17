
CREATE OR REPLACE TABLE CRYPTO_DB.STAGING.STG_CRYPTO_MARKET_DATA AS

SELECT
    ID                                      AS coin_id,
    SYMBOL                                  AS symbol,
    NAME                                    AS coin_name,
    CURRENT_PRICE                           AS current_price_usd,
    HIGH_24H                                AS high_price_24h,
    LOW_24H                                 AS low_price_24h,
    PRICE_CHANGE_24H                        AS price_change_24h,
    PRICE_CHANGE_PERCENTAGE_24H             AS price_change_pct_24h,
    (HIGH_24H - LOW_24H)                    AS price_range_24h,
    CASE
        WHEN LOW_24H > 0
        THEN ROUND((HIGH_24H - LOW_24H) / LOW_24H * 100, 4)
        ELSE NULL
    END                                     AS price_range_pct_24h,
    MARKET_CAP                              AS market_cap_usd,
    MARKET_CAP_RANK                         AS market_cap_rank,
    TOTAL_VOLUME                            AS total_volume_usd,
    CASE
        WHEN MARKET_CAP > 0
        THEN ROUND(TOTAL_VOLUME / MARKET_CAP * 100, 4)
        ELSE NULL
    END                                     AS volume_to_market_cap_pct,
    CIRCULATING_SUPPLY                      AS circulating_supply,
    TOTAL_SUPPLY                            AS total_supply,
    CASE
        WHEN TOTAL_SUPPLY > 0
        THEN ROUND(CIRCULATING_SUPPLY / TOTAL_SUPPLY * 100, 2)
        ELSE NULL
    END                                     AS circulating_supply_pct,
    LAST_UPDATED                            AS last_updated_at,
    FETCHED_AT                              AS fetched_at,
    FETCHED_AT::DATE                        AS data_date,
    CASE
        WHEN PRICE_CHANGE_PERCENTAGE_24H >  1 THEN 'GAINER'
        WHEN PRICE_CHANGE_PERCENTAGE_24H < -1 THEN 'LOSER'
        ELSE 'STABLE'
    END                                     AS price_movement_label

FROM CRYPTO_DB.RAW.RAW_CRYPTO_MARKET_DATA

WHERE
    CURRENT_PRICE   IS NOT NULL
    AND MARKET_CAP  IS NOT NULL
    AND ID          IS NOT NULL;