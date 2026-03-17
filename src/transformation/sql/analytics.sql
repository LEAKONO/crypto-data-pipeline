
CREATE OR REPLACE TABLE CRYPTO_DB.ANALYTICS.ANALYTICS_DAILY_CRYPTO_SUMMARY AS

SELECT
    data_date,
    COUNT(DISTINCT coin_id)                         AS total_coins_tracked,
    ROUND(SUM(market_cap_usd), 2)                   AS total_market_cap_usd,
    ROUND(SUM(total_volume_usd), 2)                 AS total_volume_usd,
    ROUND(AVG(price_change_pct_24h), 4)             AS avg_price_change_pct,
    MAX(price_change_pct_24h)                       AS max_price_change_pct,
    MIN(price_change_pct_24h)                       AS min_price_change_pct,
    COUNT(
        CASE WHEN price_movement_label = 'GAINER'
        THEN 1 END
    )                                               AS coins_gaining,
    COUNT(
        CASE WHEN price_movement_label = 'LOSER'
        THEN 1 END
    )                                               AS coins_losing,
    COUNT(
        CASE WHEN price_movement_label = 'STABLE'
        THEN 1 END
    )                                               AS coins_stable,
    MAX_BY(coin_id, total_volume_usd)               AS top_coin_by_volume,
    MAX_BY(coin_id, price_change_pct_24h)           AS top_gaining_coin,
    MIN_BY(coin_id, price_change_pct_24h)           AS top_losing_coin,
    MAX_BY(coin_id, price_range_pct_24h)            AS most_volatile_coin,
    CURRENT_TIMESTAMP()                             AS created_at

FROM CRYPTO_DB.STAGING.STG_CRYPTO_MARKET_DATA

GROUP BY data_date
ORDER BY data_date DESC;