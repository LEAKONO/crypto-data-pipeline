
import logging
from datetime import datetime, timezone
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from config.settings import SNOWFLAKE_CONFIG, SNOWFLAKE_TABLES, validate_config
logger = logging.getLogger(__name__)

RAW_TABLE = SNOWFLAKE_TABLES["raw"]  
# Snowflake stores column names in UPPERCASE by default.
# We need to rename our DataFrame columns before writing.
COLUMN_RENAME_MAP = {
    "id":                            "ID",
    "symbol":                        "SYMBOL",
    "name":                          "NAME",
    "current_price":                 "CURRENT_PRICE",
    "market_cap":                    "MARKET_CAP",
    "market_cap_rank":               "MARKET_CAP_RANK",
    "total_volume":                  "TOTAL_VOLUME",
    "high_24h":                      "HIGH_24H",
    "low_24h":                       "LOW_24H",
    "price_change_24h":              "PRICE_CHANGE_24H",
    "price_change_percentage_24h":   "PRICE_CHANGE_PERCENTAGE_24H",
    "circulating_supply":            "CIRCULATING_SUPPLY",
    "total_supply":                  "TOTAL_SUPPLY",
    "last_updated":                  "LAST_UPDATED",
    "fetched_at":                    "FETCHED_AT",
}

def get_snowflake_connection():
    validate_config()

    logger.info(
        "Connecting to Snowflake | account=%s | user=%s | database=%s | schema=%s",
        SNOWFLAKE_CONFIG["account"],
        SNOWFLAKE_CONFIG["user"],
        SNOWFLAKE_CONFIG["database"],
        SNOWFLAKE_CONFIG["schema"],
    )
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_CONFIG["account"],
        user      = SNOWFLAKE_CONFIG["user"],
        password  = SNOWFLAKE_CONFIG["password"],
        database  = SNOWFLAKE_CONFIG["database"],
        schema    = SNOWFLAKE_CONFIG["schema"],
        warehouse = SNOWFLAKE_CONFIG["warehouse"],
        role      = SNOWFLAKE_CONFIG["role"],
    )

    logger.info("Snowflake connection established successfully")
    return conn


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError(
            "Cannot load an empty DataFrame into Snowflake. "
            "Check that Stage 1 (ingestion) returned data."
        )

    # Check that all expected columns are present
    expected_cols = set(COLUMN_RENAME_MAP.keys())
    actual_cols   = set(df.columns)
    missing_cols  = expected_cols - actual_cols

    if missing_cols:
        raise ValueError(
            f"DataFrame is missing columns required by Snowflake schema: "
            f"{missing_cols}. Check the ingestion script."
        )

    logger.info(
        "Preparing DataFrame for Snowflake | rows=%d | cols=%d",
        len(df), len(df.columns),
    )
    df_copy = df.copy()

    # ── Rename columns to UPPERCASE ───────────────────────
    df_copy = df_copy.rename(columns=COLUMN_RENAME_MAP)
    for col in ["LAST_UPDATED", "FETCHED_AT"]:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], utc=True).dt.tz_localize(None)

    if "MARKET_CAP_RANK" in df_copy.columns:
        df_copy["MARKET_CAP_RANK"] = df_copy["MARKET_CAP_RANK"].astype(float)

    logger.info(
        "DataFrame preparation complete | final shape=%s",
        df_copy.shape,
    )

    return df_copy


# ─────────────────────────────────────────────
# FUNCTION 3 — load_to_snowflake
# ─────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame, conn) -> dict:
    logger.info(
        "Loading %d rows into Snowflake table: %s",
        len(df), RAW_TABLE,
    )
    success, num_chunks, num_rows, output = write_pandas(
        conn              = conn,
        df                = df,
        table_name        = RAW_TABLE,
        database          = SNOWFLAKE_CONFIG["database"],
        schema            = SNOWFLAKE_CONFIG["schema"],
        auto_create_table = False,
        overwrite         = False,
        quote_identifiers = True,
    )

    if not success:
        raise Exception(
            f"write_pandas reported failure loading into {RAW_TABLE}. "
            f"Output: {output}"
        )

    load_stats = {
        "table":      RAW_TABLE,
        "rows_loaded": num_rows,
        "chunks":     num_chunks,
        "success":    success,
        "loaded_at":  datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "Load complete | table=%s | rows=%d | chunks=%d",
        RAW_TABLE, num_rows, num_chunks,
    )

    return load_stats


# ─────────────────────────────────────────────
# FUNCTION 4 — verify_load
# ─────────────────────────────────────────────
def verify_load(conn, expected_rows: int) -> bool:
    logger.info("Verifying load — querying row count in Snowflake …")

    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{RAW_TABLE}
            WHERE FETCHED_AT >= CURRENT_DATE()
        """)

        row_count = cursor.fetchone()[0]

        logger.info(
            "Verification result | expected=%d | found_in_snowflake=%d",
            expected_rows, row_count,
        )

        if row_count >= expected_rows:
            logger.info("Verification PASSED")
            return True
        else:
            logger.warning(
                "Verification WARNING — fewer rows than expected. "
                "Expected: %d | Found: %d",
                expected_rows, row_count,
            )
            return False

    finally:
        cursor.close()


# ─────────────────────────────────────────────
# FUNCTION 5 — run_load  (Airflow entry point)
# ─────────────────────────────────────────────
def run_load(df: pd.DataFrame) -> dict:
    logger.info("━" * 50)
    logger.info("STAGE 2 — Snowflake load started")
    logger.info("━" * 50)
    df_prepared = prepare_dataframe(df)

    conn = None  # Initialise to None so the finally block is always safe

    try:
        # Open the Snowflake connection
        conn = get_snowflake_connection()

        # Write the DataFrame to Snowflake
        load_stats = load_to_snowflake(df_prepared, conn)

        # Verify the data landed correctly
        verify_load(conn, expected_rows=len(df_prepared))

        logger.info(
            "STAGE 2 complete | %d rows loaded into %s",
            load_stats["rows_loaded"], RAW_TABLE,
        )

        return load_stats

    except Exception as exc:
        logger.error(
            "STAGE 2 FAILED — error loading into Snowflake: %s",
            exc, exc_info=True,
            
        )
        raise  

    finally:
        
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")


# ─────────────────────────────────────────────
# MAIN BLOCK — standalone test runner
# ─────────────────────────────────────────────
if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Import Stage 1 to get real data for the test
    from src.ingestion.coingecko_ingest import run_ingestion

    print("\nRunning Stage 1 to get data …\n")
    df = run_ingestion(coins_limit=10)

    print("\nRunning Stage 2 — loading into Snowflake …\n")
    stats = run_load(df)

    print("\n" + "═" * 60)
    print(" LOAD STATISTICS")
    print("═" * 60)
    for key, value in stats.items():
        print(f"  {key:<20} {value}")
    print("═" * 60)