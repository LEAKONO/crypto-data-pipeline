"""
snowflake_loader.py
-------------------
Stage 2 of the crypto_pipeline.

Responsibility:
    Take the clean pandas DataFrame produced by Stage 1 and
    load it into the RAW_CRYPTO_MARKET_DATA table in Snowflake.

    This module also handles first-time setup automatically —
    it creates the database, schemas, warehouse, and table
    if they do not already exist.

Usage (standalone test):
    python3 -m src.storage.snowflake_loader

Usage (imported by Airflow or another script):
    from src.storage.snowflake_loader import run_load
    run_load(df)
"""

# ─────────────────────────────────────────────
# STANDARD LIBRARY IMPORTS
# ─────────────────────────────────────────────
import logging
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# THIRD-PARTY IMPORTS
# ─────────────────────────────────────────────
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─────────────────────────────────────────────
# INTERNAL IMPORTS
# ─────────────────────────────────────────────
from config.settings import SNOWFLAKE_CONFIG, SNOWFLAKE_TABLES, validate_config


# ─────────────────────────────────────────────
# MODULE-LEVEL LOGGER
# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
RAW_TABLE = SNOWFLAKE_TABLES["raw"]  # "RAW_CRYPTO_MARKET_DATA"

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

# The full DDL for the RAW table
# Defined here so setup_snowflake_environment() can create it
# automatically without needing any manual SQL steps
CREATE_RAW_TABLE_SQL = """
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
"""


# ─────────────────────────────────────────────
# FUNCTION 1 — get_bootstrap_connection
# ─────────────────────────────────────────────
def get_bootstrap_connection():
    """
    Create a minimal Snowflake connection WITHOUT specifying
    a database or schema.

    Why a separate bootstrap connection?
      When we connect normally, Snowflake tries to activate
      CRYPTO_DB immediately. If it doesn't exist yet, the
      connection itself fails before we can create it.

      The bootstrap connection connects at the account level
      only — no database, no schema — so we can safely run
      CREATE DATABASE and CREATE SCHEMA commands.

    Returns:
        snowflake.connector.SnowflakeConnection
    """

    logger.info("Opening bootstrap connection (no database selected) …")

    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_CONFIG["account"],
        user      = SNOWFLAKE_CONFIG["user"],
        password  = SNOWFLAKE_CONFIG["password"],
        warehouse = SNOWFLAKE_CONFIG["warehouse"],
        role      = SNOWFLAKE_CONFIG["role"],
        # NOTE: No database or schema here intentionally
        # We cannot specify them because they may not exist yet
    )

    return conn


# ─────────────────────────────────────────────
# FUNCTION 2 — setup_snowflake_environment
# ─────────────────────────────────────────────
def setup_snowflake_environment():
    """
    Automatically create all Snowflake objects if they do not
    already exist:
        - Warehouse (COMPUTE_WH)
        - Database  (CRYPTO_DB)
        - Schemas   (RAW, STAGING, ANALYTICS)
        - Table     (RAW_CRYPTO_MARKET_DATA)

    This runs at the START of every pipeline execution.
    Because every statement uses IF NOT EXISTS, it is
    completely safe to run repeatedly — it only creates
    objects that are missing, never overwrites existing ones.

    Why automate this?
      No manual Snowflake worksheet steps required.
      The pipeline is fully self-contained and portable.
      A new team member can clone the repo and run immediately.
    """

    logger.info("Setting up Snowflake environment …")

    conn = get_bootstrap_connection()
    cursor = conn.cursor()

    try:
        # ── Step 1: Activate the role ─────────────────────
        logger.info("Activating role: %s", SNOWFLAKE_CONFIG["role"])
        cursor.execute(f"USE ROLE {SNOWFLAKE_CONFIG['role']}")

        # ── Step 2: Create warehouse if missing ───────────
        # The warehouse must exist before we can run any DDL
        logger.info("Ensuring warehouse exists: %s", SNOWFLAKE_CONFIG["warehouse"])
        cursor.execute(f"""
            CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_CONFIG['warehouse']}
                WAREHOUSE_SIZE = 'X-SMALL'
                AUTO_SUSPEND   = 60
                AUTO_RESUME    = TRUE
        """)
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")

        # ── Step 3: Create database if missing ────────────
        logger.info("Ensuring database exists: %s", SNOWFLAKE_CONFIG["database"])
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}"
        )
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")

        # ── Step 4: Create all three schemas if missing ───
        for schema in ["RAW", "STAGING", "ANALYTICS"]:
            logger.info("Ensuring schema exists: %s", schema)
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # ── Step 5: Create the RAW table if missing ───────
        logger.info("Ensuring table exists: %s", RAW_TABLE)
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['database']}.RAW")
        cursor.execute(CREATE_RAW_TABLE_SQL)

        logger.info("Snowflake environment setup complete ✓")

    except Exception as exc:
        logger.error("Failed to set up Snowflake environment: %s", exc)
        raise

    finally:
        cursor.close()
        conn.close()
        logger.info("Bootstrap connection closed")


# ─────────────────────────────────────────────
# FUNCTION 3 — get_snowflake_connection
# ─────────────────────────────────────────────
def get_snowflake_connection():
    """
    Create and return a full Snowflake connection with
    database, schema, and warehouse explicitly activated.

    Called AFTER setup_snowflake_environment() has confirmed
    all objects exist — so this connection is always safe.

    Returns:
        snowflake.connector.SnowflakeConnection
    """

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

    # Explicitly activate session context with USE commands
    # This guarantees the session is correctly scoped
    # even if the connector did not activate them automatically
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE ROLE      {SNOWFLAKE_CONFIG['role']}")
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        cursor.execute(f"USE DATABASE  {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(
            f"USE SCHEMA {SNOWFLAKE_CONFIG['database']}."
            f"{SNOWFLAKE_CONFIG['schema']}"
        )
        logger.info(
            "Session context set | role=%s | warehouse=%s | database=%s | schema=%s",
            SNOWFLAKE_CONFIG["role"],
            SNOWFLAKE_CONFIG["warehouse"],
            SNOWFLAKE_CONFIG["database"],
            SNOWFLAKE_CONFIG["schema"],
        )
    finally:
        cursor.close()

    logger.info("Snowflake connection established successfully")
    return conn


# ─────────────────────────────────────────────
# FUNCTION 4 — prepare_dataframe
# ─────────────────────────────────────────────
def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the DataFrame for Snowflake ingestion.

    Steps:
      1. Validate the DataFrame has rows and required columns
      2. Make a copy so we never mutate the original
      3. Rename columns from lowercase to UPPERCASE
      4. Convert timezone-aware datetimes to timezone-naive UTC
      5. Convert nullable Int64 to float (Snowflake compatible)

    Args:
        df: Clean DataFrame from Stage 1

    Returns:
        pd.DataFrame: Copy ready to write to Snowflake

    Raises:
        ValueError: If the DataFrame is empty or missing columns
    """

    if df is None or df.empty:
        raise ValueError(
            "Cannot load an empty DataFrame into Snowflake. "
            "Check that Stage 1 (ingestion) returned data."
        )

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
    df_copy = df_copy.rename(columns=COLUMN_RENAME_MAP)

    for col in ["LAST_UPDATED", "FETCHED_AT"]:
        if col in df_copy.columns:
            df_copy[col] = (
                pd.to_datetime(df_copy[col], utc=True)
                .dt.tz_localize(None)
            )

    if "MARKET_CAP_RANK" in df_copy.columns:
        df_copy["MARKET_CAP_RANK"] = df_copy["MARKET_CAP_RANK"].astype(float)

    logger.info("DataFrame preparation complete | final shape=%s", df_copy.shape)

    return df_copy


# ─────────────────────────────────────────────
# FUNCTION 5 — load_to_snowflake
# ─────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame, conn) -> dict:
    """
    Write the prepared DataFrame into the RAW Snowflake table
    using write_pandas — Snowflake's official bulk-load method.

    Args:
        df:   Prepared DataFrame from prepare_dataframe()
        conn: Live Snowflake connection

    Returns:
        dict: Load statistics

    Raises:
        Exception: If the write operation fails
    """

    logger.info("Loading %d rows into Snowflake table: %s", len(df), RAW_TABLE)

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
        "table":       RAW_TABLE,
        "rows_loaded": num_rows,
        "chunks":      num_chunks,
        "success":     success,
        "loaded_at":   datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "Load complete | table=%s | rows=%d | chunks=%d",
        RAW_TABLE, num_rows, num_chunks,
    )

    return load_stats


# ─────────────────────────────────────────────
# FUNCTION 6 — verify_load
# ─────────────────────────────────────────────
def verify_load(conn, expected_rows: int) -> bool:
    """
    Run a COUNT query to confirm data landed in Snowflake.

    Args:
        conn:          Live Snowflake connection
        expected_rows: Number of rows we attempted to load

    Returns:
        bool: True if row count matches expected
    """

    logger.info("Verifying load — querying row count in Snowflake …")

    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM   {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{RAW_TABLE}
            WHERE  FETCHED_AT >= CURRENT_DATE()
        """)

        row_count = cursor.fetchone()[0]

        logger.info(
            "Verification result | expected=%d | found_in_snowflake=%d",
            expected_rows, row_count,
        )

        if row_count >= expected_rows:
            logger.info("Verification PASSED ✓")
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
# FUNCTION 7 — run_load  (Airflow entry point)
# ─────────────────────────────────────────────
def run_load(df: pd.DataFrame) -> dict:
    """
    Orchestrates the full Stage 2 process:
        1. Auto-setup Snowflake environment if needed
        2. Prepare the DataFrame
        3. Connect to Snowflake
        4. Write data
        5. Verify data landed
        6. Close connection

    This is the single function Apache Airflow calls.

    Args:
        df: Clean DataFrame from Stage 1

    Returns:
        dict: Load statistics
    """

    logger.info("━" * 50)
    logger.info("STAGE 2 — Snowflake load started")
    logger.info("━" * 50)

    # ── Step 1: Auto-create all Snowflake objects if missing ──
    # This is safe to run every time — IF NOT EXISTS means
    # it skips creation of anything that already exists
    validate_config()
    setup_snowflake_environment()

    # ── Step 2: Prepare the DataFrame ─────────────────────
    df_prepared = prepare_dataframe(df)

    conn = None

    try:
        # ── Step 3: Open the full connection ──────────────
        conn = get_snowflake_connection()

        # ── Step 4: Write to Snowflake ────────────────────
        load_stats = load_to_snowflake(df_prepared, conn)

        # ── Step 5: Verify data landed ────────────────────
        verify_load(conn, expected_rows=len(df_prepared))

        logger.info(
            "STAGE 2 complete | %d rows loaded into %s",
            load_stats["rows_loaded"], RAW_TABLE,
        )

        return load_stats

    except Exception as exc:
        logger.error(
            "STAGE 2 FAILED — error loading into Snowflake: %s",
            exc,
            exc_info=True,
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


