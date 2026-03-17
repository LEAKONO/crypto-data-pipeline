"""
transform_queries.py
--------------------
Stage 3 of the crypto_pipeline.

Responsibility:
    Read the SQL transformation files and execute them
    against Snowflake in the correct order:
        1. staging.sql   — RAW → STAGING
        2. analytics.sql — STAGING → ANALYTICS
"""

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import logging
from pathlib import Path

from config.settings import SNOWFLAKE_CONFIG, SNOWFLAKE_TABLES
from src.storage.snowflake_loader import get_snowflake_connection

# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# PATHS TO SQL FILES
# ─────────────────────────────────────────────

SQL_DIR = Path(__file__).resolve().parent / "sql"

STAGING_SQL_PATH   = SQL_DIR / "staging.sql"
ANALYTICS_SQL_PATH = SQL_DIR / "analytics.sql"


# ─────────────────────────────────────────────
# FUNCTION 1 — load_sql_file
# ─────────────────────────────────────────────
def load_sql_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {path}\n"
            f"Make sure the file exists at: {path}"
        )

    logger.info("Loading SQL file: %s", path.name)
    return path.read_text(encoding="utf-8")


# ─────────────────────────────────────────────
# FUNCTION 2 — execute_transformation
# ─────────────────────────────────────────────
def execute_transformation(conn, sql: str, label: str) -> None:
    logger.info("━" * 40)
    logger.info("Running transformation: %s", label.upper())
    logger.info("━" * 40)

    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            logger.info("Result: %s", result[0])

        logger.info("Transformation complete: %s ✓", label.upper())

    except Exception as exc:
        logger.error(
            "Transformation FAILED: %s | error: %s",
            label.upper(), exc,
            exc_info=True,
        )
        raise

    finally:
        cursor.close()


# ─────────────────────────────────────────────
# FUNCTION 3 — verify_transformations
# ─────────────────────────────────────────────
def verify_transformations(conn) -> None:
    logger.info("Verifying transformation results …")

    checks = [
        (
            "STAGING",
            f"SELECT COUNT(*) FROM CRYPTO_DB.STAGING.{SNOWFLAKE_TABLES['staging']}",
        ),
        (
            "ANALYTICS",
            f"SELECT COUNT(*) FROM CRYPTO_DB.ANALYTICS.{SNOWFLAKE_TABLES['analytics']}",
        ),
    ]

    cursor = conn.cursor()

    try:
        for label, query in checks:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            logger.info(
                "Verification | %s table | rows=%d",
                label, count,
            )
            if count == 0:
                logger.warning(
                    "%s table has 0 rows — transformation may have "
                    "filtered everything out. Check your RAW data.",
                    label,
                )

    finally:
        cursor.close()


# ─────────────────────────────────────────────
# FUNCTION 4 — run_transformations (Airflow entry point)
# ─────────────────────────────────────────────
def run_transformations() -> None:
    logger.info("━" * 50)
    logger.info("STAGE 3 — SQL transformations started")
    logger.info("━" * 50)

    staging_sql   = load_sql_file(STAGING_SQL_PATH)
    analytics_sql = load_sql_file(ANALYTICS_SQL_PATH)

    conn = None

    try:
        conn = get_snowflake_connection()
        execute_transformation(
            conn  = conn,
            sql   = staging_sql,
            label = "staging",
        )
        execute_transformation(
            conn  = conn,
            sql   = analytics_sql,
            label = "analytics",
        )

        verify_transformations(conn)

        logger.info("STAGE 3 complete ✓")

    except Exception as exc:
        logger.error(
            "STAGE 3 FAILED: %s",
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

    print("\nRunning Stage 3 — SQL transformations …\n")
    run_transformations()