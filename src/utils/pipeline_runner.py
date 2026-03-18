"""
pipeline_runner.py
------------------
Runs all pipeline stages end-to-end with full logging,
error handling, timing, and a summary report.
"""

import sys
import logging
import time
from datetime import datetime, timezone

# ── Internal imports ──────────────────────────────────────
from config.logger   import setup_logging
from config.settings import validate_config, LOG_LEVEL

from src.ingestion.coingecko_ingest       import run_ingestion
from src.storage.snowflake_loader         import run_load
from src.transformation.transform_queries import run_transformations
from src.utils.exceptions import (
    CryptoPipelineError,
    ConfigError,
    IngestionError,
    StorageError,
    TransformError,
)


# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPER — run a stage with timing and error handling
# ─────────────────────────────────────────────
def run_stage(stage_name: str, func, error_class, *args, **kwargs):
    """
    Runs a pipeline stage function and handles errors uniformly.
    """

    logger.info("━" * 50)
    logger.info("Starting %s", stage_name)
    logger.info("━" * 50)

    start_time = time.time()

    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time

        logger.info(
            "%s completed successfully in %.2f seconds",
            stage_name, elapsed,
        )
        return result

    except CryptoPipelineError:
        # Already the right type — re-raise as-is
        raise

    except Exception as exc:
        elapsed = time.time() - start_time
        logger.error(
            "%s FAILED after %.2f seconds | error: %s",
            stage_name, elapsed, exc,
            exc_info=True,
        )
        # Wrap the raw exception in our custom type
        # so callers know exactly which stage failed
        raise error_class(
            f"{stage_name} failed: {exc}"
        ) from exc


# ─────────────────────────────────────────────
# MAIN PIPELINE RUNNER
# ─────────────────────────────────────────────
def run_pipeline() -> dict:
    """
    Runs all three pipeline stages in sequence.
    """

    pipeline_start = time.time()
    run_timestamp  = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("CRYPTO PIPELINE RUN STARTED")
    logger.info("Timestamp: %s", run_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("=" * 60)

    summary = {
        "run_timestamp":  run_timestamp.isoformat(),
        "status":         "FAILED",   # default — updated on success
        "stages":         {},
        "rows_ingested":  0,
        "rows_loaded":    0,
        "total_duration": 0,
        "error":          None,
    }

    try:
        logger.info("Validating configuration …")
        try:
            validate_config()
            logger.info("Configuration valid ✓")
        except EnvironmentError as exc:
            raise ConfigError(str(exc)) from exc

        # ── Stage 1: Ingestion ────────────────────────────
        stage1_start = time.time()
        df = run_stage(
            stage_name  = "Stage 1 — CoinGecko Ingestion",
            func        = run_ingestion,
            error_class = IngestionError,
        )
        stage1_duration = time.time() - stage1_start

        summary["rows_ingested"] = len(df)
        summary["stages"]["ingestion"] = {
            "status":   "SUCCESS",
            "rows":     len(df),
            "duration": round(stage1_duration, 2),
        }

        # ── Stage 2: Load ─────────────────────────────────
        stage2_start = time.time()
        load_stats = run_stage(
            stage_name  = "Stage 2 — Snowflake Load",
            func        = run_load,
            error_class = StorageError,
            df          = df,
        )
        stage2_duration = time.time() - stage2_start

        summary["rows_loaded"] = load_stats.get("rows_loaded", 0)
        summary["stages"]["storage"] = {
            "status":      "SUCCESS",
            "rows_loaded": load_stats.get("rows_loaded", 0),
            "table":       load_stats.get("table", ""),
            "duration":    round(stage2_duration, 2),
        }

        # ── Stage 3: Transform ────────────────────────────
        stage3_start = time.time()
        run_stage(
            stage_name  = "Stage 3 — SQL Transformations",
            func        = run_transformations,
            error_class = TransformError,
        )
        stage3_duration = time.time() - stage3_start

        summary["stages"]["transformation"] = {
            "status":   "SUCCESS",
            "duration": round(stage3_duration, 2),
        }

        # ── Mark as success ───────────────────────────────
        summary["status"] = "SUCCESS"

    except CryptoPipelineError as exc:
        summary["status"] = "FAILED"
        summary["error"]  = str(exc)
        logger.error("Pipeline run FAILED: %s", exc)

    finally:
        total_duration = time.time() - pipeline_start
        summary["total_duration"] = round(total_duration, 2)

        _print_summary(summary)

    return summary


# ─────────────────────────────────────────────
# SUMMARY REPORT
# ─────────────────────────────────────────────
def _print_summary(summary: dict) -> None:

    status_symbol = "✓" if summary["status"] == "SUCCESS" else "✗"

    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY  %s %s", status_symbol, summary["status"])
    logger.info("=" * 60)
    logger.info("Timestamp    : %s", summary["run_timestamp"])
    logger.info("Total time   : %.2f seconds", summary["total_duration"])
    logger.info("Rows ingested: %d", summary["rows_ingested"])
    logger.info("Rows loaded  : %d", summary["rows_loaded"])
    logger.info("")

    for stage, info in summary.get("stages", {}).items():
        status = info.get("status", "UNKNOWN")
        duration = info.get("duration", 0)
        symbol = "✓" if status == "SUCCESS" else "✗"
        logger.info("  %s %-15s %s  (%.2fs)", symbol, stage, status, duration)

    if summary.get("error"):
        logger.error("")
        logger.error("Error: %s", summary["error"])

    logger.info("=" * 60)
    logger.info("")


# ─────────────────────────────────────────────
# MAIN BLOCK
# ─────────────────────────────────────────────
if __name__ == "__main__":

    setup_logging(log_level="INFO")

    logger.info("Starting crypto pipeline runner …")

    summary = run_pipeline()
    if summary["status"] != "SUCCESS":
        sys.exit(1)

    sys.exit(0)