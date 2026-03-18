"""
logger.py
---------
Central logging configuration for the entire crypto_pipeline project.
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime

# Project root is: crypto_pipeline/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR      = PROJECT_ROOT / "logs"

# Create the logs/ directory if it does not exist
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMAT    = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT   = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    log_level: str = "INFO",
    log_filename: str = None,
) -> None:
    if log_filename is None:
        today = datetime.now().strftime("%Y-%m-%d")
        log_filename = f"pipeline_{today}.log"

    log_filepath = LOG_DIR / log_filename
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(numeric_level)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename    = log_filepath,
        when        = "midnight",   # rotate at midnight every day
        interval    = 1,            # every 1 day
        backupCount = 30,           # keep 30 days of history
        encoding    = "utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

  
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    if not root_logger.handlers:
        root_logger.addHandler(stream_handler)
        root_logger.addHandler(file_handler)
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialised | level=%s | file=%s",
        log_level, log_filepath,
    )