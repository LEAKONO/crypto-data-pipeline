"""
settings.py
-----------
Central configuration for the entire crypto_pipeline project.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# LOAD ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
# Find the project root directory (two levels up from this file)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load the .env file from the project root
# This makes all variables in .env available via os.getenv()
load_dotenv(PROJECT_ROOT / ".env")


# ─────────────────────────────────────────────
# SNOWFLAKE SETTINGS
# ─────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "CRYPTO_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "role":      os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN"),
}

# ─────────────────────────────────────────────
# COINGECKO API SETTINGS
# ─────────────────────────────────────────────
COINGECKO_CONFIG = {
    "base_url":    os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
    "coins_limit": int(os.getenv("COINGECKO_COINS_LIMIT", "100")),
    "vs_currency": os.getenv("COINGECKO_VS_CURRENCY", "usd"),
}


# ─────────────────────────────────────────────
# PIPELINE SETTINGS
# ─────────────────────────────────────────────
PIPELINE_ENV = os.getenv("PIPELINE_ENV", "development")
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")

# Paths
LOG_DIR  = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"

# Snowflake table names — centralized so changing a table name
# doesn't require hunting through multiple files
SNOWFLAKE_TABLES = {
    "raw":       "RAW_CRYPTO_MARKET_DATA",
    "staging":   "STG_CRYPTO_MARKET_DATA",
    "analytics": "ANALYTICS_DAILY_CRYPTO_SUMMARY",
}


# ─────────────────────────────────────────────
# VALIDATION — catch missing credentials early
# ─────────────────────────────────────────────
def validate_config():
    """
    Call this at pipeline startup to ensure all required
    environment variables are set. Fail fast with a clear
    message rather than cryptic errors later.
    """
    required = [
        ("SNOWFLAKE_ACCOUNT",   SNOWFLAKE_CONFIG["account"]),
        ("SNOWFLAKE_USER",      SNOWFLAKE_CONFIG["user"]),
        ("SNOWFLAKE_PASSWORD",  SNOWFLAKE_CONFIG["password"]),
    ]

    missing = [name for name, value in required if not value]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Please check your .env file at: {PROJECT_ROOT / '.env'}"
        )