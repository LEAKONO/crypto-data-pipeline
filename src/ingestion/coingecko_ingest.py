"""
coingecko_ingest.py
-------------------
Stage 1 of the crypto_pipeline.
It does ONE job: fetch and clean.)
"""

# ─────────────────────────────────────────────
# STANDARD LIBRARY IMPORTS
# ─────────────────────────────────────────────
import logging
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# THIRD-PARTY IMPORTS
# ─────────────────────────────────────────────
import requests
import pandas as pd
import sys
from pathlib import Path

# Add project root to sys.path so 'config' can be imported
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# ─────────────────────────────────────────────
# INTERNAL IMPORTS
# ─────────────────────────────────────────────
# Import our central config so settings live in one place
from config.settings import COINGECKO_CONFIG


# ─────────────────────────────────────────────
# MODULE-LEVEL LOGGER
# ─────────────────────────────────────────────
# back to their source when running the full pipeline
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
BASE_URL            = COINGECKO_CONFIG["base_url"]
DEFAULT_LIMIT       = COINGECKO_CONFIG["coins_limit"]
DEFAULT_CURRENCY    = COINGECKO_CONFIG["vs_currency"]

MAX_RETRIES         = 3
RETRY_DELAY_SECONDS = 5

# The exact API fields we want to keep.
# CoinGecko returns ~30+ fields per coin — we take only what we need.
FIELDS_TO_EXTRACT = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "last_updated",
]


# ─────────────────────────────────────────────
# FUNCTION 1 — fetch_market_data
# ─────────────────────────────────────────────
def fetch_market_data(
    vs_currency: str = DEFAULT_CURRENCY,
    coins_limit: int = DEFAULT_LIMIT,
) -> list[dict]:

    url = f"{BASE_URL}/coins/markets"

    params = {
        "vs_currency":            vs_currency,
        "order":                  "market_cap_desc",
        "per_page":               coins_limit,
        "page":                   1,
        "sparkline":              False,
        "price_change_percentage": "24h",
    }

    # ── Retry loop ──────────────────────────────────────
    # Networks fail. APIs go down. Always retry with a delay.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "CoinGecko fetch | attempt %d/%d | coins=%d | currency=%s",
                attempt, MAX_RETRIES, coins_limit, vs_currency,
            )

            response = requests.get(url, params=params, timeout=30)

            # Raises HTTPError for 4xx / 5xx responses
            response.raise_for_status()

            data = response.json()

            if not data:
                raise ValueError("CoinGecko returned an empty payload")

            logger.info("Fetched %d coin records from CoinGecko", len(data))
            return data

        except requests.exceptions.Timeout:
            logger.warning("Attempt %d: request timed out", attempt)

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else "unknown"
            logger.warning("Attempt %d: HTTP %s — %s", attempt, status, exc)
            # Rate-limited: back off longer
            if status == 429:
                logger.warning("Rate-limited. Sleeping 60 s before retry.")
                time.sleep(60)
                continue

        except requests.exceptions.ConnectionError:
            logger.warning("Attempt %d: connection error (no internet?)", attempt)

        except ValueError as exc:
            logger.warning("Attempt %d: bad response — %s", attempt, exc)

        except Exception as exc:
            logger.warning("Attempt %d: unexpected error — %s", attempt, exc)

        # Wait before next attempt (skip wait after the final attempt)
        if attempt < MAX_RETRIES:
            logger.info("Waiting %d s before retry …", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)

    raise Exception(
        f"CoinGecko fetch failed after {MAX_RETRIES} attempts. "
        "Check your internet connection or CoinGecko's status page."
    )


# ─────────────────────────────────────────────
# FUNCTION 2 — clean_coin_data
# ─────────────────────────────────────────────
def clean_coin_data(raw_data: list[dict]) -> pd.DataFrame:

    logger.info("Cleaning %d raw coin records …", len(raw_data))

    # ── Step 1: extract selected fields ──────────────────
    # dict.get() returns None (not KeyError) when a field is absent
    records = [
        {field: coin.get(field) for field in FIELDS_TO_EXTRACT}
        for coin in raw_data
    ]

    df = pd.DataFrame(records)
    logger.debug("Raw DataFrame shape: %s", df.shape)

    # ── Step 2: cast numeric columns ─────────────────────
    float_cols = [
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "circulating_supply",
        "total_supply",
    ]
    for col in float_cols:
        
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Int64 (capital I) supports pd.NA — regular int64 does not
    df["market_cap_rank"] = (
        pd.to_numeric(df["market_cap_rank"], errors="coerce")
        .astype("Int64")
    )

    # ── Step 3: clean string columns ─────────────────────
    df["id"]     = df["id"].str.strip().str.lower()
    df["symbol"] = df["symbol"].str.strip().str.upper()
    df["name"]   = df["name"].str.strip()

  
    df["last_updated"] = pd.to_datetime(
        df["last_updated"], utc=True, errors="coerce"
    )
    df["fetched_at"] = datetime.now(timezone.utc)

    # ── Step 6: drop rows with no coin id ─────────────────
    before = len(df)
    df = df.dropna(subset=["id"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with null id", dropped)

    logger.info(
        "Cleaning complete | rows=%d | cols=%d",
        len(df), len(df.columns),
    )

    return df


# ─────────────────────────────────────────────
# FUNCTION 3 — run_ingestion  (Airflow entry point)
# ─────────────────────────────────────────────
def run_ingestion(
    vs_currency: str = DEFAULT_CURRENCY,
    coins_limit: int = DEFAULT_LIMIT,
) -> pd.DataFrame:

    logger.info("━" * 50)
    logger.info("STAGE 1 — CoinGecko ingestion started")
    logger.info("━" * 50)

    raw  = fetch_market_data(vs_currency=vs_currency, coins_limit=coins_limit)
    df   = clean_coin_data(raw)

    logger.info(
        "STAGE 1 complete | %d rows | %d columns",
        len(df), len(df.columns),
    )

    return df


# ─────────────────────────────────────────────
# MAIN BLOCK — standalone test runner
# ─────────────────────────────────────────────

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print("\nRunning Stage 1 ingestion test (10 coins) …\n")
    df = run_ingestion(coins_limit=10)

    print("\n" + "═" * 60)
    print(" SAMPLE — first 5 rows")
    print("═" * 60)
    print(df[["id", "symbol", "current_price", "market_cap_rank"]].head())

    print("\n" + "═" * 60)
    print(" DATA TYPES")
    print("═" * 60)
    print(df.dtypes)

    print("\n" + "═" * 60)
    print(" NULL COUNTS")
    print("═" * 60)
    print(df.isnull().sum())

    print("\n" + "═" * 60)
    print(f" TOTAL ROWS: {len(df)} | TOTAL COLUMNS: {len(df.columns)}")
    print("═" * 60)