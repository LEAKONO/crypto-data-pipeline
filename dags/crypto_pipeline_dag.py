"""
crypto_pipeline_dag.py
----------------------
"""
import sys
import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
from src.ingestion.coingecko_ingest       import run_ingestion
from src.storage.snowflake_loader         import run_load
from src.transformation.transform_queries import run_transformations
logger = logging.getLogger(__name__)
default_args = {
    "owner":            "data_engineer",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
    "depends_on_past":  False,
}
def task_ingest_fn(**context):
    logger.info("Task 1 starting: CoinGecko ingestion")
    df = run_ingestion()
    logger.info("Task 1 complete: fetched %d rows | pushing to XCom", len(df))
    context["ti"].xcom_push(
        key   = "raw_dataframe",
        value = df.to_json(date_format="iso"),
    )
def task_load_fn(**context):
    import pandas as pd
    logger.info("Task 2 starting: Snowflake load")
    df_json = context["ti"].xcom_pull(
        task_ids = "task_ingest",
        key      = "raw_dataframe",
    )

    if not df_json:
        raise ValueError(
        )
    df = pd.read_json(df_json)

    logger.info("Task 2: received DataFrame with %d rows from XCom", len(df))

    stats = run_load(df)

    logger.info(
        "Task 2 complete: loaded %d rows into Snowflake",
        stats["rows_loaded"],
    )


def task_transform_fn(**context):
    logger.info("Task 3 starting: SQL transformations")

    run_transformations()

    logger.info("Task 3 complete: STAGING and ANALYTICS tables refreshed")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id            = "crypto_pipeline",
    description       = "Daily crypto market data pipeline: "
                        "CoinGecko API → Snowflake RAW → STAGING → ANALYTICS",
    default_args      = default_args,
    start_date        = datetime(2026, 3, 18),
    schedule_interval = "0 8 * * *",
    catchup           = False,
    tags              = ["crypto", "snowflake", "coingecko"],
) as dag:

    task_ingest = PythonOperator(
        task_id         = "task_ingest",
        python_callable = task_ingest_fn,
        provide_context = True,
    )

    task_load = PythonOperator(
        task_id         = "task_load",
        python_callable = task_load_fn,
        provide_context = True,
    )

    task_transform = PythonOperator(
        task_id         = "task_transform",
        python_callable = task_transform_fn,
        provide_context = True,
    )

    # If any task fails the chain stops
    task_ingest >> task_load >> task_transform