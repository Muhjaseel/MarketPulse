"""
MarketPulse master orchestration DAG.

Every 5 minutes:
  1. build_staging_views     -> dbt run --select stg_*
  2. assert_data_contracts   -> Great Expectations gate over the staging
                                 views just built (BranchPythonOperator)
       success -> compile_analytics_marts -> dbt run --select int_* fact_* dim_*
       failure -> quarantine_corrupt_batch -> alert + halt before marts

All filesystem locations are resolved from environment variables set on
the Airflow container (see docker/docker-compose.yaml) so this file has
zero machine-specific hardcoding and runs unchanged in any environment
that sets AIRFLOW_HOME / DBT_PROJECT_DIR / DBT_PROFILES_DIR.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Dynamic, environment-driven paths — nothing below is hardcoded.
# ---------------------------------------------------------------------
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", os.path.join(AIRFLOW_HOME, "transform_dbt"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

# Make the mounted `src/` package importable (docker-compose mounts it to
# $AIRFLOW_HOME/src, and PYTHONPATH=$AIRFLOW_HOME is set on the container).
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

TASK_ID_COMPILE_MARTS = "compile_analytics_marts"
TASK_ID_QUARANTINE = "quarantine_corrupt_batch"

DEFAULT_ARGS = {
    "owner": "marketpulse-data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}


def _dbt_command(select_clause: str) -> str:
    """Build a dbt invocation rooted at the environment-resolved project dir."""
    return (
        f'cd "{DBT_PROJECT_DIR}" && '
        f'dbt run --select {select_clause} --profiles-dir "{DBT_PROFILES_DIR}"'
    )


def _assert_data_contracts(**context) -> str:
    """
    Pull the freshly-built staging views out of DuckDB and run Great
    Expectations checks against them. Returns the downstream task_id to
    branch into, and never lets a validation problem crash the DAG run —
    any failure (missing view, lock contention, GX exception) is treated
    as a data-quality failure and routed to quarantine rather than raised.
    """
    ti = context["ti"]

    try:
        from src.quality.gx_validation import dq_gate
        from src.utils.duckdb_client import query_dataframe

        market_df = query_dataframe("SELECT * FROM stg_market_prices")
        sentiment_df = query_dataframe("SELECT * FROM stg_sentiment")

        market_ok, market_details = dq_gate.validate_staging_market_prices(market_df)
        sentiment_ok, sentiment_details = dq_gate.validate_staging_sentiment(sentiment_df)

        overall_success = market_ok and sentiment_ok
        details = {
            "market_prices": market_details,
            "social_sentiment": sentiment_details,
            "market_rows": len(market_df),
            "sentiment_rows": len(sentiment_df),
        }

        logger.info(
            "Data contract assertion result: success=%s details=%s",
            overall_success,
            details,
        )
        ti.xcom_push(key="quality_report", value=details)

    except Exception as exc:  # noqa: BLE001 - any failure here means "quarantine", not "crash"
        logger.exception("Data contract assertion raised an exception; quarantining batch.")
        ti.xcom_push(key="quality_report", value={"error": str(exc)})
        return TASK_ID_QUARANTINE

    return TASK_ID_COMPILE_MARTS if overall_success else TASK_ID_QUARANTINE


def _quarantine_corrupt_batch(**context) -> None:
    """
    Terminal alerting sink for failed data-contract checks. Logs the
    quality report at CRITICAL level so it surfaces in monitoring/alerting
    without ever letting the corrupted batch reach the analytics marts.
    """
    ti = context["ti"]
    quality_report = ti.xcom_pull(task_ids="assert_data_contracts", key="quality_report")
    logger.critical(
        "🚨 MarketPulse data-quality gate FAILED — quarantining batch instead of "
        "compiling analytics marts. Report: %s",
        quality_report,
    )
    # Extension point: page on-call, write to a quarantine table, publish to
    # a dead-letter topic, etc. Kept as a log-based alert here so the DAG
    # has no hard dependency on an external notification service.


with DAG(
    dag_id="marketpulse_master_pipeline",
    description="MarketPulse ingestion -> staging -> data-contract gate -> analytics marts",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # avoid concurrent runs contending for the DuckDB file lock
    tags=["marketpulse", "dbt", "great_expectations"],
) as dag:

    build_staging_views = BashOperator(
        task_id="build_staging_views",
        bash_command=_dbt_command("stg_*"),
    )

    assert_data_contracts = BranchPythonOperator(
        task_id="assert_data_contracts",
        python_callable=_assert_data_contracts,
    )

    compile_analytics_marts = BashOperator(
        task_id=TASK_ID_COMPILE_MARTS,
        bash_command=_dbt_command("int_* fact_* dim_*"),
    )

    quarantine_corrupt_batch = PythonOperator(
        task_id=TASK_ID_QUARANTINE,
        python_callable=_quarantine_corrupt_batch,
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    build_staging_views >> assert_data_contracts
    assert_data_contracts >> [compile_analytics_marts, quarantine_corrupt_batch]
    [compile_analytics_marts, quarantine_corrupt_batch] >> pipeline_complete
