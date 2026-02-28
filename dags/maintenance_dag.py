"""Airflow DAG for weekly maintenance tasks."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sports-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def vacuum_duckdb(**context):
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    loader = DuckDBLoader()
    loader.execute("VACUUM")


def recalibrate_models(**context):
    from sports_pipeline.analytics.calibration import IsotonicCalibrator
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader

    loader = DuckDBLoader()
    try:
        resolved = loader.query("""
            SELECT model_prob, CAST(actual_outcome AS INTEGER) as outcome
            FROM gold.edge_signals
            WHERE resolved = TRUE AND actual_outcome IS NOT NULL
        """)
        if not resolved.empty and len(resolved) >= 20:
            calibrator = IsotonicCalibrator()
            calibrator.fit(
                resolved["model_prob"].tolist(),
                resolved["outcome"].tolist(),
            )
    except Exception as e:
        print(f"Recalibration error: {e}")


def cleanup_old_snapshots(**context):
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    loader = DuckDBLoader()
    try:
        loader.execute("""
            DELETE FROM gold.kalshi_market_snapshots
            WHERE snapshot_timestamp < CURRENT_TIMESTAMP - INTERVAL '90 days'
        """)
    except Exception as e:
        print(f"Snapshot cleanup error: {e}")


with DAG(
    dag_id="maintenance_pipeline",
    default_args=default_args,
    description="Weekly maintenance: vacuum, recalibrate, cleanup",
    schedule="0 2 * * 0",  # Sunday 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance"],
) as dag:

    vacuum = PythonOperator(task_id="vacuum_duckdb", python_callable=vacuum_duckdb)
    recalibrate = PythonOperator(task_id="recalibrate_models", python_callable=recalibrate_models)
    cleanup = PythonOperator(task_id="cleanup_old_snapshots", python_callable=cleanup_old_snapshots)

    vacuum >> recalibrate >> cleanup
