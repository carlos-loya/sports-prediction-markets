"""Airflow DAG for edge detection: model scoring + edge detection + alerts."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sports-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def detect_edges(**context):
    from sports_pipeline.edge_detection.detector import EdgeDetector
    detector = EdgeDetector()
    edges = detector.detect_all()
    context["ti"].xcom_push(key="raw_edges", value=len(edges))
    return edges


def filter_edges(**context):
    from sports_pipeline.edge_detection.filters import EdgeFilter
    edge_filter = EdgeFilter()
    # In production, edges would be passed via XCom or intermediate storage
    # Simplified: re-detect and filter
    from sports_pipeline.edge_detection.detector import EdgeDetector
    detector = EdgeDetector()
    raw = detector.detect_all()
    filtered = edge_filter.apply(raw)
    return filtered


def size_positions(**context):
    from sports_pipeline.edge_detection.kelly import KellyCriterion
    kelly = KellyCriterion()
    # Apply Kelly to filtered edges
    # Simplified for DAG definition
    return []


def dispatch_alerts(**context):
    from sports_pipeline.edge_detection.alerts import AlertDispatcher
    dispatcher = AlertDispatcher()
    # Would receive edges from upstream tasks
    dispatcher.dispatch([])


def update_resolved_edges(**context):
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    loader = DuckDBLoader()
    try:
        # Find markets that have settled and update edge resolution
        loader.execute("""
            UPDATE gold.edge_signals
            SET resolved = TRUE
            WHERE resolved = FALSE
            AND kalshi_ticker IN (
                SELECT DISTINCT ticker
                FROM gold.kalshi_market_snapshots
                WHERE status = 'settled' OR status = 'closed'
            )
        """)
    except Exception as e:
        print(f"Resolution update error: {e}")


def update_model_performance(**context):
    from sports_pipeline.analytics.calibration import brier_score, log_loss_score, calibration_error
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    import pandas as pd
    from datetime import date

    loader = DuckDBLoader()
    try:
        resolved = loader.query("""
            SELECT model_name, sport, market_type, model_prob, actual_outcome
            FROM gold.edge_signals
            WHERE resolved = TRUE AND actual_outcome IS NOT NULL
        """)

        if resolved.empty:
            return

        for (model, sport, mtype), group in resolved.groupby(["model_name", "sport", "market_type"]):
            probs = group["model_prob"].tolist()
            outcomes = group["actual_outcome"].astype(int).tolist()

            perf = pd.DataFrame([{
                "model_name": model,
                "sport": sport,
                "market_type": mtype,
                "evaluation_date": date.today(),
                "brier_score": brier_score(probs, outcomes),
                "log_loss": log_loss_score(probs, outcomes),
                "calibration_error": calibration_error(probs, outcomes),
                "total_predictions": len(probs),
                "hit_rate": sum(1 for p, o in zip(probs, outcomes) if (p > 0.5) == bool(o)) / len(probs),
                "avg_edge": 0,
                "roi": 0,
            }])
            loader.load_dataframe(perf, "model_performance")
    except Exception as e:
        print(f"Model performance update error: {e}")


with DAG(
    dag_id="edge_detection_pipeline",
    default_args=default_args,
    description="Edge detection: model scoring, edge detection, alerts",
    schedule="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["edge-detection", "models"],
) as dag:

    detect = PythonOperator(task_id="detect_edges", python_callable=detect_edges)
    filter_task = PythonOperator(task_id="filter_edges", python_callable=filter_edges)
    size = PythonOperator(task_id="size_positions", python_callable=size_positions)
    alerts = PythonOperator(task_id="dispatch_alerts", python_callable=dispatch_alerts)
    resolve = PythonOperator(task_id="update_resolved_edges", python_callable=update_resolved_edges)
    perf = PythonOperator(task_id="update_model_performance", python_callable=update_model_performance)

    detect >> filter_task >> size >> alerts
    detect >> resolve >> perf
