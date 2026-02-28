"""Airflow DAG for Kalshi market data ingestion (runs frequently on game days)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sports-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def fetch_active_markets(**context):
    from sports_pipeline.extractors.kalshi.market_extractor import KalshiMarketExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_kalshi_path
    from datetime import date

    extractor = KalshiMarketExtractor()
    df = extractor.extract(status="active")
    if not df.empty:
        path = bronze_kalshi_path(date.today(), "markets")
        write_parquet(df, path)
    return len(df)


def transform_and_match(**context):
    from sports_pipeline.transformers.kalshi.market_transformer import KalshiMarketTransformer
    from sports_pipeline.transformers.kalshi.entity_matcher import EntityMatcher
    from sports_pipeline.storage.parquet_store import read_parquet_dir
    from sports_pipeline.storage.paths import bronze_kalshi_path
    from sports_pipeline.config import PROJECT_ROOT, get_settings
    from datetime import date

    bronze_dir = PROJECT_ROOT / get_settings().storage.bronze_path / "kalshi"
    if not bronze_dir.exists():
        return 0

    df = read_parquet_dir(bronze_dir)
    transformer = KalshiMarketTransformer()
    silver = transformer.transform(df)

    matcher = EntityMatcher()
    matched = matcher.match_dataframe(silver)
    return len(matched)


def load_to_gold(**context):
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    from sports_pipeline.storage.parquet_store import read_parquet_dir
    from sports_pipeline.storage.paths import silver_kalshi_path
    from sports_pipeline.config import PROJECT_ROOT, get_settings
    import pandas as pd
    import uuid

    loader = DuckDBLoader()
    # Load transformed markets directly
    # In production, this would read from silver parquet
    # For now, we re-run the transform (idempotent)
    try:
        bronze_dir = PROJECT_ROOT / get_settings().storage.bronze_path / "kalshi"
        if bronze_dir.exists():
            from sports_pipeline.transformers.kalshi.market_transformer import KalshiMarketTransformer
            from sports_pipeline.transformers.kalshi.entity_matcher import EntityMatcher

            df = read_parquet_dir(bronze_dir)
            transformer = KalshiMarketTransformer()
            silver = transformer.transform(df)
            matcher = EntityMatcher()
            matched = matcher.match_dataframe(silver)

            if not matched.empty:
                matched["snapshot_id"] = [str(uuid.uuid4())[:12] for _ in range(len(matched))]
                loader.load_dataframe(matched, "kalshi_market_snapshots")
    except Exception as e:
        print(f"Kalshi load error: {e}")


with DAG(
    dag_id="kalshi_markets_pipeline",
    default_args=default_args,
    description="Kalshi sports market data ingestion",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kalshi", "markets"],
) as dag:

    fetch = PythonOperator(task_id="fetch_active_markets", python_callable=fetch_active_markets)
    transform = PythonOperator(task_id="transform_and_match", python_callable=transform_and_match)
    load = PythonOperator(task_id="load_to_gold", python_callable=load_to_gold)

    fetch >> transform >> load
