"""DuckDB view layer over Jon Becker's prediction-market-analysis parquet files.

Creates SQL views that query raw parquet in-place using DuckDB's read_parquet()
with glob patterns. Predicate pushdown filters to sports tickers efficiently
without copying or transforming the source data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

# Kalshi sports series ticker prefixes
SPORTS_TICKER_PREFIXES = [
    "KXNBA",
    "KXNFL",
    "KXMLB",
    "KXNHL",
    "KXSOC",
    "KXMMA",
]


def _sports_filter_sql(column: str = "ticker") -> str:
    """Build SQL WHERE clause for sports ticker filtering."""
    conditions = [f"{column} LIKE '{prefix}%'" for prefix in SPORTS_TICKER_PREFIXES]
    return " OR ".join(conditions)


def create_becker_views(loader: DuckDBLoader, becker_path: str | Path) -> None:
    """Create DuckDB views over Becker's raw parquet files.

    Args:
        loader: DuckDBLoader instance.
        becker_path: Path to the extracted Becker dataset root.
    """
    becker_path = Path(becker_path)
    trades_glob = str(becker_path / "kalshi" / "trades" / "*.parquet")
    markets_glob = str(becker_path / "kalshi" / "markets" / "*.parquet")
    filter_clause = _sports_filter_sql()

    conn = loader.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

        # View over all sports trades
        conn.execute(f"""
            CREATE OR REPLACE VIEW gold.becker_trades AS
            SELECT * FROM read_parquet('{trades_glob}')
            WHERE {filter_clause}
        """)

        # View over all sports markets
        conn.execute(f"""
            CREATE OR REPLACE VIEW gold.becker_markets AS
            SELECT * FROM read_parquet('{markets_glob}')
            WHERE {filter_clause}
        """)

        # Settled markets with known outcomes (for backtesting)
        conn.execute("""
            CREATE OR REPLACE VIEW gold.becker_settled AS
            SELECT * FROM gold.becker_markets
            WHERE status = 'settled' AND result IN ('yes', 'no')
        """)

        log.info(
            "becker_views_created",
            trades_glob=trades_glob,
            markets_glob=markets_glob,
        )
    finally:
        conn.close()


def get_sports_trade_count(loader: DuckDBLoader) -> int:
    """Quick sanity check: count of sports trades in Becker dataset."""
    df = loader.query("SELECT COUNT(*) AS cnt FROM gold.becker_trades")
    return int(df["cnt"].iloc[0])


def get_settled_markets(loader: DuckDBLoader) -> pd.DataFrame:
    """Return DataFrame of settled sports markets with known outcomes."""
    return loader.query("""
        SELECT * FROM gold.becker_settled
        ORDER BY close_time
    """)


def get_trades_for_ticker(loader: DuckDBLoader, ticker: str) -> pd.DataFrame:
    """Return all trades for a specific ticker, ordered by time."""
    return loader.query(f"""
        SELECT * FROM gold.becker_trades
        WHERE ticker = '{ticker}'
        ORDER BY created_time
    """)


def get_settled_markets_by_sport(
    loader: DuckDBLoader, sport_prefix: str, limit: int | None = None
) -> pd.DataFrame:
    """Return settled markets for a specific sport prefix.

    Args:
        loader: DuckDBLoader instance.
        sport_prefix: Ticker prefix like 'KXNBA'.
        limit: Max number of markets to return.
    """
    sql = f"""
        SELECT * FROM gold.becker_settled
        WHERE ticker LIKE '{sport_prefix}%'
        ORDER BY close_time
    """
    if limit is not None:
        sql += f" LIMIT {limit}"
    return loader.query(sql)
