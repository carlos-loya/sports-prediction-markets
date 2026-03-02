"""Tests for Becker DuckDB view creation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from sports_pipeline.loaders.becker_views import (
    SPORTS_TICKER_PREFIXES,
    _sports_filter_sql,
    create_becker_views,
    get_settled_markets,
    get_settled_markets_by_sport,
    get_sports_trade_count,
    get_trades_for_ticker,
)


class TestSportsFilterSql:
    def test_generates_like_clauses(self):
        sql = _sports_filter_sql()
        for prefix in SPORTS_TICKER_PREFIXES:
            assert f"ticker LIKE '{prefix}%'" in sql

    def test_custom_column(self):
        sql = _sports_filter_sql(column="market_ticker")
        assert "market_ticker LIKE 'KXNBA%'" in sql

    def test_all_prefixes_joined_with_or(self):
        sql = _sports_filter_sql()
        parts = sql.split(" OR ")
        assert len(parts) == len(SPORTS_TICKER_PREFIXES)


class TestCreateBeckerViews:
    def test_creates_views_with_correct_paths(self):
        loader = MagicMock()
        conn = MagicMock()
        loader.get_connection.return_value = conn

        becker_path = Path("/data/becker")
        create_becker_views(loader, becker_path)

        # Should create schema + 3 views
        calls = conn.execute.call_args_list
        sql_statements = [c[0][0] for c in calls]

        # Schema creation
        assert any("CREATE SCHEMA IF NOT EXISTS gold" in s for s in sql_statements)

        # Trades view with glob path
        trades_sql = [s for s in sql_statements if "becker_trades" in s]
        assert len(trades_sql) == 1
        assert "/data/becker/kalshi/trades/*.parquet" in trades_sql[0]
        assert "KXNBA" in trades_sql[0]

        # Markets view with glob path
        markets_sql = [s for s in sql_statements if "VIEW gold.becker_markets" in s]
        assert len(markets_sql) == 1
        assert "/data/becker/kalshi/markets/*.parquet" in markets_sql[0]

        # Settled view
        settled_sql = [s for s in sql_statements if "becker_settled" in s]
        assert len(settled_sql) == 1
        assert "status = 'settled'" in settled_sql[0]
        assert "result IN ('yes', 'no')" in settled_sql[0]

        conn.close.assert_called_once()

    def test_accepts_string_path(self):
        loader = MagicMock()
        conn = MagicMock()
        loader.get_connection.return_value = conn

        create_becker_views(loader, "/data/becker")
        conn.execute.assert_called()
        conn.close.assert_called_once()


class TestHelperQueries:
    def test_get_sports_trade_count(self):
        loader = MagicMock()
        loader.query.return_value = pd.DataFrame({"cnt": [42000]})

        count = get_sports_trade_count(loader)
        assert count == 42000
        assert "becker_trades" in loader.query.call_args[0][0]

    def test_get_settled_markets(self):
        loader = MagicMock()
        loader.query.return_value = pd.DataFrame({"ticker": ["KXNBA-1"]})

        df = get_settled_markets(loader)
        assert len(df) == 1
        assert "becker_settled" in loader.query.call_args[0][0]
        assert "ORDER BY close_time" in loader.query.call_args[0][0]

    def test_get_trades_for_ticker(self):
        loader = MagicMock()
        loader.query.return_value = pd.DataFrame({"trade_id": ["t1"]})

        df = get_trades_for_ticker(loader, "KXNBA-TEST")
        assert len(df) == 1
        sql = loader.query.call_args[0][0]
        assert "KXNBA-TEST" in sql
        assert "ORDER BY created_time" in sql

    def test_get_settled_markets_by_sport_with_limit(self):
        loader = MagicMock()
        loader.query.return_value = pd.DataFrame({"ticker": ["KXNBA-1"]})

        get_settled_markets_by_sport(loader, "KXNBA", limit=50)
        sql = loader.query.call_args[0][0]
        assert "KXNBA%" in sql
        assert "LIMIT 50" in sql

    def test_get_settled_markets_by_sport_no_limit(self):
        loader = MagicMock()
        loader.query.return_value = pd.DataFrame({"ticker": ["KXNBA-1"]})

        get_settled_markets_by_sport(loader, "KXNBA")
        sql = loader.query.call_args[0][0]
        assert "LIMIT" not in sql
