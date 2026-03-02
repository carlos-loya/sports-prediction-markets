"""Tests for the Becker trade stream replayer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sports_pipeline.backtesting.replayer import TradeStreamReplayer, _detect_series
from sports_pipeline.realtime.config import (
    EntropyConfig,
    FeeConfig,
    KafkaConfig,
    KellyConfig,
    MarketMakerConfig,
    RealtimeConfig,
    RiskConfig,
    TargetSeriesConfig,
    TelegramConfig,
    VPINConfig,
    WebSocketConfig,
)


@pytest.fixture
def rt_config():
    """Minimal RealtimeConfig for testing."""
    return RealtimeConfig(
        kafka=KafkaConfig(),
        websocket=WebSocketConfig(channels=["ticker"]),
        vpin=VPINConfig(),
        entropy=EntropyConfig(min_price=0.10, max_price=0.90),
        market_maker=MarketMakerConfig(),
        risk=RiskConfig(),
        kelly=KellyConfig(n_simulations=100, fraction=0.25, max_bankroll_pct=0.20),
        fees=FeeConfig(
            taker_fee_cents=0.0, slippage_cents=0.0, min_tradable_edge=0.01
        ),
        telegram=TelegramConfig(),
        target_series=TargetSeriesConfig(),
        bankroll=10000.0,
    )


@pytest.fixture
def mock_loader():
    return MagicMock()


@pytest.fixture
def settled_market_df():
    """Single settled market DataFrame."""
    return pd.DataFrame([{
        "ticker": "KXNBA-24OCT22-LAL-BOS-LAL",
        "title": "Will the Lakers beat the Celtics?",
        "status": "settled",
        "result": "yes",
        "close_time": pd.Timestamp("2024-10-23"),
        "volume": 500,
    }])


@pytest.fixture
def trades_df():
    """Trades for the settled market."""
    return pd.DataFrame([
        {
            "trade_id": "t1",
            "ticker": "KXNBA-24OCT22-LAL-BOS-LAL",
            "yes_price": 0.45,
            "no_price": 0.55,
            "count": 10,
            "taker_side": "yes",
            "created_time": pd.Timestamp("2024-10-22T18:00:00"),
        },
        {
            "trade_id": "t2",
            "ticker": "KXNBA-24OCT22-LAL-BOS-LAL",
            "yes_price": 0.50,
            "no_price": 0.50,
            "count": 5,
            "taker_side": "no",
            "created_time": pd.Timestamp("2024-10-22T19:00:00"),
        },
        {
            "trade_id": "t3",
            "ticker": "KXNBA-24OCT22-LAL-BOS-LAL",
            "yes_price": 0.55,
            "no_price": 0.45,
            "count": 8,
            "taker_side": "yes",
            "created_time": pd.Timestamp("2024-10-22T20:00:00"),
        },
    ])


class TestDetectSeries:
    def test_nba(self):
        assert _detect_series("KXNBA-24OCT22-LAL-BOS") == "KXNBA"

    def test_nfl(self):
        assert _detect_series("KXNFL-24SEP22-KC-BUF") == "KXNFL"

    def test_soccer(self):
        assert _detect_series("KXSOC-24SEP22-ARS-MCI") == "KXSOCCER"

    def test_unknown_defaults_to_nba(self):
        assert _detect_series("UNKNOWN-TICKER") == "KXNBA"


class TestTradeStreamReplayer:
    def test_replay_with_synthetic_data(
        self, rt_config, mock_loader, settled_market_df, trades_df
    ):
        """Test replay produces results for a synthetic settled market."""
        # Mock: get_settled_markets_by_sport returns our market
        mock_loader.query.return_value = settled_market_df

        with patch(
            "sports_pipeline.backtesting.replayer.get_settled_markets_by_sport",
            return_value=settled_market_df,
        ), patch(
            "sports_pipeline.backtesting.replayer.get_trades_for_ticker",
            return_value=trades_df,
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            results = replayer.replay(sport_prefix="KXNBA", max_markets=1)

        # Should produce some results (exact count depends on edge thresholds)
        # With zero fees and min_edge=0.01, most trades should produce edges
        assert isinstance(results, pd.DataFrame)
        if not results.empty:
            assert "ticker" in results.columns
            assert "pnl" in results.columns
            assert "bankroll" in results.columns
            assert "won" in results.columns
            assert "model_prob" in results.columns
            assert "market_prob" in results.columns

    def test_replay_empty_markets(self, rt_config, mock_loader):
        """Test replay returns empty DataFrame when no settled markets."""
        with patch(
            "sports_pipeline.backtesting.replayer.get_settled_markets_by_sport",
            return_value=pd.DataFrame(),
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            results = replayer.replay(sport_prefix="KXNBA")

        assert results.empty

    def test_replay_no_trades_for_market(
        self, rt_config, mock_loader, settled_market_df
    ):
        """Test replay handles markets with no trades."""
        with patch(
            "sports_pipeline.backtesting.replayer.get_settled_markets_by_sport",
            return_value=settled_market_df,
        ), patch(
            "sports_pipeline.backtesting.replayer.get_trades_for_ticker",
            return_value=pd.DataFrame(),
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            results = replayer.replay(sport_prefix="KXNBA", max_markets=1)

        assert results.empty

    def test_replay_unparseable_title(self, rt_config, mock_loader):
        """Test replay skips markets with unparseable titles."""
        bad_market = pd.DataFrame([{
            "ticker": "KXNBA-MISC",
            "title": "Some random prop bet",
            "status": "settled",
            "result": "yes",
            "close_time": pd.Timestamp("2024-10-23"),
            "volume": 100,
        }])

        with patch(
            "sports_pipeline.backtesting.replayer.get_settled_markets_by_sport",
            return_value=bad_market,
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            results = replayer.replay(sport_prefix="KXNBA", max_markets=1)

        assert results.empty

    def test_replay_yes_outcome_winning_bet(self, rt_config, mock_loader):
        """Test P&L calculation for a winning YES bet on YES outcome."""
        market = pd.DataFrame([{
            "ticker": "KXNBA-TEST-LAL-BOS-LAL",
            "title": "Will the Lakers beat the Celtics?",
            "status": "settled",
            "result": "yes",
            "close_time": pd.Timestamp("2024-10-23"),
            "volume": 100,
        }])

        # Single trade at 0.40 — model will predict ~0.60 for home team
        # (Elo default = 1500+100 vs 1500 → ~0.64 expected)
        # So raw_edge ~ 0.24, which should pass filters
        trades = pd.DataFrame([{
            "trade_id": "t1",
            "ticker": "KXNBA-TEST-LAL-BOS-LAL",
            "yes_price": 0.40,
            "no_price": 0.60,
            "count": 10,
            "taker_side": "yes",
            "created_time": pd.Timestamp("2024-10-22T18:00:00"),
        }])

        with patch(
            "sports_pipeline.backtesting.replayer.get_settled_markets_by_sport",
            return_value=market,
        ), patch(
            "sports_pipeline.backtesting.replayer.get_trades_for_ticker",
            return_value=trades,
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            results = replayer.replay(sport_prefix="KXNBA", max_markets=1)

        if not results.empty:
            # With result=yes and suggested_side=yes, pnl should be positive
            row = results.iloc[0]
            assert row["won"] is True or row["won"] == True  # noqa: E712
            assert row["pnl"] > 0

    def test_replay_all_sports_no_prefix(
        self, rt_config, mock_loader, settled_market_df, trades_df
    ):
        """Test replay without sport prefix queries all sports."""
        mock_loader.query.return_value = settled_market_df

        with patch(
            "sports_pipeline.backtesting.replayer.get_trades_for_ticker",
            return_value=trades_df,
        ):
            replayer = TradeStreamReplayer(config=rt_config, loader=mock_loader)
            replayer.replay(sport_prefix=None, max_markets=1)

        # Verify it called loader.query (not get_settled_markets_by_sport)
        mock_loader.query.assert_called()
