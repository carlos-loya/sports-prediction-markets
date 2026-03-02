"""Trade stream replayer for historical backtesting against Becker dataset.

Replays historical trade streams through the real-time EdgeProcessor pipeline,
resolving against known market outcomes to measure actual P&L and calibration.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from sports_pipeline.analytics.elo import EloModel
from sports_pipeline.loaders.becker_views import (
    get_settled_markets_by_sport,
    get_trades_for_ticker,
)
from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.realtime.config import RealtimeConfig
from sports_pipeline.realtime.events import TickEvent
from sports_pipeline.realtime.model_loader import SERIES_SPORT_MAP, parse_teams_from_title
from sports_pipeline.realtime.processors.edge_processor import (
    EdgeProcessor,
    ModelCache,
    ModelCacheEntry,
)
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

# Map sport prefix to SERIES_SPORT_MAP key
_PREFIX_TO_SERIES: dict[str, str] = {
    "KXNBA": "KXNBA",
    "KXNFL": "KXNFL",
    "KXMLB": "KXMLB",
    "KXNHL": "KXNHL",
    "KXSOC": "KXSOCCER",
    "KXMMA": "KXMMA",
}


def _detect_series(ticker: str) -> str:
    """Detect the series ticker from a market ticker."""
    for prefix, series in _PREFIX_TO_SERIES.items():
        if ticker.startswith(prefix):
            return series
    return "KXNBA"


class TradeStreamReplayer:
    """Replay Becker's historical trades through EdgeProcessor + Kelly sizing.

    For each settled market:
    1. Parse title → extract teams → EloModel.predict() → model_prob
    2. Query trades ordered by time
    3. For each trade, construct TickEvent from trade price
    4. Feed through EdgeProcessor.evaluate() → collect EdgeEvents
    5. For non-rejected edges, simulate Kelly-sized bet
    6. Resolve against known outcome
    """

    def __init__(
        self,
        config: RealtimeConfig,
        loader: DuckDBLoader,
    ) -> None:
        self._config = config
        self._loader = loader
        self._elo_models: dict[str, EloModel] = {}

    def _get_elo_model(self, sport: str) -> EloModel:
        if sport not in self._elo_models:
            self._elo_models[sport] = EloModel(sport=sport)
        return self._elo_models[sport]

    def replay(
        self,
        sport_prefix: str | None = None,
        max_markets: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Replay trades and return per-evaluation results.

        Args:
            sport_prefix: Filter to sport (e.g., 'KXNBA'). None = all sports.
            max_markets: Max number of markets to replay.
            start_date: ISO date string to filter markets (close_time >= start_date).
            end_date: ISO date string to filter markets (close_time <= end_date).

        Returns:
            DataFrame with columns: ticker, timestamp, model_prob, market_prob,
            edge, kelly_fraction, bet_amount, pnl, bankroll, won, result
        """
        # Load settled markets
        if sport_prefix:
            markets_df = get_settled_markets_by_sport(
                self._loader, sport_prefix, limit=max_markets
            )
        else:
            sql = "SELECT * FROM gold.becker_settled ORDER BY close_time"
            if max_markets:
                sql += f" LIMIT {max_markets}"
            markets_df = self._loader.query(sql)

        if markets_df.empty:
            log.warning("no_settled_markets_found", sport=sport_prefix)
            return pd.DataFrame()

        # Apply date filters if provided
        if "close_time" in markets_df.columns:
            if start_date:
                markets_df = markets_df[
                    markets_df["close_time"] >= pd.Timestamp(start_date)
                ]
            if end_date:
                markets_df = markets_df[
                    markets_df["close_time"] <= pd.Timestamp(end_date)
                ]

        log.info("replaying_markets", count=len(markets_df), sport=sport_prefix)

        bankroll = self._config.bankroll
        all_results: list[dict[str, Any]] = []

        for _, market in markets_df.iterrows():
            market_results = self._replay_market(market, bankroll)
            for r in market_results:
                bankroll = r["bankroll"]
            all_results.extend(market_results)

        result_df = pd.DataFrame(all_results)
        log.info(
            "replay_complete",
            markets=len(markets_df),
            trades=len(all_results),
            final_bankroll=round(bankroll, 2),
        )
        return result_df

    def _replay_market(
        self, market: pd.Series, bankroll: float
    ) -> list[dict[str, Any]]:
        """Replay all trades for a single settled market."""
        ticker = market["ticker"]
        title = market.get("title", "")
        result = market.get("result", "")

        # Parse teams and compute model probability
        teams = parse_teams_from_title(str(title))
        if teams is None:
            return []

        home_team, away_team = teams
        series = _detect_series(ticker)
        sport = SERIES_SPORT_MAP.get(series, "basketball")
        elo = self._get_elo_model(sport)
        prediction = elo.predict(home_team, away_team)
        model_prob = prediction["home_win"]

        # Set up EdgeProcessor with this market's model probability
        model_cache = ModelCache()
        model_cache.put(
            ModelCacheEntry(
                ticker=ticker,
                model_prob=model_prob,
                model_uncertainty=0.05,
                model_name=elo.name,
            )
        )
        processor = EdgeProcessor(self._config, model_cache)

        # Load trades for this market
        trades_df = get_trades_for_ticker(self._loader, ticker)
        if trades_df.empty:
            return []

        results: list[dict[str, Any]] = []
        outcome_yes = result == "yes"

        for _, trade in trades_df.iterrows():
            yes_price = float(trade.get("yes_price", 0.5))
            no_price = float(trade.get("no_price", 1.0 - yes_price))

            # Normalize prices if in cents
            if yes_price > 1:
                yes_price = yes_price / 100.0
            if no_price > 1:
                no_price = no_price / 100.0

            # Construct TickEvent
            created_time = trade.get("created_time")
            if isinstance(created_time, str):
                ts = datetime.fromisoformat(created_time)
            elif isinstance(created_time, pd.Timestamp):
                ts = created_time.to_pydatetime()
            else:
                ts = datetime.now(tz=UTC)

            tick = TickEvent(
                ticker=ticker,
                yes_price=yes_price,
                no_price=no_price,
                timestamp=ts,
            )

            edge_event = processor.evaluate(tick)

            if edge_event.rejected:
                continue

            # Simulate the bet
            kelly = edge_event.kelly_fraction
            bet_amount = bankroll * kelly

            if bet_amount <= 0:
                continue

            # Resolve P&L
            side = edge_event.suggested_side
            if side == "yes":
                if outcome_yes:
                    pnl = bet_amount * (1.0 / yes_price - 1)
                else:
                    pnl = -bet_amount
            else:
                if not outcome_yes:
                    pnl = bet_amount * (1.0 / no_price - 1) if no_price > 0 else 0
                else:
                    pnl = -bet_amount

            bankroll += pnl

            results.append({
                "timestamp": ts,
                "ticker": ticker,
                "title": title,
                "sport": sport,
                "model_name": edge_event.model_name,
                "model_prob": round(model_prob, 4),
                "market_prob": round(yes_price, 4),
                "raw_edge": round(edge_event.raw_edge, 4),
                "tradable_edge": round(edge_event.tradable_edge, 4),
                "kelly_fraction": round(kelly, 4),
                "suggested_side": side,
                "confidence": edge_event.confidence,
                "bet_amount": round(bet_amount, 2),
                "pnl": round(pnl, 2),
                "bankroll": round(bankroll, 2),
                "won": pnl > 0,
                "result": result,
            })

        return results
