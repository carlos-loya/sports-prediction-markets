"""Tests for ModelCacheLoader."""

from __future__ import annotations

from sports_pipeline.realtime.discovery import DiscoveredMarket
from sports_pipeline.realtime.model_loader import (
    ModelCacheLoader,
    parse_teams_from_title,
)
from sports_pipeline.realtime.processors.edge_processor import ModelCache


class TestParseTeamsFromTitle:
    def test_will_beat_pattern(self) -> None:
        result = parse_teams_from_title("Will the Lakers beat the Celtics?")
        assert result == ("Lakers", "Celtics")

    def test_will_win_against_pattern(self) -> None:
        result = parse_teams_from_title("Will the Warriors win against the Suns?")
        assert result == ("Warriors", "Suns")

    def test_will_defeat_pattern(self) -> None:
        result = parse_teams_from_title("Will the Bucks defeat the Heat?")
        assert result == ("Bucks", "Heat")

    def test_vs_pattern(self) -> None:
        result = parse_teams_from_title("Lakers vs Celtics")
        assert result == ("Lakers", "Celtics")

    def test_vs_dot_pattern(self) -> None:
        result = parse_teams_from_title("Lakers vs. Celtics")
        assert result == ("Lakers", "Celtics")

    def test_vs_with_colon(self) -> None:
        result = parse_teams_from_title("Lakers vs Celtics: Who will win?")
        assert result == ("Lakers", "Celtics")

    def test_no_match(self) -> None:
        result = parse_teams_from_title("Total points over 220.5")
        assert result is None

    def test_without_the(self) -> None:
        result = parse_teams_from_title("Will Arsenal beat Liverpool?")
        assert result == ("Arsenal", "Liverpool")


class TestModelCacheLoader:
    def _make_market(
        self,
        ticker: str = "T1",
        title: str = "Will the Lakers beat the Celtics?",
        yes_price: float = 0.55,
        series: str = "KXNBA",
    ) -> DiscoveredMarket:
        return DiscoveredMarket(
            ticker=ticker,
            title=title,
            yes_price=yes_price,
            series_ticker=series,
        )

    def test_load_populates_cache(self) -> None:
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()
        markets = [self._make_market()]

        count = loader.load(markets, cache)

        assert count == 1
        assert cache.size == 1
        entry = cache.get("T1")
        assert entry is not None
        assert entry.model_name == "elo_basketball"
        assert 0.0 < entry.model_prob < 1.0
        assert entry.model_uncertainty == 0.05

    def test_load_skips_unparseable_titles(self) -> None:
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()
        markets = [
            self._make_market(ticker="T1", title="Total points over 220.5"),
            self._make_market(ticker="T2", title="Will the Lakers beat the Celtics?"),
        ]

        count = loader.load(markets, cache)

        assert count == 1
        assert cache.get("T1") is None
        assert cache.get("T2") is not None

    def test_load_soccer_series(self) -> None:
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()
        markets = [
            self._make_market(
                ticker="S1",
                title="Will Arsenal beat Liverpool?",
                series="KXSOCCER",
            )
        ]

        count = loader.load(markets, cache)

        assert count == 1
        entry = cache.get("S1")
        assert entry is not None
        assert entry.model_name == "elo_soccer"

    def test_load_with_db_ratings(self) -> None:
        """Verify ratings from DB are used when available."""
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()

        # Manually inject ratings
        model = loader._get_elo_model("basketball")
        model.set_rating("Lakers", 1600.0)
        model.set_rating("Celtics", 1400.0)
        loader._ratings_loaded = True

        markets = [self._make_market()]
        count = loader.load(markets, cache)

        assert count == 1
        entry = cache.get("T1")
        assert entry is not None
        # Lakers (1600 + home_adv) vs Celtics (1400) → Lakers should be favored
        assert entry.model_prob > 0.5

    def test_load_empty_markets(self) -> None:
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()
        count = loader.load([], cache)
        assert count == 0
        assert cache.size == 0

    def test_multiple_markets(self) -> None:
        loader = ModelCacheLoader(db_path=None)
        cache = ModelCache()
        markets = [
            self._make_market(ticker="T1", title="Will the Lakers beat the Celtics?"),
            self._make_market(ticker="T2", title="Warriors vs Suns"),
        ]

        count = loader.load(markets, cache)

        assert count == 2
        assert cache.get("T1") is not None
        assert cache.get("T2") is not None
