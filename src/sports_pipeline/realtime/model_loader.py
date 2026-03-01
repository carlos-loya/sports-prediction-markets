"""Model cache loader for real-time edge detection.

Loads Elo ratings from DuckDB, parses market titles to extract teams,
computes model probabilities, and populates the real-time ModelCache.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

from sports_pipeline.analytics.elo import EloModel
from sports_pipeline.realtime.discovery import DiscoveredMarket
from sports_pipeline.realtime.processors.edge_processor import ModelCache, ModelCacheEntry
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

# Series ticker → sport mapping
SERIES_SPORT_MAP: dict[str, str] = {
    "KXNBA": "basketball",
    "KXNFL": "basketball",  # football uses same Elo structure
    "KXMLB": "basketball",
    "KXNHL": "basketball",
    "KXSOCCER": "soccer",
    "KXMMA": "basketball",
}

# Regex patterns to extract two teams from Kalshi market titles.
# Covers patterns like:
#   "Will the Lakers beat the Celtics?"
#   "Lakers vs Celtics"
#   "Will the Lakers win against the Celtics?"
#   "Lakers vs. Celtics: Who will win?"
TITLE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"Will\s+(?:the\s+)?(.+?)\s+(?:beat|win against|defeat)\s+(?:the\s+)?(.+?)[\?\.]",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(.+?)\s+vs\.?\s+(.+?)(?:\s*[\:\?]|\s*$)",
        re.IGNORECASE,
    ),
]


def parse_teams_from_title(title: str) -> tuple[str, str] | None:
    """Extract home and away team from a market title.

    Returns:
        (home_team, away_team) or None if parsing fails.
    """
    for pattern in TITLE_PATTERNS:
        match = pattern.search(title)
        if match:
            home = match.group(1).strip()
            away = match.group(2).strip()
            if home and away:
                return home, away
    return None


class ModelCacheLoader:
    """Loads batch model outputs into the real-time ModelCache.

    Workflow:
        1. Receive discovered markets (ticker, title, series)
        2. Parse titles to extract team matchups
        3. Load Elo ratings from DuckDB (if available)
        4. Compute model probabilities via EloModel.predict()
        5. Populate ModelCache with ModelCacheEntry per ticker
    """

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path
        self._elo_models: dict[str, EloModel] = {}
        self._ratings_loaded = False

    def _get_elo_model(self, sport: str) -> EloModel:
        """Get or create an EloModel for a sport."""
        if sport not in self._elo_models:
            self._elo_models[sport] = EloModel(sport=sport)
        return self._elo_models[sport]

    def load_ratings_from_db(self) -> None:
        """Load Elo ratings from DuckDB gold.elo_ratings table."""
        if not self._db_path or not self._db_path.exists():
            log.warning("elo_db_not_found", path=str(self._db_path))
            return

        try:
            from sports_pipeline.loaders.duckdb_loader import DuckDBLoader

            loader = DuckDBLoader(db_path=self._db_path)
            df = loader.query(
                "SELECT team, sport, rating FROM gold.elo_ratings"
            )
            for _, row in df.iterrows():
                sport = row["sport"]
                model = self._get_elo_model(sport)
                model.set_rating(row["team"], row["rating"])

            self._ratings_loaded = True
            log.info("elo_ratings_loaded", count=len(df))
        except Exception:
            log.warning("elo_ratings_load_failed", exc_info=True)

    def load(
        self,
        markets: list[DiscoveredMarket],
        model_cache: ModelCache,
    ) -> int:
        """Parse markets, compute model probs, populate cache.

        Args:
            markets: Discovered markets with titles.
            model_cache: ModelCache to populate.

        Returns:
            Number of entries added to cache.
        """
        if not self._ratings_loaded:
            self.load_ratings_from_db()

        entries: dict[str, ModelCacheEntry] = {}

        for market in markets:
            entry = self._compute_entry(market)
            if entry:
                entries[entry.ticker] = entry

        if entries:
            model_cache.refresh_from_dict(entries)

        log.info(
            "model_cache_loaded",
            markets=len(markets),
            entries=len(entries),
            cache_size=model_cache.size,
        )
        return len(entries)

    def _compute_entry(self, market: DiscoveredMarket) -> ModelCacheEntry | None:
        """Compute a ModelCacheEntry for a single market."""
        teams = parse_teams_from_title(market.title)
        if not teams:
            log.debug("title_parse_failed", ticker=market.ticker, title=market.title)
            return None

        home_team, away_team = teams
        sport = SERIES_SPORT_MAP.get(market.series_ticker, "basketball")
        model = self._get_elo_model(sport)
        prediction = model.predict(home_team, away_team)

        # For game outcome markets, model_prob = home team win probability
        # The "yes" side is assumed to be the first team (home)
        model_prob = prediction["home_win"]

        return ModelCacheEntry(
            ticker=market.ticker,
            model_prob=model_prob,
            model_uncertainty=0.05,  # fixed for single Elo model
            model_name=model.name,
        )

    async def refresh_loop(
        self,
        model_cache: ModelCache,
        get_markets: object,
        interval: int = 300,
    ) -> None:
        """Periodically refresh the model cache.

        Args:
            model_cache: ModelCache to refresh.
            get_markets: Callable returning list[DiscoveredMarket].
            interval: Seconds between refreshes.
        """
        while True:
            try:
                markets = get_markets()  # type: ignore[operator]
                if markets:
                    self._ratings_loaded = False  # force reload
                    self.load(markets, model_cache)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("model_refresh_error")
            await asyncio.sleep(interval)
