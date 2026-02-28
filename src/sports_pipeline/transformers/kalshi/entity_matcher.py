"""Match Kalshi market tickers to sports entities (teams, players, games)."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

import pandas as pd
import yaml

from sports_pipeline.config import PROJECT_ROOT
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@lru_cache
def _load_market_mappings() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "kalshi_market_mappings.yaml"
    with open(path) as f:
        return yaml.safe_load(f) or {}


class EntityMatcher:
    """Match Kalshi markets to sports entities using ticker patterns and title parsing."""

    def __init__(self) -> None:
        self._mappings = _load_market_mappings()
        self._normalizer = NameNormalizer()
        self._patterns = self._compile_patterns()

    def _compile_patterns(self) -> list[dict[str, Any]]:
        """Pre-compile regex patterns from mapping config."""
        compiled = []
        for name, config in self._mappings.get("patterns", {}).items():
            ticker_re = re.compile(config["ticker_pattern"])
            title_re = None
            if "title_pattern" in config:
                title_re = re.compile(config["title_pattern"], re.IGNORECASE)
            compiled.append({
                "name": name,
                "ticker_re": ticker_re,
                "title_re": title_re,
                "sport": config.get("sport"),
                "market_type": config.get("market_type"),
                "prop_type": config.get("prop_type"),
            })
        return compiled

    def match(self, ticker: str, title: str) -> dict[str, Any]:
        """Match a single market to its sport/type/entity.

        Returns dict with keys: sport, market_type, matched_entity_id, matched_entity_name
        """
        # Check manual overrides first
        overrides = self._mappings.get("overrides", {})
        if ticker in overrides:
            return overrides[ticker]

        # Try pattern matching
        for pattern in self._patterns:
            if pattern["ticker_re"].match(ticker):
                result = {
                    "sport": pattern["sport"],
                    "market_type": pattern["market_type"],
                    "matched_entity_id": None,
                    "matched_entity_name": None,
                }

                # Try to extract entity from title
                if pattern["title_re"] and title:
                    title_match = pattern["title_re"].search(title)
                    if title_match:
                        groups = title_match.groupdict()
                        team = groups.get("team")
                        player = groups.get("player")
                        if team:
                            canonical = self._normalizer.normalize_team(team, pattern["sport"])
                            result["matched_entity_name"] = canonical
                            result["matched_entity_id"] = canonical.lower().replace(" ", "_")
                        elif player:
                            result["matched_entity_name"] = player
                            result["matched_entity_id"] = player.lower().replace(" ", "_")

                return result

        return {
            "sport": None,
            "market_type": None,
            "matched_entity_id": None,
            "matched_entity_name": None,
        }

    def match_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply entity matching to a DataFrame of Kalshi markets.

        Expects columns: ticker, title
        Adds columns: sport, market_type, matched_entity_id, matched_entity_name
        """
        if df.empty:
            return df

        matches = df.apply(
            lambda row: self.match(row.get("ticker", ""), row.get("title", "")),
            axis=1,
            result_type="expand",
        )

        for col in ["sport", "market_type", "matched_entity_id", "matched_entity_name"]:
            df[col] = matches[col]

        matched_count = df["sport"].notna().sum()
        log.info("entity_matching_complete", total=len(df), matched=int(matched_count))
        return df
