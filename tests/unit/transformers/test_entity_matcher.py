"""Tests for Kalshi entity matcher."""

from __future__ import annotations

from sports_pipeline.transformers.kalshi.entity_matcher import EntityMatcher


class TestEntityMatcher:
    def test_match_nba_game_outcome(self):
        matcher = EntityMatcher()
        result = matcher.match(
            ticker="KXNBA-24OCT22-LAL-BOS-LAL",
            title="Will the Lakers beat the Celtics?",
        )
        assert result["sport"] == "basketball"
        assert result["market_type"] == "game_outcome"

    def test_match_nba_total(self):
        matcher = EntityMatcher()
        result = matcher.match(
            ticker="KXNBAOU-24OCT22-LAL-BOS",
            title="Will Lakers vs Celtics score over 220 total points?",
        )
        assert result["sport"] == "basketball"
        assert result["market_type"] == "total"

    def test_no_match(self):
        matcher = EntityMatcher()
        result = matcher.match(
            ticker="UNKNOWN-TICKER",
            title="Some random market",
        )
        assert result["sport"] is None
        assert result["market_type"] is None
