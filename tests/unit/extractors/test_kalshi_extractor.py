"""Tests for Kalshi extractors."""

from __future__ import annotations

from unittest.mock import MagicMock

from sports_pipeline.extractors.kalshi.market_extractor import KalshiMarketExtractor


class TestKalshiMarketExtractor:
    def test_is_sports_market_by_category(self):
        market = {"category": "Sports", "ticker": "SOME-TICKER"}
        assert KalshiMarketExtractor._is_sports_market(market)

    def test_is_sports_market_by_ticker(self):
        market = {"category": "Other", "ticker": "KXNBA-24OCT22-LAL-BOS-LAL"}
        assert KalshiMarketExtractor._is_sports_market(market)

    def test_non_sports_market(self):
        market = {"category": "Politics", "ticker": "PRES-24-DEM"}
        assert not KalshiMarketExtractor._is_sports_market(market)

    def test_extract_with_mock_client(self):
        client = MagicMock()
        client.get_markets.return_value = {
            "markets": [
                {
                    "ticker": "KXNBA-TEST",
                    "event_ticker": "KXNBA-EVENT",
                    "title": "Test NBA Market",
                    "category": "Sports",
                    "sub_title": "NBA",
                    "status": "active",
                    "yes_bid": 45,
                    "yes_ask": 47,
                    "no_bid": 53,
                    "no_ask": 55,
                    "volume": 100,
                    "open_interest": 50,
                    "close_time": None,
                    "result": None,
                },
            ],
            "cursor": None,
        }
        extractor = KalshiMarketExtractor(client=client)
        result = extractor.extract()

        assert not result.empty
        assert result.iloc[0]["ticker"] == "KXNBA-TEST"
