"""Tests for real-time configuration models."""

from __future__ import annotations

from sports_pipeline.realtime.config import (
    EntropyConfig,
    FeeConfig,
    KafkaConfig,
    MarketMakerConfig,
    RealtimeConfig,
    RiskConfig,
    VPINConfig,
)


class TestRealtimeConfig:
    def test_defaults(self):
        config = RealtimeConfig()
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.websocket.url == "wss://api.elections.kalshi.com/trade-api/ws/v2"
        assert config.market_maker.enabled is False
        assert config.risk.daily_loss_limit == 500.0
        assert config.kelly.n_simulations == 10000
        assert config.fees.taker_fee_cents == 7.0
        assert config.telegram.enabled is False

    def test_custom_values(self):
        config = RealtimeConfig(
            kafka=KafkaConfig(bootstrap_servers="kafka:19092"),
            vpin=VPINConfig(bucket_size=100),
            risk=RiskConfig(daily_loss_limit=1000.0),
        )
        assert config.kafka.bootstrap_servers == "kafka:19092"
        assert config.vpin.bucket_size == 100
        assert config.risk.daily_loss_limit == 1000.0


class TestVPINConfig:
    def test_thresholds(self):
        config = VPINConfig()
        assert config.threshold_elevated < config.threshold_critical


class TestEntropyConfig:
    def test_price_range(self):
        config = EntropyConfig()
        assert config.min_price < config.max_price
        assert 0 < config.min_price < 1
        assert 0 < config.max_price < 1


class TestMarketMakerConfig:
    def test_disabled_by_default(self):
        config = MarketMakerConfig()
        assert config.enabled is False


class TestFeeConfig:
    def test_default_fees(self):
        config = FeeConfig()
        assert config.taker_fee_cents == 7.0
        assert config.maker_rebate_cents == 0.0
        assert config.min_tradable_edge > 0
