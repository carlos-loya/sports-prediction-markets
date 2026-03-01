"""Real-time system configuration."""

from __future__ import annotations

from pydantic import BaseModel


class KafkaConfig(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "sports-rt"
    group_id: str = "sports-rt-group"


class WebSocketConfig(BaseModel):
    url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    channels: list[str] = [
        "ticker",
        "trade",
        "orderbook_delta",
        "fill",
        "market_lifecycle_v2",
    ]
    reconnect_delay_initial: float = 1.0
    reconnect_delay_max: float = 60.0
    ping_interval: float = 10.0


class VPINConfig(BaseModel):
    bucket_size: int = 50
    n_buckets: int = 50
    threshold_elevated: float = 0.3
    threshold_critical: float = 0.6


class EntropyConfig(BaseModel):
    min_price: float = 0.30
    max_price: float = 0.70


class MarketMakerConfig(BaseModel):
    enabled: bool = False
    gamma: float = 0.1  # risk aversion
    kappa: float = 1.5  # order arrival intensity
    sigma_window: int = 100  # ticks for volatility estimation
    min_spread_cents: int = 2
    max_position: int = 50  # max contracts per side


class RiskConfig(BaseModel):
    max_position_per_market: int = 100
    max_total_exposure: float = 5000.0
    daily_loss_limit: float = 500.0
    emergency_loss_limit: float = 1000.0
    gtd_expiry_minutes: int = 10


class KellyConfig(BaseModel):
    n_simulations: int = 10000
    fraction: float = 0.25  # fractional Kelly
    max_bankroll_pct: float = 0.20


class FeeConfig(BaseModel):
    taker_fee_cents: float = 7.0
    maker_rebate_cents: float = 0.0
    slippage_cents: float = 1.0
    min_tradable_edge: float = 0.03


class TelegramConfig(BaseModel):
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""


class TargetSeriesConfig(BaseModel):
    series_tickers: list[str] = [
        "KXNBA",
        "KXNFL",
        "KXMLB",
        "KXNHL",
        "KXSOCCER",
        "KXMMA",
    ]


class RealtimeConfig(BaseModel):
    kafka: KafkaConfig = KafkaConfig()
    websocket: WebSocketConfig = WebSocketConfig()
    vpin: VPINConfig = VPINConfig()
    entropy: EntropyConfig = EntropyConfig()
    market_maker: MarketMakerConfig = MarketMakerConfig()
    risk: RiskConfig = RiskConfig()
    kelly: KellyConfig = KellyConfig()
    fees: FeeConfig = FeeConfig()
    telegram: TelegramConfig = TelegramConfig()
    target_series: TargetSeriesConfig = TargetSeriesConfig()
    model_cache_refresh_seconds: int = 300
    paper_mode: bool = True
    discovery_interval_seconds: int = 300
    bankroll: float = 10000.0
