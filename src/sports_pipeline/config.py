"""Application configuration loaded from YAML + environment variables."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from sports_pipeline.realtime.config import RealtimeConfig

PROJECT_ROOT = Path(__file__).parent.parent.parent


class LeagueConfig(BaseModel):
    name: str
    seasons: list[str]
    fbref_id: str | None = None
    country: str | None = None


class RateLimitConfig(BaseModel):
    requests_per_minute: int = 8


class KalshiRateLimitConfig(BaseModel):
    reads_per_second: int = 20


class RateLimitsConfig(BaseModel):
    fbref: RateLimitConfig = RateLimitConfig()
    nba_api: RateLimitConfig = RateLimitConfig(requests_per_minute=30)
    kalshi: KalshiRateLimitConfig = KalshiRateLimitConfig()


class EloConfig(BaseModel):
    initial_rating: float = 1500
    k_factor_soccer: float = 32
    k_factor_nba: float = 20
    home_advantage_soccer: float = 65
    home_advantage_nba: float = 100


class PoissonConfig(BaseModel):
    league_avg_goals: float = 1.35


class PaceAdjustedConfig(BaseModel):
    league_avg_pace: float = 100.0


class ModelsConfig(BaseModel):
    elo: EloConfig = EloConfig()
    poisson: PoissonConfig = PoissonConfig()
    pace_adjusted: PaceAdjustedConfig = PaceAdjustedConfig()


class EdgeDetectionConfig(BaseModel):
    min_edge_pct: float = 0.05
    min_volume: int = 100
    min_time_to_close_hours: float = 1.0
    kelly_fraction: float = 0.25
    confidence_levels: dict[str, float] = {
        "high": 0.10,
        "medium": 0.07,
        "low": 0.05,
    }


class StorageConfig(BaseModel):
    bronze_path: str = "data/bronze"
    silver_path: str = "data/silver"
    gold_path: str = "data/gold"
    duckdb_path: str = "data/gold/sports_analytics.duckdb"


class LeaguesConfig(BaseModel):
    soccer: list[LeagueConfig] = []
    basketball: list[LeagueConfig] = []


class Settings(BaseSettings):
    environment: str = "dev"
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = ""
    slack_webhook_url: str = ""

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    leagues: LeaguesConfig = LeaguesConfig()
    rate_limits: RateLimitsConfig = RateLimitsConfig()
    edge_detection: EdgeDetectionConfig = EdgeDetectionConfig()
    models: ModelsConfig = ModelsConfig()
    storage: StorageConfig = StorageConfig()
    realtime: RealtimeConfig = RealtimeConfig()

    model_config = {"env_prefix": "", "env_file": ".env", "extra": "ignore"}


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


@lru_cache
def get_settings() -> Settings:
    """Load settings from YAML config + env vars. Dev overlay applied if ENVIRONMENT=dev."""
    config_dir = PROJECT_ROOT / "config"
    base = _load_yaml(config_dir / "settings.yaml")

    env = os.getenv("ENVIRONMENT", "dev")
    overlay_path = config_dir / f"settings.{env}.yaml"
    if overlay_path.exists():
        overlay = _load_yaml(overlay_path)
        base = _deep_merge(base, overlay)

    return Settings(**base)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base dict."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
