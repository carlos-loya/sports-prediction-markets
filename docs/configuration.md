# Configuration

Complete reference for all configuration knobs, defaults, and environment variables.

## Configuration Loading

**File:** `config.py`

Settings are loaded in this order (later overrides earlier):

1. `config/settings.yaml` — base configuration with all defaults
2. `config/settings.{ENVIRONMENT}.yaml` — environment-specific overlay (deep-merged)
3. `.env` file — environment variables
4. Shell environment variables

The `Settings` class uses `pydantic-settings` with `env_file=".env"`.

```python
settings = get_settings()  # cached singleton
```

## Storage

**Class:** `StorageConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `storage.bronze_path` | `data/bronze` | Raw extracted data |
| `storage.silver_path` | `data/silver` | Cleaned/validated data |
| `storage.gold_path` | `data/gold` | Analytical layer |
| `storage.duckdb_path` | `data/gold/sports_analytics.duckdb` | DuckDB database file |
| `storage.becker_data_path` | `data/becker` | Becker dataset location |

## Leagues

**Class:** `LeaguesConfig`

Defines which leagues and seasons to extract data for.

### Soccer

```yaml
leagues:
  soccer:
    - name: "Premier League"
      fbref_id: "9"
      country: "England"
      seasons: ["2024-2025", "2023-2024"]
    - name: "La Liga"
      fbref_id: "12"
      country: "Spain"
      seasons: ["2024-2025", "2023-2024"]
    - name: "Serie A"
      fbref_id: "11"
      country: "Italy"
      seasons: ["2024-2025", "2023-2024"]
    - name: "Bundesliga"
      fbref_id: "20"
      country: "Germany"
      seasons: ["2024-2025", "2023-2024"]
    - name: "Ligue 1"
      fbref_id: "13"
      country: "France"
      seasons: ["2024-2025", "2023-2024"]
```

### Basketball

```yaml
leagues:
  basketball:
    - name: "NBA"
      seasons: ["2024-25", "2023-24"]
```

## Rate Limits

**Class:** `RateLimitsConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `rate_limits.fbref.requests_per_minute` | 8 | FBref scraping rate limit |
| `rate_limits.nba_api.requests_per_minute` | 30 | nba_api rate limit |
| `rate_limits.kalshi.reads_per_second` | 20 | Kalshi API read rate limit |

## Models

**Class:** `ModelsConfig`

### Elo

| Key | Default | Description |
|-----|---------|-------------|
| `models.elo.initial_rating` | 1500 | Starting Elo for unknown teams |
| `models.elo.k_factor_soccer` | 32 | Update speed for soccer matches |
| `models.elo.k_factor_nba` | 20 | Update speed for NBA games |
| `models.elo.home_advantage_soccer` | 65 | Elo points added to home team (soccer) |
| `models.elo.home_advantage_nba` | 100 | Elo points added to home team (NBA) |

### Poisson

| Key | Default | Description |
|-----|---------|-------------|
| `models.poisson.league_avg_goals` | 1.35 | Average goals per team per match |

### Pace-Adjusted

| Key | Default | Description |
|-----|---------|-------------|
| `models.pace_adjusted.league_avg_pace` | 100.0 | League average possessions per game |

## Edge Detection (Batch)

**Class:** `EdgeDetectionConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `edge_detection.min_edge_pct` | 0.05 | Minimum edge to report (5%) |
| `edge_detection.min_volume` | 100 | Minimum market volume |
| `edge_detection.min_time_to_close_hours` | 1.0 | Minimum hours before market close |
| `edge_detection.kelly_fraction` | 0.25 | Fractional Kelly for batch sizing |
| `edge_detection.confidence_levels.high` | 0.10 | Edge threshold for high confidence |
| `edge_detection.confidence_levels.medium` | 0.07 | Edge threshold for medium confidence |
| `edge_detection.confidence_levels.low` | 0.05 | Edge threshold for low confidence |

## Real-Time System

**Class:** `RealtimeConfig`

### Kafka

**Class:** `KafkaConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.kafka.bootstrap_servers` | `localhost:9092` | Kafka broker address |
| `realtime.kafka.client_id` | `sports-rt` | Kafka client identifier |
| `realtime.kafka.group_id` | `sports-rt-group` | Default consumer group |

### WebSocket

**Class:** `WebSocketConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.websocket.url` | `wss://api.elections.kalshi.com/trade-api/ws/v2` | Kalshi WebSocket URL |
| `realtime.websocket.channels` | `[ticker, trade, orderbook_delta, fill, market_lifecycle_v2]` | Channels to subscribe |
| `realtime.websocket.reconnect_delay_initial` | 1.0 | Initial reconnect delay (seconds) |
| `realtime.websocket.reconnect_delay_max` | 60.0 | Maximum reconnect delay (seconds) |
| `realtime.websocket.ping_interval` | 10.0 | WebSocket ping interval (seconds) |

### VPIN

**Class:** `VPINConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.vpin.bucket_size` | 50 | Contracts per volume bucket |
| `realtime.vpin.n_buckets` | 50 | Number of buckets in rolling window |
| `realtime.vpin.threshold_elevated` | 0.3 | VPIN level triggering spread widening |
| `realtime.vpin.threshold_critical` | 0.6 | VPIN level triggering kill switch L2 |

### Entropy Filter

**Class:** `EntropyConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.entropy.min_price` | 0.30 | Minimum YES price to process |
| `realtime.entropy.max_price` | 0.70 | Maximum YES price to process |

Markets outside this range have low information entropy and are skipped.

### Market Maker

**Class:** `MarketMakerConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.market_maker.enabled` | false | Enable/disable market making |
| `realtime.market_maker.gamma` | 0.1 | Risk aversion parameter |
| `realtime.market_maker.kappa` | 1.5 | Order arrival intensity |
| `realtime.market_maker.sigma_window` | 100 | Ticks for volatility estimation |
| `realtime.market_maker.min_spread_cents` | 2 | Minimum bid-ask spread (cents) |
| `realtime.market_maker.max_position` | 50 | Maximum contracts per side |

### Risk

**Class:** `RiskConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.risk.max_position_per_market` | 100 | Max contracts per market |
| `realtime.risk.max_total_exposure` | 5000.0 | Max total dollar exposure |
| `realtime.risk.daily_loss_limit` | 500.0 | Daily loss limit (triggers cancel all) |
| `realtime.risk.emergency_loss_limit` | 1000.0 | Emergency loss (triggers shutdown) |
| `realtime.risk.gtd_expiry_minutes` | 10 | Good-Til-Date order expiry |

### Kelly (Real-Time)

**Class:** `KellyConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.kelly.n_simulations` | 10000 | Monte Carlo samples for empirical Kelly |
| `realtime.kelly.fraction` | 0.25 | Fractional Kelly multiplier |
| `realtime.kelly.max_bankroll_pct` | 0.20 | Maximum bet as fraction of bankroll |

### Fees

**Class:** `FeeConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.fees.taker_fee_cents` | 7.0 | Kalshi taker fee per contract (cents) |
| `realtime.fees.maker_rebate_cents` | 0.0 | Maker rebate (cents, currently 0) |
| `realtime.fees.slippage_cents` | 1.0 | Expected slippage per contract (cents) |
| `realtime.fees.min_tradable_edge` | 0.03 | Minimum edge after fees to trade (3%) |

### Telegram

**Class:** `TelegramConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.telegram.enabled` | false | Enable Telegram alerts |

Bot token and chat ID come from environment variables (see below).

### Target Series

**Class:** `TargetSeriesConfig`

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.target_series.series_tickers` | `[KXNBA, KXNFL, KXMLB, KXNHL, KXSOCCER, KXMMA]` | Kalshi series to monitor |

### Top-Level Real-Time Settings

| Key | Default | Description |
|-----|---------|-------------|
| `realtime.model_cache_refresh_seconds` | 300 | How often to reload models from DuckDB |
| `realtime.paper_mode` | true | Paper trading (no real orders) |
| `realtime.discovery_interval_seconds` | 300 | How often to discover new markets |
| `realtime.bankroll` | 10000.0 | Starting bankroll for Kelly sizing |

## Environment Variables

Set in `.env` or shell environment:

| Variable | Description |
|----------|-------------|
| `ENVIRONMENT` | Environment name (default: `dev`). Loads `settings.{env}.yaml` overlay |
| `KALSHI_API_KEY_ID` | Kalshi API key ID for authentication |
| `KALSHI_PRIVATE_KEY_PATH` | Path to RSA private key for Kalshi WS auth |
| `SLACK_WEBHOOK_URL` | Slack webhook for batch alerts |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for real-time alerts |
| `TELEGRAM_CHAT_ID` | Telegram chat ID for real-time alerts |

## Per-Environment Overlays

Create `config/settings.{env}.yaml` to override specific values. Deep-merged with base config.

Example `config/settings.prod.yaml`:

```yaml
realtime:
  paper_mode: false
  bankroll: 25000.0
  risk:
    daily_loss_limit: 1000.0
    emergency_loss_limit: 2500.0
  fees:
    min_tradable_edge: 0.05
```

Set `ENVIRONMENT=prod` to activate.

## Tuning Guide

### Conservative Settings

Reduce risk for initial deployment:

```yaml
realtime:
  kelly:
    fraction: 0.10          # 10% Kelly (very conservative)
    max_bankroll_pct: 0.05   # 5% max per bet
  fees:
    min_tradable_edge: 0.05  # Only trade 5%+ edges
  risk:
    max_position_per_market: 25
    max_total_exposure: 1000.0
    daily_loss_limit: 100.0
```

### Aggressive Settings

After validating model calibration:

```yaml
realtime:
  kelly:
    fraction: 0.50
    max_bankroll_pct: 0.20
  fees:
    min_tradable_edge: 0.03
  entropy:
    min_price: 0.20          # Wider price range
    max_price: 0.80
```

See also: [Architecture](architecture.md) | [Real-Time System](realtime.md) | [Development](development.md)
