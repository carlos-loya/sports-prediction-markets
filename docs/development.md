# Development

Developer setup, testing, linting, and project conventions.

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — fast Python package manager
- **Docker** — for Kafka, PostgreSQL, Airflow (optional for local development)

## Setup

```bash
# Clone the repo
git clone <repo-url>
cd sports-prediction-markets

# Install all dependencies (production + dev)
make install-dev

# Initialize the DuckDB database
make init-db

# Verify everything works
make check
```

## Running Tests

258 tests across unit and integration suites.

```bash
# Run all tests
make test

# Run with coverage report
make test-cov
# Open htmlcov/index.html in browser

# Run a specific test file
uv run pytest tests/unit/realtime/test_edge_processor.py

# Run a specific test
uv run pytest tests/unit/realtime/test_edge_processor.py::test_evaluate_produces_edge -v

# Run only unit tests
uv run pytest tests/unit/

# Run tests matching a keyword
uv run pytest -k "elo"

# Run with full output
uv run pytest -v --tb=long
```

### Test Markers

```bash
# Skip slow tests
uv run pytest -m "not slow"

# Run only integration tests
uv run pytest -m integration
```

### Test Structure

```
tests/
├── conftest.py                    # Shared fixtures
├── unit/
│   ├── analytics/
│   │   └── test_models.py         # Elo, Poisson, ensemble, calibration
│   ├── backtesting/
│   │   ├── test_calibration.py    # Calibration analysis functions
│   │   └── test_replayer.py       # TradeStreamReplayer
│   ├── edge_detection/
│   │   └── test_detector.py       # Batch edge detector
│   ├── extractors/
│   │   ├── test_kalshi_extractor.py
│   │   └── test_nba_extractors.py
│   ├── loaders/
│   │   └── test_becker_views.py
│   ├── realtime/
│   │   ├── test_app_wiring.py     # End-to-end wiring
│   │   ├── test_auth.py           # WebSocket auth
│   │   ├── test_bayesian.py       # Bayesian updater
│   │   ├── test_config.py         # Config loading
│   │   ├── test_discovery.py      # Market discovery
│   │   ├── test_edge_processor.py # EdgeProcessor pipeline
│   │   ├── test_empirical_kelly.py# Monte Carlo Kelly
│   │   ├── test_entropy.py        # Entropy filter
│   │   ├── test_events.py         # Event serialization
│   │   ├── test_fee_model.py      # Fee calculations
│   │   ├── test_kafka.py          # Producer/consumer
│   │   ├── test_market_maker.py   # Avellaneda-Stoikov
│   │   ├── test_messages.py       # WS message parsing
│   │   ├── test_model_loader.py   # Model cache loading
│   │   ├── test_orderbook.py      # Orderbook sync
│   │   ├── test_risk.py           # Risk manager + kill switch
│   │   ├── test_spread_monitor.py # Spread monitoring
│   │   ├── test_trade_logger.py   # Trade logging
│   │   └── test_vpin.py           # VPIN calculator
│   └── transformers/
│       ├── test_entity_matcher.py
│       └── test_transformers.py
└── integration/
```

## Linting and Formatting

```bash
# Lint (check for issues)
make lint

# Auto-format
make format

# Both lint + test
make check
```

### Ruff Configuration

From `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py311"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]
```

Rules enabled:
- **E** — pycodestyle errors
- **F** — pyflakes
- **I** — isort (import ordering)
- **N** — pep8-naming
- **W** — pycodestyle warnings
- **UP** — pyupgrade (modern Python syntax)

## Project Conventions

### Logging

Use **structlog** everywhere. Never use `print()` or stdlib `logging` directly.

```python
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

log.info("event_name", key1=value1, key2=value2)
log.warning("something_wrong", detail="...")
log.exception("error_occurred")  # includes traceback
```

### Configuration

All configuration flows through `get_settings()`. Never hardcode values that belong in config.

```python
from sports_pipeline.config import get_settings

settings = get_settings()
k_factor = settings.models.elo.k_factor_soccer
```

### Schema Validation

- **Pydantic** for configuration and event models
- **Pandera** for DataFrame schema enforcement in transformers

### Data Layer

- Bronze/silver use Parquet files on disk
- Gold layer uses DuckDB exclusively
- All DuckDB access goes through `DuckDBLoader`
- Schema defined in `loaders/views.py`

### Async

The real-time system is fully async using `asyncio`. Use `async/await` throughout.
Kafka uses `aiokafka`, HTTP uses `aiohttp`, WebSocket uses `websockets`.

## Adding a New Probability Model

1. Create `src/sports_pipeline/analytics/your_model.py`
2. Extend `BaseProbabilityModel`:

```python
from sports_pipeline.analytics.base import BaseProbabilityModel

class YourModel(BaseProbabilityModel):
    @property
    def name(self) -> str:
        return "your_model"

    def predict(self, **kwargs) -> dict[str, float]:
        # Return probabilities that sum to ~1.0
        return {"home_win": 0.55, "away_win": 0.45}
```

3. Add to ensemble in the edge detection DAG or model loader
4. Add unit tests in `tests/unit/analytics/`
5. Add any new config to `ModelsConfig` in `config.py` and defaults in `settings.yaml`

## Adding a New Real-Time Processor

1. Create `src/sports_pipeline/realtime/processors/your_processor.py`
2. Follow the `evaluate(event) -> result` pattern used by `EdgeProcessor`
3. Wire into `app.py`:
   - Create a handler function
   - Create a Kafka consumer with a dedicated consumer group
   - Add to the tasks list
4. Add unit tests in `tests/unit/realtime/`
5. Add any new config to `RealtimeConfig` in `realtime/config.py`

## Docker Services

```bash
# Start everything (Kafka, PostgreSQL, Airflow)
make airflow-up

# Start just Kafka + UI
make kafka-up

# Stop everything
make airflow-down
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8081 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |
| Kafka | localhost:9092 | — |

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/init_duckdb.py` | Initialize DuckDB schema |
| `scripts/scan_edges.py` | Run batch edge detection |
| `scripts/backtest.py` | Run backtest on resolved edges |
| `scripts/run_realtime.py` | Start real-time pipeline |
| `scripts/download_becker.py` | Download Becker dataset |
| `scripts/run_becker_backtest.py` | Run Becker historical backtest |
| `scripts/backfill.py` | Backfill historical data |

See also: [Architecture](architecture.md) | [Configuration](configuration.md)
