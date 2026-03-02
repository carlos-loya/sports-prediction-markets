# Backtesting

Running backtests, using the Becker dataset, and interpreting calibration analysis.

## Overview

Two backtesting approaches:

1. **Batch backtesting** (`BacktestSimulator`) — replays resolved edge signals from DuckDB
2. **Historical replay** (`TradeStreamReplayer`) — replays Becker dataset trade streams through the full EdgeProcessor pipeline

## Batch Backtesting

**File:** `backtesting/simulator.py`

```bash
make backtest
```

Runs `scripts/backtest.py`, which:
1. Loads resolved edges from `gold.edge_signals` (where `resolved=TRUE` and `actual_outcome IS NOT NULL`)
2. For each edge, computes Kelly-sized bet amount
3. Resolves P&L against known outcome
4. Tracks running bankroll

### How P&L is Calculated

For YES side:
- Win: `pnl = bet_amount × (1/market_price - 1)`
- Loss: `pnl = -bet_amount`

For NO side:
- Win: `pnl = bet_amount × (1/no_price - 1)`
- Loss: `pnl = -bet_amount`

### Output

Returns a DataFrame with columns: timestamp, ticker, sport, market_type, model_name, edge, kelly_fraction, bet_amount, pnl, bankroll, won.

## Becker Dataset

Jon Becker's [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) dataset contains historical Kalshi trades and market metadata as Parquet files.

### Download

```bash
make download-becker
```

Runs `scripts/download_becker.py`, which downloads and extracts the dataset to `data/becker/`.

### Directory Structure

```
data/becker/
└── kalshi/
    ├── trades/
    │   └── *.parquet    # Historical trade data
    └── markets/
        └── *.parquet    # Market metadata (title, status, result)
```

### DuckDB Views

**File:** `loaders/becker_views.py`

Creates SQL views directly over the raw Parquet files using DuckDB's `read_parquet()` with glob patterns. No data copying — predicate pushdown filters to sports tickers efficiently.

Sports ticker prefixes: `KXNBA`, `KXNFL`, `KXMLB`, `KXNHL`, `KXSOC`, `KXMMA`

| View | Description |
|------|-------------|
| `gold.becker_trades` | All trades for sports tickers |
| `gold.becker_markets` | All markets for sports tickers |
| `gold.becker_settled` | Settled markets with known yes/no outcomes |

## Trade Stream Replay

**File:** `backtesting/replayer.py`

```bash
make becker-backtest
```

Runs `scripts/run_becker_backtest.py`.

### How It Works

For each settled market in the Becker dataset:

1. **Parse title** — extract team names from market title
2. **Compute model probability** — `EloModel.predict(home_team, away_team)` → `home_win` probability
3. **Set up EdgeProcessor** — create fresh `ModelCache` with the computed probability
4. **Load trades** — query `gold.becker_trades` for this ticker, ordered by time
5. **Replay** — for each trade, construct a `TickEvent` from the trade price and feed through `EdgeProcessor.evaluate()`
6. **Simulate bet** — for non-rejected edges, compute Kelly-sized bet from current bankroll
7. **Resolve** — compare `suggested_side` against known market outcome

### Parameters

| Parameter | Description |
|-----------|-------------|
| `sport_prefix` | Filter to sport (e.g., `"KXNBA"`). `None` = all sports |
| `max_markets` | Limit number of markets to replay |
| `start_date` | ISO date filter on `close_time` |
| `end_date` | ISO date filter on `close_time` |

### Output

DataFrame with columns: timestamp, ticker, title, sport, model_name, model_prob, market_prob, raw_edge, tradable_edge, kelly_fraction, suggested_side, confidence, bet_amount, pnl, bankroll, won, result.

## Calibration Analysis

**File:** `backtesting/calibration.py`

Four analysis functions that answer key questions about model performance.

### Edge Calibration

```python
edge_calibration(results, n_bins=10) -> DataFrame
```

**Question:** "When the model says 5% edge, how often is it right?"

Bins edges by magnitude and measures actual win rate per bin.

Output columns: edge_bin, count, win_rate, avg_pnl, avg_edge.

### Model Uncertainty

```python
model_uncertainty(results) -> dict
```

**Question:** "What is the actual model error distribution?"

Computes the difference between model probability and realized outcome (1 for yes, 0 for no). Deduplicates to one row per market.

Output: `mean_error`, `std_error`, `mae`, `rmse`.

The `std_error` is the real sigma that should be used for empirical Kelly sizing.

### Optimal Thresholds

```python
optimal_thresholds(results) -> dict
```

**Question:** "What min_edge threshold maximizes risk-adjusted return?"

Tests a grid of thresholds from 1% to 19% (1% steps). For each threshold, computes the Sharpe-like ratio (mean pnl / std pnl) on the subset of trades meeting that threshold.

Output: `best_min_edge`, `best_sharpe`, `best_hit_rate`, `best_pnl`.

### VPIN Effectiveness

```python
vpin_effectiveness(trades_df, markets_df, bucket_size=50, n_buckets=50) -> DataFrame
```

**Question:** "Does VPIN predict large price moves?"

For each settled market with sufficient trade volume:
1. Compute VPIN from the trade stream
2. Measure the total price move (first trade to last trade)
3. Check if VPIN > 0.3 preceded a move > 10 cents

Output columns: ticker, max_vpin, final_price_move, result, vpin_predicted.

## Calibration Report

```python
generate_calibration_report(results, edge_cal, uncertainty, thresholds) -> str
```

Generates a formatted text report combining all calibration metrics:

```
============================================================
BECKER DATASET CALIBRATION REPORT
============================================================

Markets replayed: 150
Total evaluations: 2340
Overall hit rate: 54.2%
Total P&L: $1,234.56

--- Model Uncertainty ---
Mean error: 0.0234
Std error (sigma): 0.1456
MAE: 0.1234
RMSE: 0.1567

--- Optimal Thresholds ---
Best min_edge: 0.070
Best Sharpe: 0.4567
Best hit rate: 58.3%
Best P&L: $2,345.67

--- Edge Calibration ---
Edge Bin                  Count  Win%     Avg PnL
--------------------------------------------------
(0.03, 0.05]                45  52.0%      -1.23
(0.05, 0.07]                38  55.3%       2.45
...

============================================================
```

## Performance Metrics

**File:** `backtesting/metrics.py`

`calculate_metrics(results)` computes:

| Metric | Description |
|--------|-------------|
| `total_trades` | Number of trades executed |
| `winning_trades` | Trades with positive P&L |
| `losing_trades` | Trades with negative P&L |
| `hit_rate` | Win rate |
| `total_pnl` | Sum of all P&L |
| `roi` | Return on initial bankroll |
| `avg_win` | Average winning trade P&L |
| `avg_loss` | Average losing trade P&L |
| `max_drawdown` | Maximum peak-to-trough decline |
| `sharpe_ratio` | Annualized Sharpe (x sqrt(252)) |
| `profit_factor` | Gross profit / gross loss |
| `initial_bankroll` | Starting capital |
| `final_bankroll` | Ending capital |

## Interpreting Results

### Good Signs

- **Edge calibration slope** — win rate increases with edge magnitude
- **Positive Sharpe** — consistent risk-adjusted returns
- **`std_error` < 0.15** — model uncertainty is manageable for Kelly sizing
- **Profit factor > 1.2** — winners meaningfully exceed losers

### Red Flags

- **Flat calibration** — win rate doesn't increase with edge → model isn't informative
- **`mean_error` far from 0** — systematic bias in model (overconfident or underconfident)
- **Best `min_edge` very high (>15%)** — model only works on extreme edges, missing volume
- **Max drawdown > 30%** — Kelly sizing too aggressive, reduce `kelly.fraction`

### Tuning from Results

| Finding | Action |
|---------|--------|
| Low hit rate at small edges | Increase `fees.min_tradable_edge` |
| High `std_error` | Increase `kelly.fraction` conservatism or increase `kelly.n_simulations` |
| Best `min_edge` differs from config | Update `edge_detection.min_edge_pct` in settings.yaml |
| VPIN predicts moves | Lower `vpin.threshold_elevated` for earlier spread widening |

See also: [Models](models.md) | [Configuration](configuration.md) | [Architecture](architecture.md)
