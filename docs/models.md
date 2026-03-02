# Probability Models

Statistical models for predicting sports outcomes and detecting market mispricings.

## Model Hierarchy

All models extend `BaseProbabilityModel` (`analytics/base.py`), which requires:
- `predict(**kwargs) -> dict[str, float]` — returns outcome labels mapped to probabilities
- `name -> str` — model identifier for logging and tracking

```
BaseProbabilityModel (ABC)
├── EloModel            # Elo rating system (soccer + basketball)
├── PoissonModel        # Poisson goal model (soccer)
├── PaceAdjustedModel   # Efficiency ratings (NBA)
├── PlayerPropModel     # Player stat distributions
├── LogisticModel       # Logistic regression (feature-based)
└── EnsembleModel       # Weighted average of sub-models
```

## Elo Rating System

**File:** `analytics/elo.py`

Maintains per-team ratings and computes win probabilities from rating differences.

### Expected Score

```
E_A = 1 / (1 + 10^((R_B - R_A) / 400))
```

Where `R_A` includes home advantage (added to the home team's rating before comparison).

### Rating Update

```
Δ = K × M × (S_actual - E_expected)
```

- **K-factor**: soccer=32, NBA=20
- **M** (goal difference multiplier): 1.0 for 1-goal margin, 1.5 for 2, `(11 + diff) / 8` for 3+
- **S_actual**: 1 (win), 0.5 (draw), 0 (loss)

### Home Advantage

Added to the home team's raw rating before computing expected score:
- Soccer: +65 Elo points
- NBA: +100 Elo points

### Soccer Draw Estimation

Draw probability is estimated as a function of rating closeness:

```
draw_base = 0.26
draw_adj = max(0, draw_base - |rating_diff| / 2000)
home_win = E_home × (1 - draw_prob)
away_win = (1 - E_home) × (1 - draw_prob)
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `initial_rating` | 1500 | Starting Elo for unknown teams |
| `k_factor_soccer` | 32 | Update speed for soccer |
| `k_factor_nba` | 20 | Update speed for NBA |
| `home_advantage_soccer` | 65 | Elo points added to home team |
| `home_advantage_nba` | 100 | Elo points added to home team |

## Poisson Model

**File:** `analytics/poisson.py`

Models goals scored as independent Poisson processes, fit from historical match data.

### Attack/Defense Strengths

For each team, compute from historical matches:

```
attack_strength  = avg_goals_scored / league_avg_goals
defense_strength = avg_goals_conceded / league_avg_goals
```

### Expected Goals

```
home_λ = home_attack × away_defense × league_avg × home_advantage_factor
away_λ = away_attack × home_defense × league_avg
```

Home advantage factor is 1.15 (15% boost to home expected goals).

### Match Outcome Probabilities

Enumerate all scorelines up to `max_goals=7`:

```
P(h, a) = Poisson(h; home_λ) × Poisson(a; away_λ)
```

Sum into home_win (h > a), draw (h == a), away_win (h < a).

### Over/Under

For a given total line (e.g., 2.5):

```
total_λ = home_λ + away_λ
P(under) = CDF_Poisson(floor(line), total_λ)
P(over)  = 1 - P(under)
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `league_avg_goals` | 1.35 | Average goals per team per match |

## Pace-Adjusted Model (NBA)

**File:** `analytics/pace_adjusted.py`

Uses offensive/defensive efficiency ratings and pace to predict scores.

### Score Prediction

```
game_pace   = (home_pace + away_pace) / 2
home_eff    = (home_off_rating + away_def_rating) / 2
away_eff    = (away_off_rating + home_def_rating) / 2
home_score  = home_eff × game_pace / 100 + 1.5   # home court
away_score  = away_eff × game_pace / 100 - 1.5
```

### Win Probability

Logistic mapping from expected margin:

```
P(home_win) = 1 / (1 + exp(-0.15 × margin))
```

Each point of margin approximates ~3% win probability shift.

### Over/Under

Normal approximation with std dev = 15 points:

```
z = (line - predicted_total) / 15
P(under) = Φ(z)
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `league_avg_pace` | 100.0 | League average possessions per game |

## Player Prop Model

**File:** `analytics/player_props.py`

Models individual player stat distributions for prop market predictions.

### Distribution Fitting

Fits normal distributions from historical game logs (minimum 5 games):

```
mean = sample_mean(values)
std  = sample_std(values, ddof=1)
```

### Prediction

```
z = (line - mean) / std
P(under) = Φ(z)
P(over)  = 1 - Φ(z)
```

### Matchup Adjustment

Adjusts mean based on opponent defensive rating:

```
adj_mean = mean × (opp_def_rating / league_avg_def_rating)
```

## Logistic Regression Model

**File:** `analytics/logistic.py`

Feature-based logistic regression using scikit-learn.

### Features

1. Elo difference (home - away)
2. Form difference (recent points)
3. xG difference
4. H2H home win percentage
5. Home indicator

Soccer uses multinomial (3-class: home/draw/away). Basketball uses binary.

## Ensemble Model

**File:** `analytics/ensemble.py`

Combines sub-models via weighted average.

### Prediction

```
P(outcome) = Σ (w_i × P_i(outcome)) / Σ w_i
```

Normalized so probabilities sum to 1.0.

### Adaptive Weights

Weights can be updated from Brier scores (lower = better):

```
w_i = 1 / brier_score_i
```

## Edge Detection

An "edge" is the difference between model probability and market implied probability.

### Pipeline (Batch)

**File:** `edge_detection/detector.py`

1. Load model predictions from DuckDB
2. Load Kalshi market snapshots
3. Match markets to model predictions
4. Compute `edge = model_prob - market_prob`
5. Apply filters (min volume, min time to close, min edge)
6. Size with Kelly criterion
7. Store in `gold.edge_signals`

### Pipeline (Real-Time)

**File:** `realtime/processors/edge_processor.py`

For each tick event:

1. **Model cache lookup** — get cached model probability for this ticker
2. **Entropy filter** — reject if price outside [0.30, 0.70] range
3. **Raw edge** — `model_prob - market_prob` (take absolute value, determine side)
4. **Fee adjustment** — `tradable_edge = raw_edge - taker_fee - slippage`
5. **Tradability check** — reject if `tradable_edge < min_tradable_edge` (3%)
6. **Kelly sizing** — empirical Kelly via Monte Carlo
7. **Confidence classification** — high (>=10%), medium (>=7%), low (<7%)

Every evaluation produces an `EdgeEvent` — non-rejected edges have sizing info, rejected edges record the reject reason.

### Entropy Filter

**File:** `realtime/processors/entropy_filter.py`

Binary entropy measures information content:

```
H(p) = -p·log₂(p) - (1-p)·log₂(1-p)
```

Maximum at p=0.50 (1 bit). Markets near 0 or 1 have low entropy and little informational value. The filter passes only markets with YES price in `[min_price, max_price]` (default `[0.30, 0.70]`).

### Fee Model

**File:** `realtime/sizing/fee_model.py`

```
tradable_edge = raw_edge - (taker_fee_cents / 100) - (slippage_cents / 100)
```

Default fees: 7c taker fee + 1c slippage = 8% total cost.

A trade is worthwhile when `tradable_edge >= min_tradable_edge` (default 3%).

## Kelly Criterion

### Standard Kelly

```
f* = edge / (odds × market_price)
```

Where `odds = (1 / market_price) - 1` for binary markets.

Applied as fractional Kelly: `f = f* × fraction` (default 0.25).

### Empirical Kelly (Monte Carlo)

**File:** `realtime/sizing/empirical_kelly.py`

When model uncertainty is known (e.g., from ensemble disagreement):

1. Sample `n_simulations` (10,000) model probabilities from `N(model_prob, uncertainty)`
2. Compute Kelly fraction for each sample
3. Take mean of positive Kelly values
4. Apply CV penalty: `cv_penalty = max(0, 1 - CV(positive_edges))`
5. Final: `f = mean_kelly × cv_penalty × fraction`

```
f_empirical = mean(positive_kellys) × (1 - CV_edge) × fraction
```

Capped at `max_bankroll_pct` (default 20%).

Falls back to standard Kelly when `model_uncertainty <= 0`.

## Calibration

**File:** `analytics/calibration.py`

### Brier Score

```
BS = (1/n) × Σ(p_i - o_i)²
```

Lower is better. Perfect = 0, worst = 1. A constant 0.5 prediction scores 0.25.

### Log Loss

```
LL = -(1/n) × Σ[o_i·ln(p_i) + (1-o_i)·ln(1-p_i)]
```

Heavily penalizes confident wrong predictions.

### Expected Calibration Error (ECE)

Bins predictions into `n_bins` (default 10) by predicted probability. For each bin:

```
ECE = Σ (bin_size / total) × |bin_accuracy - bin_confidence|
```

### Isotonic Calibration

`IsotonicCalibrator` uses scikit-learn's `IsotonicRegression` to fit a monotonic mapping from raw model probabilities to calibrated probabilities. Trained on historical predictions vs outcomes.

## VPIN (Volume-Synchronized Probability of Informed Trading)

**File:** `realtime/processors/vpin.py`

Measures order flow toxicity using volume buckets:

```
VPIN = Σ|buy_vol_i - sell_vol_i| / Σ(buy_vol_i + sell_vol_i)
```

Over a rolling window of `n_buckets` (default 50) volume buckets of `bucket_size` (default 50) contracts each.

Trade classification uses the **tick rule**: price > previous = buyer-initiated, price < previous = seller-initiated. Explicit `taker_side` from the exchange overrides this.

| Threshold | Level | Action |
|-----------|-------|--------|
| >= 0.30 | Elevated | Widen spreads |
| >= 0.60 | Critical | Cancel all orders (kill switch L2) |

See also: [Architecture](architecture.md) | [Real-Time System](realtime.md) | [Backtesting](backtesting.md)
