"""Calibration analysis for backtesting results.

Analyzes replay results to answer:
- When model says X% edge, how often is it right?
- What is the actual model error distribution?
- What thresholds maximize risk-adjusted return?
- Does VPIN predict price moves?
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def edge_calibration(results: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
    """Bin edges by magnitude and measure actual win rate per bin.

    Answers: 'When model says 5% edge, how often is it right?'

    Args:
        results: DataFrame from TradeStreamReplayer.replay() with 'raw_edge' and 'won' columns.
        n_bins: Number of edge bins.

    Returns:
        DataFrame with columns: edge_bin, count, win_rate, avg_pnl, avg_edge
    """
    if results.empty or "raw_edge" not in results.columns:
        return pd.DataFrame(columns=["edge_bin", "count", "win_rate", "avg_pnl", "avg_edge"])

    df = results.copy()
    df["edge_bin"] = pd.cut(df["raw_edge"], bins=n_bins)

    grouped = df.groupby("edge_bin", observed=True).agg(
        count=("won", "size"),
        win_rate=("won", "mean"),
        avg_pnl=("pnl", "mean"),
        avg_edge=("raw_edge", "mean"),
    ).reset_index()

    log.info("edge_calibration_complete", bins=len(grouped))
    return grouped


def model_uncertainty(results: pd.DataFrame) -> dict[str, float]:
    """Measure actual model error distribution.

    Computes the difference between model probability and realized outcome
    (1 for yes, 0 for no), providing the real sigma for Kelly sizing.

    Args:
        results: DataFrame with 'model_prob' and 'result' columns.

    Returns:
        Dict with mean_error, std_error, mae, rmse.
    """
    if results.empty or "model_prob" not in results.columns:
        return {"mean_error": 0.0, "std_error": 0.0, "mae": 0.0, "rmse": 0.0}

    df = results.copy()

    # Deduplicate to one row per market for calibration
    if "ticker" in df.columns:
        df = df.drop_duplicates(subset=["ticker"], keep="first")

    # Convert result to numeric outcome
    df["outcome"] = (df["result"] == "yes").astype(float)

    errors = df["model_prob"] - df["outcome"]
    abs_errors = errors.abs()

    return {
        "mean_error": round(float(errors.mean()), 4),
        "std_error": round(float(errors.std()), 4),
        "mae": round(float(abs_errors.mean()), 4),
        "rmse": round(float(np.sqrt((errors**2).mean())), 4),
    }


def optimal_thresholds(results: pd.DataFrame) -> dict[str, float]:
    """Find min_edge and entropy bounds that maximize risk-adjusted return.

    Tests a grid of minimum edge thresholds and reports which
    value of min_edge produces the best Sharpe-like ratio.

    Args:
        results: DataFrame from replay with 'raw_edge', 'pnl', 'bankroll' columns.

    Returns:
        Dict with best_min_edge, best_sharpe, best_hit_rate, best_pnl.
    """
    if results.empty or "raw_edge" not in results.columns:
        return {
            "best_min_edge": 0.0,
            "best_sharpe": 0.0,
            "best_hit_rate": 0.0,
            "best_pnl": 0.0,
        }

    edge_thresholds = np.arange(0.01, 0.20, 0.01)
    best = {"threshold": 0.0, "sharpe": -np.inf, "hit_rate": 0.0, "pnl": 0.0}

    for threshold in edge_thresholds:
        subset = results[results["raw_edge"] >= threshold]
        if len(subset) < 10:
            continue

        total_pnl = subset["pnl"].sum()
        hit_rate = (subset["won"]).mean()

        # Sharpe-like ratio: mean pnl / std pnl
        if subset["pnl"].std() > 0:
            sharpe = subset["pnl"].mean() / subset["pnl"].std()
        else:
            sharpe = 0.0

        if sharpe > best["sharpe"]:
            best = {
                "threshold": float(threshold),
                "sharpe": float(sharpe),
                "hit_rate": float(hit_rate),
                "pnl": float(total_pnl),
            }

    result = {
        "best_min_edge": round(best["threshold"], 3),
        "best_sharpe": round(best["sharpe"], 4),
        "best_hit_rate": round(best["hit_rate"], 4),
        "best_pnl": round(best["pnl"], 2),
    }
    log.info("optimal_thresholds", **result)
    return result


def vpin_effectiveness(
    trades_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    bucket_size: int = 50,
    n_buckets: int = 50,
) -> pd.DataFrame:
    """Run VPIN calculation on historical trade streams and check predictive power.

    Computes volume-synchronized probability of informed trading (VPIN)
    for each market and checks if VPIN spikes precede large price moves.

    Args:
        trades_df: DataFrame of trades with 'ticker', 'count', 'taker_side', 'yes_price'.
        markets_df: DataFrame of settled markets with 'ticker', 'result'.
        bucket_size: Volume per VPIN bucket.
        n_buckets: Number of buckets in rolling window.

    Returns:
        DataFrame with columns: ticker, max_vpin, final_price_move, result, vpin_predicted
    """
    if trades_df.empty or markets_df.empty:
        return pd.DataFrame(
            columns=["ticker", "max_vpin", "final_price_move", "result", "vpin_predicted"]
        )

    results = []
    settled_tickers = set(markets_df["ticker"].unique())

    for ticker in trades_df["ticker"].unique():
        if ticker not in settled_tickers:
            continue

        ticker_trades = trades_df[trades_df["ticker"] == ticker].sort_values("created_time")
        if len(ticker_trades) < bucket_size * 2:
            continue

        # Compute VPIN
        vpin_values = _compute_vpin(ticker_trades, bucket_size, n_buckets)
        if not vpin_values:
            continue

        max_vpin = max(vpin_values)

        # Price move: difference between first and last trade price
        first_price = float(ticker_trades["yes_price"].iloc[0])
        last_price = float(ticker_trades["yes_price"].iloc[-1])
        if first_price > 1:
            first_price /= 100.0
        if last_price > 1:
            last_price /= 100.0
        price_move = abs(last_price - first_price)

        market_row = markets_df[markets_df["ticker"] == ticker].iloc[0]
        result = market_row["result"]

        # VPIN > 0.3 "predicts" a significant move (> 10 cents)
        vpin_predicted = max_vpin > 0.3 and price_move > 0.10

        results.append({
            "ticker": ticker,
            "max_vpin": round(max_vpin, 4),
            "final_price_move": round(price_move, 4),
            "result": result,
            "vpin_predicted": vpin_predicted,
        })

    result_df = pd.DataFrame(results)
    log.info("vpin_analysis_complete", markets=len(result_df))
    return result_df


def _compute_vpin(
    trades: pd.DataFrame, bucket_size: int, n_buckets: int
) -> list[float]:
    """Compute VPIN values from a trade stream.

    VPIN = abs(buy_volume - sell_volume) / total_volume per bucket window.
    """
    buy_volume = 0
    sell_volume = 0
    current_bucket = 0
    bucket_imbalances: list[float] = []
    vpin_values: list[float] = []

    for _, trade in trades.iterrows():
        count = int(trade.get("count", 1))
        side = trade.get("taker_side", "")

        if side == "yes":
            buy_volume += count
        else:
            sell_volume += count

        current_bucket += count

        if current_bucket >= bucket_size:
            total = buy_volume + sell_volume
            if total > 0:
                imbalance = abs(buy_volume - sell_volume) / total
            else:
                imbalance = 0.0
            bucket_imbalances.append(imbalance)

            # Rolling VPIN over n_buckets
            if len(bucket_imbalances) >= n_buckets:
                window = bucket_imbalances[-n_buckets:]
                vpin = sum(window) / len(window)
                vpin_values.append(vpin)

            # Reset bucket
            buy_volume = 0
            sell_volume = 0
            current_bucket = 0

    return vpin_values


def generate_calibration_report(
    results: pd.DataFrame,
    edge_cal: pd.DataFrame,
    uncertainty: dict[str, float],
    thresholds: dict[str, float],
) -> str:
    """Generate a human-readable calibration report.

    Args:
        results: Full replay results DataFrame.
        edge_cal: Edge calibration DataFrame.
        uncertainty: Model uncertainty dict.
        thresholds: Optimal thresholds dict.

    Returns:
        Formatted report string.
    """
    lines = [
        "=" * 60,
        "BECKER DATASET CALIBRATION REPORT",
        "=" * 60,
        "",
    ]

    # Summary
    if not results.empty:
        lines.extend([
            f"Markets replayed: {results['ticker'].nunique()}",
            f"Total evaluations: {len(results)}",
            f"Overall hit rate: {results['won'].mean():.1%}",
            f"Total P&L: ${results['pnl'].sum():,.2f}",
            "",
        ])

    # Model uncertainty
    lines.extend([
        "--- Model Uncertainty ---",
        f"Mean error: {uncertainty.get('mean_error', 0):.4f}",
        f"Std error (sigma): {uncertainty.get('std_error', 0):.4f}",
        f"MAE: {uncertainty.get('mae', 0):.4f}",
        f"RMSE: {uncertainty.get('rmse', 0):.4f}",
        "",
    ])

    # Optimal thresholds
    lines.extend([
        "--- Optimal Thresholds ---",
        f"Best min_edge: {thresholds.get('best_min_edge', 0):.3f}",
        f"Best Sharpe: {thresholds.get('best_sharpe', 0):.4f}",
        f"Best hit rate: {thresholds.get('best_hit_rate', 0):.1%}",
        f"Best P&L: ${thresholds.get('best_pnl', 0):,.2f}",
        "",
    ])

    # Edge calibration table
    if not edge_cal.empty:
        lines.append("--- Edge Calibration ---")
        lines.append(f"{'Edge Bin':<25} {'Count':>6} {'Win%':>7} {'Avg PnL':>10}")
        lines.append("-" * 50)
        for _, row in edge_cal.iterrows():
            lines.append(
                f"{str(row['edge_bin']):<25} {int(row['count']):>6} "
                f"{row['win_rate']:>6.1%} {row['avg_pnl']:>10.2f}"
            )
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)
