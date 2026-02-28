"""DuckDB views for BI and edge analysis."""

from __future__ import annotations

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

-- Core sports tables
CREATE TABLE IF NOT EXISTS gold.soccer_matches (
    match_id VARCHAR PRIMARY KEY,
    season VARCHAR,
    league VARCHAR,
    match_date DATE,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    home_goals INTEGER,
    away_goals INTEGER,
    home_xg DOUBLE,
    away_xg DOUBLE,
    result VARCHAR,
    venue VARCHAR
);

CREATE TABLE IF NOT EXISTS gold.soccer_player_matches (
    match_id VARCHAR,
    season VARCHAR,
    league VARCHAR,
    match_date DATE,
    player_name VARCHAR NOT NULL,
    team VARCHAR NOT NULL,
    opponent VARCHAR,
    is_home BOOLEAN,
    minutes INTEGER DEFAULT 0,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    xg DOUBLE DEFAULT 0,
    xa DOUBLE DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gold.nba_games (
    game_id VARCHAR PRIMARY KEY,
    season VARCHAR,
    game_date DATE,
    home_team_id INTEGER NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team_id INTEGER NOT NULL,
    away_team VARCHAR NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    home_win BOOLEAN,
    total_points INTEGER
);

CREATE TABLE IF NOT EXISTS gold.nba_player_games (
    game_id VARCHAR,
    season VARCHAR,
    game_date DATE,
    player_id INTEGER NOT NULL,
    player_name VARCHAR NOT NULL,
    team_id INTEGER NOT NULL,
    team VARCHAR NOT NULL,
    is_home BOOLEAN,
    minutes DOUBLE DEFAULT 0,
    points INTEGER DEFAULT 0,
    rebounds INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    steals INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    turnovers INTEGER DEFAULT 0,
    field_goal_pct DOUBLE,
    three_point_pct DOUBLE,
    free_throw_pct DOUBLE,
    plus_minus DOUBLE DEFAULT 0
);

-- Kalshi market tables
CREATE TABLE IF NOT EXISTS gold.kalshi_market_snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    snapshot_timestamp TIMESTAMP NOT NULL,
    ticker VARCHAR NOT NULL,
    title VARCHAR,
    sport VARCHAR,
    market_type VARCHAR,
    matched_entity_id VARCHAR,
    yes_price DOUBLE NOT NULL,
    no_price DOUBLE NOT NULL,
    volume INTEGER,
    open_interest INTEGER,
    close_time TIMESTAMP
);

-- Edge detection tables
CREATE TABLE IF NOT EXISTS gold.edge_signals (
    signal_id VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    kalshi_ticker VARCHAR NOT NULL,
    market_title VARCHAR,
    sport VARCHAR NOT NULL,
    market_type VARCHAR NOT NULL,
    kalshi_implied_prob DOUBLE NOT NULL,
    model_prob DOUBLE NOT NULL,
    edge DOUBLE NOT NULL,
    edge_pct DOUBLE NOT NULL,
    confidence VARCHAR,
    model_name VARCHAR,
    kelly_fraction DOUBLE,
    suggested_side VARCHAR,
    reasoning VARCHAR,
    resolved BOOLEAN DEFAULT FALSE,
    actual_outcome BOOLEAN,
    was_profitable BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.model_performance (
    model_name VARCHAR NOT NULL,
    sport VARCHAR NOT NULL,
    market_type VARCHAR NOT NULL,
    evaluation_date DATE NOT NULL,
    brier_score DOUBLE,
    log_loss DOUBLE,
    calibration_error DOUBLE,
    total_predictions INTEGER,
    hit_rate DOUBLE,
    avg_edge DOUBLE,
    roi DOUBLE,
    PRIMARY KEY (model_name, sport, market_type, evaluation_date)
);

-- Elo ratings table
CREATE TABLE IF NOT EXISTS gold.elo_ratings (
    team VARCHAR NOT NULL,
    sport VARCHAR NOT NULL,
    rating DOUBLE NOT NULL,
    last_updated DATE,
    games_played INTEGER DEFAULT 0,
    PRIMARY KEY (team, sport)
);
"""

VIEW_DDL = """
-- Active edges for dashboard
CREATE OR REPLACE VIEW gold.v_active_edges AS
SELECT * FROM gold.edge_signals
WHERE resolved = FALSE AND ABS(edge) >= 0.05
ORDER BY ABS(edge) DESC;

-- Model leaderboard
CREATE OR REPLACE VIEW gold.v_model_leaderboard AS
SELECT model_name, sport, market_type,
    AVG(brier_score) AS avg_brier,
    AVG(hit_rate) AS avg_hit_rate,
    AVG(roi) AS avg_roi
FROM gold.model_performance
GROUP BY model_name, sport, market_type
ORDER BY avg_roi DESC;

-- Edge P&L tracking
CREATE OR REPLACE VIEW gold.v_edge_pnl AS
SELECT sport, market_type, model_name,
    COUNT(*) AS total_signals,
    SUM(CASE WHEN was_profitable THEN 1 ELSE 0 END) AS wins,
    ROUND(AVG(CASE WHEN was_profitable THEN 1.0 ELSE 0.0 END), 3) AS hit_rate,
    ROUND(AVG(edge), 3) AS avg_edge
FROM gold.edge_signals WHERE resolved = TRUE
GROUP BY sport, market_type, model_name;
"""


def init_schema(loader: DuckDBLoader | None = None) -> None:
    """Initialize the DuckDB schema with all tables and views."""
    loader = loader or DuckDBLoader()
    log.info("initializing_duckdb_schema")
    conn = loader.get_connection()
    try:
        conn.execute(SCHEMA_DDL)
        conn.execute(VIEW_DDL)
        log.info("duckdb_schema_initialized")
    finally:
        conn.close()


def refresh_views(loader: DuckDBLoader | None = None) -> None:
    """Refresh all views (re-create)."""
    loader = loader or DuckDBLoader()
    log.info("refreshing_views")
    conn = loader.get_connection()
    try:
        conn.execute(VIEW_DDL)
        log.info("views_refreshed")
    finally:
        conn.close()
