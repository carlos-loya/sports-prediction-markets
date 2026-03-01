"""DuckDB DDL for real-time tables."""

from __future__ import annotations

RT_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.trade_log (
    log_id VARCHAR,
    timestamp TIMESTAMP NOT NULL,
    ticker VARCHAR NOT NULL,
    model_prob DOUBLE,
    market_prob DOUBLE,
    raw_edge DOUBLE,
    tradable_edge DOUBLE,
    kelly_fraction DOUBLE,
    suggested_side VARCHAR,
    confidence VARCHAR,
    model_name VARCHAR,
    rejected BOOLEAN NOT NULL,
    reject_reason VARCHAR,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.rt_positions (
    ticker VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    avg_price DOUBLE,
    unrealized_pnl DOUBLE DEFAULT 0,
    realized_pnl DOUBLE DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, side)
);

CREATE TABLE IF NOT EXISTS gold.rt_orders (
    order_id VARCHAR PRIMARY KEY,
    ticker VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    action VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    count INTEGER NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending',
    source VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filled_at TIMESTAMP,
    cancelled_at TIMESTAMP
);
"""
