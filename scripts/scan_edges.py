#!/usr/bin/env python
"""One-off edge scan: pull current markets, run models, detect edges."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.utils.logging import setup_logging, get_logger

log = get_logger(__name__)

if __name__ == "__main__":
    setup_logging()
    log.info("edge_scan_started")

    # Import here to allow setup_logging first
    from sports_pipeline.edge_detection.detector import EdgeDetector
    from sports_pipeline.edge_detection.filters import EdgeFilter
    from sports_pipeline.edge_detection.kelly import KellyCriterion

    detector = EdgeDetector()
    edge_filter = EdgeFilter()
    kelly = KellyCriterion()

    # Run detection
    raw_edges = detector.detect_all()
    filtered = edge_filter.apply(raw_edges)

    for edge in filtered:
        edge["kelly_fraction"] = kelly.calculate(
            model_prob=edge["model_prob"],
            market_price=edge["kalshi_implied_prob"],
        )

    log.info("edge_scan_complete", total_edges=len(filtered))
    for edge in filtered:
        print(
            f"[{edge.get('confidence', '?')}] {edge.get('kalshi_ticker', '?')} | "
            f"Edge: {edge.get('edge', 0):.1%} | Kelly: {edge.get('kelly_fraction', 0):.1%} | "
            f"Side: {edge.get('suggested_side', '?')} | {edge.get('reasoning', '')}"
        )
