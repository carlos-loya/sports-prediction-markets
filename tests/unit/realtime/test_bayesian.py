"""Tests for Bayesian updater."""

from __future__ import annotations

from sports_pipeline.realtime.processors.bayesian_updater import (
    BayesianUpdater,
    BayesianUpdaterManager,
)


class TestBayesianUpdater:
    def test_uniform_prior(self):
        bu = BayesianUpdater()
        assert bu.mean == 0.5

    def test_update_yes(self):
        bu = BayesianUpdater()
        bu.update(True)
        assert bu.mean > 0.5

    def test_update_no(self):
        bu = BayesianUpdater()
        bu.update(False)
        assert bu.mean < 0.5

    def test_variance_decreases_with_data(self):
        bu = BayesianUpdater()
        v0 = bu.variance
        for _ in range(10):
            bu.update(True)
        assert bu.variance < v0

    def test_weighted_update(self):
        bu1 = BayesianUpdater()
        bu2 = BayesianUpdater()
        bu1.update(True, weight=1.0)
        bu2.update(True, weight=5.0)
        assert bu2.mean > bu1.mean

    def test_update_with_price(self):
        bu = BayesianUpdater()
        bu.update_with_price(0.70, confidence=1.0)
        assert bu.mean > 0.5

    def test_reset(self):
        bu = BayesianUpdater()
        bu.update(True)
        bu.update(True)
        bu.reset()
        assert bu.mean == 0.5


class TestBayesianUpdaterManager:
    def test_manages_multiple(self):
        mgr = BayesianUpdaterManager()
        mgr.update("T1", True)
        mgr.update("T2", False)
        assert mgr.get("T1").mean > 0.5
        assert mgr.get("T2").mean < 0.5

    def test_remove(self):
        mgr = BayesianUpdaterManager()
        mgr.update("T1", True)
        mgr.remove("T1")
        assert mgr.get("T1").mean == 0.5
