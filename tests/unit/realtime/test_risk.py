"""Tests for risk manager and kill switch."""

from __future__ import annotations

from sports_pipeline.realtime.config import RiskConfig
from sports_pipeline.realtime.events import RiskLevel
from sports_pipeline.realtime.risk.kill_switch import KillSwitch, KillSwitchState
from sports_pipeline.realtime.risk.risk_manager import RiskManager


class TestRiskManager:
    def _make_manager(self, **kwargs) -> RiskManager:
        config = RiskConfig(
            max_position_per_market=100,
            max_total_exposure=5000.0,
            daily_loss_limit=500.0,
            emergency_loss_limit=1000.0,
            **kwargs,
        )
        return RiskManager(config)

    def test_normal_state(self):
        mgr = self._make_manager()
        mgr.update_position("T1", 50)
        assert mgr.check() is None
        assert mgr.level == RiskLevel.NORMAL

    def test_position_limit_breach(self):
        mgr = self._make_manager()
        mgr.update_position("T1", 150)
        alert = mgr.check()
        assert alert is not None
        assert alert.level == RiskLevel.ELEVATED
        assert alert.ticker == "T1"
        assert alert.action == "cancel_ticker"

    def test_exposure_limit_breach(self):
        mgr = self._make_manager()
        for i in range(60):
            mgr.update_position(f"T{i}", 90)
        alert = mgr.check()
        assert alert is not None
        assert alert.level == RiskLevel.CRITICAL
        assert alert.action == "cancel_all"

    def test_daily_loss_breach(self):
        mgr = self._make_manager()
        mgr.update_pnl(-600.0)
        alert = mgr.check()
        assert alert is not None
        assert alert.level == RiskLevel.CRITICAL

    def test_reset_daily(self):
        mgr = self._make_manager()
        mgr.update_pnl(-600.0)
        mgr.reset_daily()
        assert mgr.state.daily_pnl == 0.0
        assert mgr.level == RiskLevel.NORMAL


class TestKillSwitch:
    def _make_switch(self) -> KillSwitch:
        return KillSwitch(
            config=RiskConfig(
                daily_loss_limit=500.0,
                emergency_loss_limit=1000.0,
            ),
            max_errors=3,
        )

    def test_initial_state(self):
        ks = self._make_switch()
        assert ks.is_active is True
        assert ks.state == KillSwitchState.ACTIVE

    def test_vpin_triggers_l2(self):
        ks = self._make_switch()
        alert = ks.on_vpin_update(0.7)
        assert alert is not None
        assert alert.level == RiskLevel.CRITICAL
        assert ks.state == KillSwitchState.TRIGGERED_L2

    def test_vpin_below_threshold(self):
        ks = self._make_switch()
        assert ks.on_vpin_update(0.3) is None
        assert ks.is_active is True

    def test_errors_trigger_l2(self):
        ks = self._make_switch()
        assert ks.on_error("err1") is None
        assert ks.on_error("err2") is None
        alert = ks.on_error("err3")
        assert alert is not None
        assert ks.state == KillSwitchState.TRIGGERED_L2

    def test_ws_disconnect_triggers_l3(self):
        ks = self._make_switch()
        alert = ks.on_ws_disconnect()
        assert alert.level == RiskLevel.EMERGENCY
        assert ks.state == KillSwitchState.TRIGGERED_L3
        assert alert.action == "shutdown"

    def test_emergency_loss_triggers_l3(self):
        ks = self._make_switch()
        alert = ks.on_daily_loss(-1500.0)
        assert alert is not None
        assert alert.level == RiskLevel.EMERGENCY
        assert ks.state == KillSwitchState.TRIGGERED_L3

    def test_daily_loss_triggers_l2(self):
        ks = self._make_switch()
        alert = ks.on_daily_loss(-600.0)
        assert alert is not None
        assert alert.level == RiskLevel.CRITICAL
        assert ks.state == KillSwitchState.TRIGGERED_L2

    def test_reset(self):
        ks = self._make_switch()
        ks.on_ws_disconnect()
        assert ks.state == KillSwitchState.TRIGGERED_L3
        ks.reset()
        assert ks.is_active is True
        assert len(ks.triggered_reasons) == 0
