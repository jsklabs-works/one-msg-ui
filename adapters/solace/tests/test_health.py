from solace_adapter.health import classify_spool_usage, classify_vpn_connectivity
from solace_adapter.models import HealthSeverity


def test_spool_usage_ok_when_well_under_threshold():
    severity, pct = classify_spool_usage(used=200, max_quota=1000, warn_percent=80)
    assert severity == HealthSeverity.OK
    assert pct == 20.0


def test_spool_usage_warn_at_configured_threshold():
    severity, pct = classify_spool_usage(used=850, max_quota=1000, warn_percent=80)
    assert severity == HealthSeverity.WARN
    assert pct == 85.0


def test_spool_usage_critical_above_95_regardless_of_warn_threshold():
    severity, pct = classify_spool_usage(used=960, max_quota=1000, warn_percent=80)
    assert severity == HealthSeverity.CRITICAL
    assert pct == 96.0


def test_spool_usage_respects_custom_warn_percent_from_vpn_config():
    # A VPN configured with a stricter (lower) threshold should warn sooner.
    severity, _ = classify_spool_usage(used=650, max_quota=1000, warn_percent=60)
    assert severity == HealthSeverity.WARN


def test_spool_usage_ok_when_quota_unset():
    severity, pct = classify_spool_usage(used=0, max_quota=0)
    assert severity == HealthSeverity.OK
    assert pct == 0.0


def test_vpn_connectivity_ok_when_up_and_enabled():
    assert classify_vpn_connectivity("up", True) == HealthSeverity.OK


def test_vpn_connectivity_critical_when_down():
    assert classify_vpn_connectivity("down", True) == HealthSeverity.CRITICAL


def test_vpn_connectivity_critical_when_disabled():
    assert classify_vpn_connectivity("up", False) == HealthSeverity.CRITICAL


def test_vpn_connectivity_critical_when_state_unknown():
    assert classify_vpn_connectivity(None, None) == HealthSeverity.CRITICAL
