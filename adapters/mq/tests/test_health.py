from mq_adapter.health import classify_qmgr_status, classify_queue_depth
from mq_adapter.models import HealthSeverity


def test_queue_depth_ok_when_well_under_threshold():
    severity, pct = classify_queue_depth(curdepth=20, maxdepth=1000, warn_percent=80)
    assert severity == HealthSeverity.OK
    assert pct == 2.0


def test_queue_depth_warn_at_configured_threshold():
    severity, pct = classify_queue_depth(curdepth=850, maxdepth=1000, warn_percent=80)
    assert severity == HealthSeverity.WARN
    assert pct == 85.0


def test_queue_depth_critical_above_95_regardless_of_warn_threshold():
    severity, pct = classify_queue_depth(curdepth=960, maxdepth=1000, warn_percent=80)
    assert severity == HealthSeverity.CRITICAL
    assert pct == 96.0


def test_queue_depth_ok_when_maxdepth_unset():
    severity, pct = classify_queue_depth(curdepth=0, maxdepth=0)
    assert severity == HealthSeverity.OK
    assert pct == 0.0


def test_qmgr_status_ok_when_running():
    assert classify_qmgr_status("running") == HealthSeverity.OK
    assert classify_qmgr_status("Running") == HealthSeverity.OK  # case-insensitive


def test_qmgr_status_warn_when_starting():
    assert classify_qmgr_status("starting") == HealthSeverity.WARN


def test_qmgr_status_critical_when_ended():
    assert classify_qmgr_status("ended unexpectedly") == HealthSeverity.CRITICAL
    assert classify_qmgr_status("ended normally") == HealthSeverity.CRITICAL


def test_qmgr_status_critical_when_unknown():
    assert classify_qmgr_status(None) == HealthSeverity.CRITICAL
