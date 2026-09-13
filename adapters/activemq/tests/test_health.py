from activemq_adapter.health import classify_broker_state, classify_usage


def test_classify_usage_ok_below_warn_threshold():
    severity, pct = classify_usage(10, 100)
    assert severity == "ok"
    assert pct == 10.0


def test_classify_usage_warn_at_threshold():
    severity, _ = classify_usage(80, 100)
    assert severity == "warn"


def test_classify_usage_critical_above_95_percent():
    severity, _ = classify_usage(96, 100)
    assert severity == "critical"


def test_classify_usage_unbounded_is_ok():
    severity, pct = classify_usage(1_000_000, 0)
    assert severity == "ok"
    assert pct == 0.0


def test_classify_broker_state_not_started_is_critical():
    assert classify_broker_state(False, False) == "critical"


def test_classify_broker_state_started_but_not_active_is_warn():
    # A passive/backup node in a replicated pair — up but not serving yet.
    assert classify_broker_state(True, False) == "warn"


def test_classify_broker_state_started_and_active_is_ok():
    assert classify_broker_state(True, True) == "ok"
