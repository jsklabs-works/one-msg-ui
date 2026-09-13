from rabbitmq_adapter.health import classify_node_health, classify_queue_depth


def test_classify_queue_depth_ok_below_warn_threshold():
    severity, pct = classify_queue_depth(10, 100)
    assert severity == "ok"
    assert pct == 10.0


def test_classify_queue_depth_warn_at_threshold():
    severity, _ = classify_queue_depth(80, 100)
    assert severity == "warn"


def test_classify_queue_depth_critical_above_95_percent():
    severity, _ = classify_queue_depth(96, 100)
    assert severity == "critical"


def test_classify_queue_depth_unbounded_queue_is_ok():
    # No x-max-length set — most RabbitMQ queues are like this.
    severity, pct = classify_queue_depth(1_000_000, 0)
    assert severity == "ok"
    assert pct == 0.0


def test_classify_node_health_not_running_is_critical():
    assert classify_node_health(False, False, False) == "critical"


def test_classify_node_health_alarm_is_warn_not_critical():
    # Publishers are throttled/blocked under an alarm, but the node itself
    # is still up — a warning, not the same severity as a dead node.
    assert classify_node_health(True, True, False) == "warn"
    assert classify_node_health(True, False, True) == "warn"


def test_classify_node_health_running_no_alarms_is_ok():
    assert classify_node_health(True, False, False) == "ok"
