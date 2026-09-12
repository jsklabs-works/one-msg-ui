from unittest.mock import patch

from mq_adapter.client import MQAdapter


def _adapter():
    return MQAdapter(broker_id="b1", base_url="https://example", qmgr_name="QM1", username="u", password="p")


def test_get_resources_maps_mqsc_rows_to_resource_shape():
    adapter = _adapter()
    rows = [{"QUEUE": "DEV.QUEUE.1", "CURDEPTH": "3", "MAXDEPTH": "5000", "IPPROCS": "1", "OPPROCS": "0"}]
    with patch.object(adapter, "_run_mqsc", return_value=rows) as mock_run:
        resources = adapter.get_resources(queue_pattern="DEV.*")

    assert mock_run.call_args[0][0] == "DISPLAY QUEUE(DEV.*) TYPE(QLOCAL) CURDEPTH MAXDEPTH IPPROCS OPPROCS"
    assert len(resources) == 1
    r = resources[0]
    assert r.id == "b1:QM1:DEV.QUEUE.1"
    assert r.namespace == "QM1"
    assert r.name == "DEV.QUEUE.1"
    assert r.depth_current == 3
    assert r.depth_max == 5000
    assert r.open_input_count == 1
    assert r.open_output_count == 0
    assert r.consumer_lag is None  # no per-consumer offset model in MQ


def test_get_resources_handles_missing_attributes_gracefully():
    adapter = _adapter()
    rows = [{"QUEUE": "Q1"}]  # depth attrs absent from a row (shouldn't normally happen, but be defensive)
    with patch.object(adapter, "_run_mqsc", return_value=rows):
        resources = adapter.get_resources()

    assert resources[0].depth_current is None
    assert resources[0].depth_max is None


def test_get_health_events_ok_when_running_and_under_capacity():
    adapter = _adapter()
    from mq_adapter.models import Resource

    resources = [Resource(id="b1:QM1:Q1", broker_id="b1", namespace="QM1", name="Q1", depth_current=10, depth_max=1000)]
    with patch.object(adapter, "_run_mqsc", return_value=[{"STATUS": "RUNNING"}]):
        events = adapter.get_health_events(resources=resources)

    assert events[0].category == "connectivity"
    assert events[0].severity == "ok"
    assert events[1].category == "capacity"
    assert events[1].severity == "ok"
    assert "1 queue(s)" in events[1].message


def test_get_health_events_flags_queue_over_capacity_threshold():
    adapter = _adapter()
    from mq_adapter.models import Resource

    resources = [Resource(id="b1:QM1:Q1", broker_id="b1", namespace="QM1", name="Q1", depth_current=950, depth_max=1000)]
    with patch.object(adapter, "_run_mqsc", return_value=[{"STATUS": "RUNNING"}]):
        events = adapter.get_health_events(resources=resources)

    assert events[1].severity == "critical"
    assert "Q1" in events[1].message


def test_get_health_events_critical_when_qmgr_not_running():
    adapter = _adapter()
    with patch.object(adapter, "_run_mqsc", return_value=[{"STATUS": "ENDED UNEXPECTEDLY"}]):
        events = adapter.get_health_events(resources=[])

    assert events[0].severity == "critical"
