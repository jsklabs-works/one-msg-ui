from kafka_adapter.lag import compute_lag, sum_topic_lag


def test_compute_lag_normal_case():
    assert compute_lag(log_end_offset=30, committed_offset=10) == 20


def test_compute_lag_zero_when_caught_up():
    assert compute_lag(log_end_offset=10, committed_offset=10) == 0


def test_compute_lag_clamped_at_zero_never_negative():
    # Can happen transiently (e.g. a topic was truncated/recreated between reads).
    assert compute_lag(log_end_offset=5, committed_offset=10) == 0


def test_compute_lag_none_when_no_committed_offset():
    # "no lag data" must stay distinguishable from "zero lag" (README gotcha).
    assert compute_lag(log_end_offset=30, committed_offset=None) is None


def test_compute_lag_none_when_no_log_end_offset():
    assert compute_lag(log_end_offset=None, committed_offset=10) is None


def test_sum_topic_lag_adds_known_partitions():
    assert sum_topic_lag([8, 10, 2]) == 20


def test_sum_topic_lag_ignores_none_entries_but_keeps_the_rest():
    assert sum_topic_lag([8, None, 2]) == 10


def test_sum_topic_lag_none_when_every_partition_has_no_data():
    assert sum_topic_lag([None, None]) is None


def test_sum_topic_lag_none_for_empty_list():
    assert sum_topic_lag([]) is None
