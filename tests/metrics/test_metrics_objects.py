import time

import pytest

from chouette.metrics import MergedMetric, RawMetric, WrappedMetric


def test_merged_metric_successfull_merge():
    """
    MergedMetrics of the same type can be merged.

    GIVEN: There are 2 MergedMetric objects with the same name, type and tags.
    WHEN: One metric is added to another.
    THEN: It returns a new MergedMetric of the same type with merged values
          and timestamps.
    """
    metric1 = MergedMetric(
        metric="name", type="type", values=[1], timestamps=[2], tags={"tag": "1"}
    )
    metric2 = MergedMetric(
        metric="name", type="type", values=[3], timestamps=[4], tags={"tag": "1"}
    )
    result = metric1 + metric2
    assert result.metric == "name"
    assert result.type == "type"
    assert result.tags == {"tag": "1"}
    assert result.timestamps == [2, 4]
    assert result.values == [1, 3]


def test_merged_metric_unsuccessful_merge():
    """
    MergedMetrics of different types can't be merged.

    GIVEN: There are 2 MergedMetric objects with different names.
    WHEN: One metric is added to another.
    THEN: ValueError exception is raised.
    """
    metric1 = MergedMetric(
        metric="name", type="type1", values=[1], timestamps=[2], tags={"tag": "1"}
    )
    metric2 = MergedMetric(
        metric="name", type="type2", values=[3], timestamps=[4], tags={"tag": "1"}
    )
    with pytest.raises(ValueError):
        metric1 + metric2


def test_merged_metric_str_and_repr():
    """
    MergedMetric:
    __str__, __repr__ and asdict tests.
    """
    expected_dict = {
        "metric": "mergedMetric",
        "type": "count",
        "values": [1],
        "timestamps": [2],
        "tags": ["test:test"],
        "interval": 10,
    }

    metric = MergedMetric(
        metric="mergedMetric",
        type="count",
        values=[1],
        timestamps=[2],
        tags={"test": "test"},
    )
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<MergedMetric: {str(metric_dict)}>"


def test_raw_metric_str_and_repr():
    """
    RawMetric:
    __str__, __repr__ and asdict tests.
    """
    expected_dict = {
        "metric": "rawMetric",
        "type": "count",
        "value": 1,
        "timestamp": 2,
        "tags": {},
    }

    metric = RawMetric(metric="rawMetric", type="count", value=1, timestamp=2)
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<RawMetric: {str(metric_dict)}>"


def test_wrapped_metric_str_and_repr_no_interval():
    """
    WrappedMetric:
    __str__, __repr__ and asdict tests.
    """
    expected_dict = {
        "metric": "wrappedMetric",
        "type": "count",
        "points": [[2, 1]],
        "tags": [],
    }

    metric = WrappedMetric(metric="wrappedMetric", type="count", value=1, timestamp=2)
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<WrappedMetric: {str(metric_dict)}>"


def test_wrapped_metric_str_and_repr_with_interval():
    """
    WrappedMetric:
    __str__, __repr__ and asdict tests.
    """
    expected_dict = {
        "metric": "wrappedMetric",
        "type": "count",
        "points": [[2, 1]],
        "tags": [],
        "interval": 10,
    }

    metric = WrappedMetric(
        metric="wrappedMetric", type="count", value=1, timestamp=2, interval=10
    )
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<WrappedMetric: {str(metric_dict)}>"


def test_simple_metric_gets_timestamp():
    """
    SimpleMetric instances are expected to get an actual timestamp if
    it didn't receive a "timestamp" keyword value.
    """
    now = int(time.time())
    metric = WrappedMetric(metric="wrappedMetric", type="count", value=1)
    assert metric.timestamp >= now


@pytest.mark.parametrize("second_type, are_equal", [("count", True), ("gauge", False)])
def test_metrics_equality(second_type, are_equal):
    """
    Metrics are considered equal if their dicts are equal.
    """
    metric1 = WrappedMetric(metric="wrappedMetric", type="count", value=1, timestamp=2)
    metric2 = WrappedMetric(
        metric="wrappedMetric", type=second_type, value=1, timestamp=2
    )
    assert (metric1 == metric2) is are_equal


@pytest.mark.parametrize("not_a_metric", [None, "Not a metric", 11011010])
def test_comparing_a_metric_with_not_a_netric(not_a_metric):
    """
    Metric is never equal to a not metric object.
    """
    metric = RawMetric(metric="wrappedMetric", type="count", value=1)
    assert metric != not_a_metric
