import pytest

from chouette_iot.metrics._metrics import MergedMetric, WrappedMetric
from chouette_iot.metrics.wrappers import SimpleWrapper


def test_wrap_count():
    """
    SimpleWrapper wraps `count` metrics.

    GIVEN: There is a `count` type MergedMetric in a list.
    WHEN: It is passed to the `wrap_metrics` method.
    THEN: It returns a list with one wrapped metric.
    AND: This wrapped metric has the same `metric` parameter.
    AND: Its type is `count`.
    AND: Its value is the sum of all the values.
    AND: Its timestamp is the latest timestamp from the timestamps list.
    AND: Its tags are a list of stringified MergedMetric tags.
    """
    expected_metric = WrappedMetric(
        metric="test",
        type="count",
        value=6,
        timestamp=18,
        tags=["hello:world"],
        interval=10,
    )
    merged_metric = MergedMetric(
        metric="test",
        type="count",
        values=[1, 2, 3],
        timestamps=[18, 10, 12],
        tags={"hello": "world"},
    )
    result = SimpleWrapper.wrap_metrics([merged_metric])
    assert len(result) == 1
    wrapped_metric = result.pop()
    assert wrapped_metric == expected_metric


@pytest.mark.parametrize("metric_type", ["gauge", "histogram", "distribution"])
def test_wrap_average(metric_type):
    """
    SimpleWrapper wraps `average` metrics.

    GIVEN: There is a not `count` type MergedMetric in a list.
    WHEN: It is passed to the `wrap_metrics` method.
    THEN: It returns a list with two wrapped metrics.
    AND: One of them is a `gauge` another is a `count`.
    AND: The 'gauge' one has the same `metric` parameter as the MergedMetric.
    AND: The `count` one has the same parameter with a '.count' suffix.
    AND: The `gauge`s value is an average of the values.
    AND: The `count`s value is a number of values.
    AND: Their timestamp is the latest timestamp from the timestamps list.
    AND: Their tags are a list of stringified MergedMetric tags.
    """
    expected_metrics = [
        WrappedMetric(
            metric="test", type="gauge", value=2.0, timestamp=18, tags=["hello:world"]
        ),
        WrappedMetric(
            metric="test.count",
            type="count",
            value=3,
            timestamp=18,
            tags=["hello:world"],
            interval=15,
        ),
    ]
    merged_metric = MergedMetric(
        metric="test",
        type=metric_type,
        values=[1, 2, 3],
        timestamps=[18, 10, 12],
        tags={"hello": "world"},
        interval=15,
    )
    result = SimpleWrapper.wrap_metrics([merged_metric])
    assert result == expected_metrics
