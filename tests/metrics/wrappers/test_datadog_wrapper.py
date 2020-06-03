from chouette.metrics import MergedMetric, WrappedMetric
from chouette.metrics.wrappers import DatadogWrapper


def test_datadog_unsupported_metric_type():
    """
    Unsupported metric type returns an empty list.

    GIVEN: There is a metric of an unsupported type.
    WHEN: This merged metric is wrapped.
    THEN: An empty list is returned.
    """
    merged_metric = MergedMetric(
        metric="count.test",
        type="distribution",
        timestamps=[10, 9],
        values=[1, 2],
        tags={"type": "set"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert not result


def test_datadog_count_wrapper():
    merged_metric = MergedMetric(
        metric="count.test",
        type="count",
        timestamps=[10, 9, 12],
        values=[1, 2, 3],
        tags={"type": "count"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert len(result) == 1
    metric = result.pop()
    assert isinstance(metric, WrappedMetric)
    assert metric.metric == "count.test"
    assert metric.type == "count"
    assert metric.timestamp == 9
    assert metric.value == 6
    assert metric.tags == ["type:count"]


def test_datadog_rate_wrapper():
    merged_metric = MergedMetric(
        metric="rate.test",
        type="rate",
        timestamps=[10, 9, 12],
        values=[1, 2, 3],
        tags={"type": "rate"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert len(result) == 1
    metric = result.pop()
    assert metric.metric == "rate.test"
    assert metric.type == "rate"
    assert metric.timestamp == 9
    assert metric.value == 0.6
    assert metric.tags == ["type:rate"]


def test_datadog_gauge_wrapper():
    merged_metric = MergedMetric(
        metric="gauge.test",
        type="gauge",
        timestamps=[10, 9, 12, 11],
        values=[1, 2, 3, 9],
        tags={"type": "gauge"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert len(result) == 1
    metric = result.pop()
    assert metric.metric == "gauge.test"
    assert metric.type == "gauge"
    assert metric.timestamp == 9
    assert metric.value == 3
    assert metric.tags == ["type:gauge"]


def test_datadog_set_wrapper():
    """
    Set metric wrapper:

    GIVEN: There is a set type metric with 2 values.
    AND: It contains 2 list of 2 elements each, but only 3 of the are unique.
    WHEN: This merged metric is wrapped.
    THEN: A list with a single WrappedMetric is returned.
    AND: This metric's type is 'count'.
    AND: Its value is 3 (number of unique elements in the lists).
    AND: Its timestamp is the earliest timestamp in a set.
    AND: Its name and tags are correct.
    """
    merged_metric = MergedMetric(
        metric="set.test",
        type="set",
        timestamps=[10, 9],
        values=[["Alice", "Bob"], ["Bob", "Carol"]],
        tags={"type": "set"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert len(result) == 1
    metric = result.pop()
    assert metric.metric == "set.test"
    assert metric.type == "count"
    assert metric.timestamp == 9
    assert metric.value == 3
    assert metric.tags == ["type:set"]


def test_datadog_set_wrapper_wrong_type():
    """
    Set metric wrapper used incorrectly:

    GIVEN: There is a set type metric with 2 values.
    BUT: These values are not lists.
    WHEN: This merged metric is wrapped.
    THEN: An empty list is returned.
    """
    merged_metric = MergedMetric(
        metric="wrong.set.test",
        type="set",
        timestamps=[10, 9],
        values=[1, 2],
        tags={"type": "set"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert not result


def test_datadog_histogram_wrapper():
    merged_metric = MergedMetric(
        metric="histogram.test",
        type="histogram",
        timestamps=[10, 9, 12],
        values=[1, 2, 3],
        tags={"type": "histogram"},
    )
    result = DatadogWrapper.wrap_metrics([merged_metric], flush_interval=10)
    assert len(result) == 5
    postfixes = sorted(
        [metric.metric.replace("histogram.test", "") for metric in result]
    )
    assert postfixes == sorted([".avg", ".max", ".median", ".count", ".95percentile"])
    max = next(metric for metric in result if ".max" in metric.metric)
    assert max.value == 3
    assert max.type == "gauge"
    avg = next(metric for metric in result if ".avg" in metric.metric)
    assert avg.value == 2
    assert avg.type == "gauge"
    median = next(metric for metric in result if ".median" in metric.metric)
    assert median.value == 2
    assert median.type == "gauge"
    count = next(metric for metric in result if ".count" in metric.metric)
    assert count.value == 0.3
    assert count.type == "rate"
    percentile = next(metric for metric in result if ".95percentile" in metric.metric)
    assert percentile.value == 2.9
    assert percentile.type == "gauge"
