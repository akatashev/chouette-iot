import pytest

from chouette.metrics import MergedMetric
from chouette.metrics._aggregator import MetricsMerger


@pytest.fixture
def metrics_values(raw_metrics_values):
    """
    Returns a list of metrics bytes objects cleaned of timestamps,
    one message that is not parceable to JSON and one message that
    can't be cast to a RawMetric instance.
    """
    values = [metric for key, metric in raw_metrics_values]
    values.append(b"Not a JSON parseable metric at all.")
    values.append(b'{"msg": "That is not a proper metric."}')
    expected_metrics = [
        MergedMetric(
            metric="metric-1", type="count", timestamps=[10], values=[10], interval=10
        ),
        MergedMetric(
            metric="metric-2", type="gauge", timestamps=[12], values=[7], interval=10
        ),
        MergedMetric(
            metric="metric-3",
            type="gauge",
            timestamps=[23],
            values=[12],
            tags={"very": "important"},
            interval=10,
        ),
        MergedMetric(
            metric="metric-4",
            type="gauge",
            values=[10, 6],
            timestamps=[31, 31],
            interval=10,
        ),
    ]
    return values, expected_metrics


def test_group_metric_keys(metrics_keys):
    expected_result = [
        [b"metric-uuid-1", b"metric-uuid-2"],
        [b"metric-uuid-3"],
        [b"metric-uuid-4", b"metric-uuid-5"],
    ]
    result = MetricsMerger.group_metric_keys(metrics_keys, 5)
    assert result == expected_result


@pytest.mark.parametrize(
    "storage_record, expected_result",
    [
        (b"Not a JSON parseable metric at all.", None),
        (b'{"msg": "That is not a proper metric."}', None),
        (
            b'{"metric": "name", "type": "count", "value": 1, "timestamp": 1}',
            MergedMetric(
                metric="name", type="count", values=[1], timestamps=[1], interval=10
            ),
        ),
    ],
)
def test_cast_to_metric(storage_record, expected_result):
    result = MetricsMerger._cast_to_metric(storage_record, 10)
    assert result == expected_result


def test_merge_metrics(metrics_values):
    values, expected_values = metrics_values
    result = MetricsMerger.merge_metrics(values, 10)
    assert result == expected_values
