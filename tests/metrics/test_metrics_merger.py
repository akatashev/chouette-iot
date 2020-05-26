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
        MergedMetric(metric="metric-1", type="count", timestamps=[10], values=[10]),
        MergedMetric(metric="metric-2", type="gauge", timestamps=[12], values=[7]),
        MergedMetric(
            metric="metric-3",
            type="gauge",
            timestamps=[23],
            values=[12],
            tags={"very": "important"},
        ),
        MergedMetric(
            metric="metric-4", type="gauge", values=[10, 6], timestamps=[31, 31]
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
            MergedMetric(metric="name", type="count", values=[1], timestamps=[1]),
        ),
    ],
)
def test_cast_to_metric(storage_record, expected_result):
    result = MetricsMerger._cast_to_metric(storage_record)
    assert result == expected_result


def test_merge_metrics(metrics_values):
    values, expected_values = metrics_values
    result = MetricsMerger.merge_metrics(values)
    assert result == expected_values


#
#
# @classmethod
# def merge_metrics(cls, b_metrics: List[bytes]) -> List[MergedMetric]:
#     single_metrics = cls._cast_bytes_to_metrics(b_metrics)
#     grouped_metrics = groupby(single_metrics, lambda metric: metric.id)
#     merged_metrics = map(
#         lambda group: reduce(lambda a, b: a + b, group), grouped_metrics
#     )
#     return list(merged_metrics)
#
# @classmethod
# def _get_merged_metric_pair(cls, metric: dict) -> Tuple[str, MergedMetric]:
#     metric["tags"] = cls._get_tags_list(metric)
#     tags_string = "_".join(metric["tags"])
#     key = f"{metric['name']}_{metric.get('type')}_{tags_string}"
#     merged_metric = MergedMetric(
#         name=metric["name"],
#         metric_type=metric.get("type"),
#         values=[metric["value"]],
#         timestamps=[metric["timestamp"]],
#         tags=metric["tags"],
#     )
#     return key, merged_metric
#
#
# @staticmethod
# def _get_tags_list(metric: dict) -> list:
#     tags = metric.get("tags")
#     try:
#         tags_list = [f"{name}:{str(value)}" for name, value in tags]
#     except TypeError:
#         tags_list = []
#     return sorted(tags_list)
