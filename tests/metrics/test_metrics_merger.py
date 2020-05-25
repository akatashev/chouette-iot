from chouette.metrics._aggregator import MetricsMerger
from typing import Iterator
from chouette.metrics import RawMetric
import json
import pytest


@pytest.fixture
def metrics_values(raw_metrics_values):
    """
    Returns a list of metrics bytes objects cleaned of timestamps,
    one message that is not parceable to JSON and one message that
    can't be cast to a RawMetric instance.
    """
    values = [metric for key, metric in raw_metrics_values]
    raw_metrics = [RawMetric(**json.loads(metric)) for metric in values]
    raw_metrics.sort(key=lambda metric: metric.timestamp)
    merged_metrics = [metric.mergify() for metric in raw_metrics]
    values.append(b"Not a JSON parseable metric at all.")
    values.append(b'{"msg": "That is not a proper metric."}')
    return values, merged_metrics


def test_group_metric_keys(metrics_keys):
    expected_res_list = [
        [b"metric-uuid-1", b"metric-uuid-2"],
        [b"metric-uuid-3"],
        [b"metric-uuid-4", b"metric-uuid-5"],
    ]
    result = MetricsMerger.group_metric_keys(metrics_keys, 5)
    assert isinstance(result, Iterator)
    res_list = list(result)
    assert expected_res_list == res_list


@pytest.mark.parametrize(
    "storage_record, expected_result",
    [
        (b"Not a JSON parseable metric at all.", None),
        (b'{"msg": "That is not a proper metric."}', None),
        (
            b'{"metric": "name", "type": "count", "value": 1, "timestamp": 1}',
            RawMetric(
                **json.loads(
                    b'{"metric": "name", "type": "count", "value": 1, "timestamp": 1}'
                )
            ).mergify(),
        ),
    ],
)
def test_get_raw_metric(storage_record, expected_result):
    result = MetricsMerger._get_metric(storage_record)
    assert result == expected_result


def test_cast_bytes_to_metrics(metrics_values):
    values, exp_metrics = metrics_values
    result = MetricsMerger._cast_bytes_to_metrics(values)
    assert list(result) == exp_metrics


#
#
# @classmethod
# def merge_metrics(cls, b_metrics: List[bytes]) -> List[MergedMetric]:
#     d_metrics = cls._cast_metrics_to_dicts(b_metrics)
#     key_metric_pair = map(cls._get_merged_metric_pair, d_metrics)
#
#     buffer = defaultdict(list)
#     for key, metric in key_metric_pair:
#         buffer[key].append(metric)
#
#     merged_metrics = {
#         key: reduce(lambda a, b: a + b, metric) for key, metric in buffer.items()
#     }.values()
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
