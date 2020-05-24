from chouette.metrics import MergedMetric, RawMetric, WrappedMetric
import pytest


def test_merged_metric_successfull_merge():
    """
    MergedMetrics of the same type can be merged.

    GIVEN: There are 2 MergedMetric objects with the same name, type and tags.
    WHEN: One metric is added to another.
    THEN: It returns a new MergedMetric of the same type with merged values
          and timestamps.
    """
    metric1 = MergedMetric("name", "type", [1], [2], tags=["tag:1"])
    metric2 = MergedMetric("name", "type", [3], [4], tags=["tag:1"])
    result = metric1 + metric2
    assert result.name == "name"
    assert result.type == "type"
    assert result.tags == ["tag:1"]
    assert result.timestamps == [2, 4]
    assert result.values == [1, 3]


def test_merged_metric_unsuccessful_merge():
    """
    MergedMetrics of different types can't be merged.

    GIVEN: There are 2 MergedMetric objects with different names.
    WHEN: One metric is added to another.
    THEN: ValueError exception is raised.
    """
    metric1 = MergedMetric("name", "type1", [1], [2], tags=["tag:1"])
    metric2 = MergedMetric("name", "type2", [3], [4], tags=["tag:1"])
    with pytest.raises(ValueError):
        metric1 + metric2


def test_merged_metric_str_and_repr():
    """
    MergedMetric:
    __str__, __repr__ and asdict tests.
    """
    expected_dict = {
        "name": "mergedMetric",
        "type": "count",
        "values": [1],
        "timestamps": [2],
        "tags": ["test:test"],
    }

    metric = MergedMetric("mergedMetric", "count", [1], [2], tags=["test:test"])
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
        "name": "rawMetric",
        "type": "count",
        "value": 1,
        "timestamp": 2,
        "tags": [],
    }

    metric = RawMetric("rawMetric", "count", 1, 2)
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<RawMetric: {str(metric_dict)}>"


def test_wrapped_metric_str_and_repr():
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

    metric = WrappedMetric("wrappedMetric", "count", 1, 2)
    metric_dict = metric.asdict()
    assert metric_dict == expected_dict
    assert str(metric) == str(metric_dict)
    assert repr(metric) == f"<WrappedMetric: {str(metric_dict)}>"
