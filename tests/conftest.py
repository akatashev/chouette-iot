import pytest


@pytest.fixture(scope="session")
def raw_metrics_keys():
    return [
        (b"metric-uuid-1", 10),
        (b"metric-uuid-2", 12),
        (b"metric-uuid-3", 23),
        (b"metric-uuid-4", 31),
        (b"metric-uuid-5", 34),
    ]


@pytest.fixture(scope="session")
def raw_metrics_values():
    return [
        (
            b"metric-uuid-1",
            b'{"metric": "metric-1", "type": "count", "timestamp": 10, "value": 10}',
        ),
        (
            b"metric-uuid-2",
            b'{"metric": "metric-2", "type": "gauge", "timestamp": 12, "value": 7}',
        ),
        (
            b"metric-uuid-3",
            b'{"metric": "metric-3", "type": "gauge", "timestamp": 23, "value": 12, "tags": {"very": "important"}}',
        ),
        (
            b"metric-uuid-4",
            b'{"metric": "metric-4", "type": "gauge", "timestamp": 31, "value": 10}',
        ),
        (
            b"metric-uuid-5",
            b'{"metric": "metric-4", "type": "gauge", "timestamp": 31, "value": 6}',
        ),
    ]
