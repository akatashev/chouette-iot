"""
chouette.ChouetteConfig
"""
from typing import List

from pydantic import BaseSettings  # type: ignore

__all__ = ["ChouetteConfig"]


class ChouetteConfig(BaseSettings):
    """
    Main application configuration.

    Data is being collected from environment variables.
    It's case-insensitive, so API_KEY variable will be
    loaded as an api_key parameter.
    """

    api_key: str
    global_tags: List[str]
    collector_plugins: List[str] = []
    aggregate_interval: int = 10
    capture_interval: int = 30
    datadog_url: str = "https://api.datadoghq.com/api"
    log_level: str = "INFO"
    metrics_bulk_size: int = 10000
    metric_ttl: int = 14400
    metrics_wrapper: str = "datadog"
    release_interval: int = 60
    send_self_metrics: bool = True
