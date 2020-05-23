from typing import List

from pydantic import BaseSettings


class ChouetteConfig(BaseSettings):
    api_key: str
    global_tags: List[str]
    collector_plugins: List[str] = []
    aggregate_interval: int = 10
    capture_interval: int = 30
    datadog_url: str = "https://api.datadoghq.com/api"
    log_level: str = "INFO"
    metrics_bulk_size: int = 10000
    metric_ttl: int = 14400
    metrics_wrapper: str = "simple"
    redis_host: str = "redis"
    redis_port: int = 6379
    release_interval: int = 60
    send_self_metrics: bool = True
