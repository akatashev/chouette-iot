from typing import Dict, List

from pydantic import BaseSettings


class ChouetteConfig(BaseSettings):
    api_key: str
    chouette_tags: Dict[str, str]
    collector_plugins: List[str] = []
    datadog_url: str = "https://api.datadoghq.com/api"
    interval_aggregate: int = 10
    interval_capture: int = 30
    interval_release: int = 60
    log_level: str = "INFO"
    metrics_wrapper: str = "simple"
    redis_host: str = "redis"
    redis_port: int = 6379
