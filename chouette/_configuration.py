from typing import Any, Dict, List
from pydantic import BaseSettings


class ChouetteConfig(BaseSettings):
    api_key: str
    chouette_tags: List[Dict[str, Any]]
    datadog_url: str = "https://api.datadoghq.com/api"
    interval_aggregate: int = 10
    interval_capture: int = 30
    interval_release: int = 60
    log_level: str = "INFO"
    redis_host: str = "redis"
    redis_port: int = 6379
