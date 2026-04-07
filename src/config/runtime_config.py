import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


RUNTIME_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "runtime_config.json"


@lru_cache(maxsize=1)
def load_runtime_config() -> Dict[str, Any]:
    with open(RUNTIME_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_runtime_value(*keys: str, env_var: str | None = None, default: Any = None) -> Any:
    if env_var:
        env_value = os.getenv(env_var)
        if env_value not in (None, ""):
            return env_value

    current: Any = load_runtime_config()
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def get_aws_profile() -> str | None:
    return get_runtime_value("aws", "profile", env_var="AWS_PROFILE")


def get_s3_bucket_name(default: str | None = None) -> str | None:
    return get_runtime_value("aws", "s3_bucket_name", env_var="S3_BUCKET_NAME", default=default)


def get_api_base_url(name: str, default: str | None = None) -> str | None:
    env_map = {
        "ibge_sidra_base_url": "IBGE_SIDRA_BASE_URL",
        "transparency_base_url": "TRANSPARENCY_BASE_URL",
        "bcb_ipca_monthly_url": "BCB_IPCA_MONTHLY_URL",
        "siconfi_base_url": "SICONFI_BASE_URL",
    }
    return get_runtime_value("apis", name, env_var=env_map.get(name), default=default)
