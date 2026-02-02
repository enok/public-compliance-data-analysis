import json
import hashlib
import time
from pathlib import Path
from typing import Optional

from botocore.exceptions import ClientError


def calculate_md5(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


class SkipMarkerCache:
    def __init__(
        self,
        scope: str,
        ttl_seconds: int = 300,
        cache_root: Optional[Path] = None,
    ):
        self.scope = scope
        self.ttl_seconds = ttl_seconds
        if cache_root is None:
            cache_root = Path(__file__).resolve().parents[2] / ".cache" / "ingestion_skip"
        self.cache_dir = cache_root / scope
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for_key(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def get(self, key: str) -> Optional[str]:
        path = self._path_for_key(key)
        if not path.exists():
            return None

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

        ts = payload.get("ts")
        if not isinstance(ts, (int, float)):
            return None

        if (time.time() - ts) > self.ttl_seconds:
            return None

        return payload.get("status")

    def set(self, key: str, status: str) -> None:
        path = self._path_for_key(key)
        payload = {
            "ts": time.time(),
            "status": status,
            "key": key,
        }
        try:
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            return
