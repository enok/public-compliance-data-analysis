import json
import hashlib
import threading
import time
import shutil
from pathlib import Path
from typing import Optional

from botocore.exceptions import ClientError


CONTENT_SHA256_METADATA_KEY = "content-sha256"


class TokenBucketRateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    All parallel workers share a single instance. Each call to :meth:`acquire`
    blocks until a token is available, keeping the aggregate request rate at or
    below ``requests_per_minute`` regardless of the number of threads.

    The bucket starts full (burst of up to ``burst_size`` tokens). New tokens
    are added continuously at ``requests_per_minute / 60`` tokens per second.
    Calling code does not need to sleep separately; this replaces the
    ``rate_limit_delay`` sleep in individual workers.

    Usage::

        limiter = TokenBucketRateLimiter(requests_per_minute=280)
        # Before every CGU API call:
        limiter.acquire()
        response = self.http_client.fetch(...)
    """

    def __init__(
        self,
        requests_per_minute: float,
        burst_size: Optional[float] = None,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self._rate_per_second: float = requests_per_minute / 60.0
        # Default burst = 1 second's worth of tokens (avoids tiny initial stalls)
        self._capacity: float = burst_size if burst_size is not None else self._rate_per_second
        self._tokens: float = self._capacity
        self._last_refill: float = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_per_second)
        self._last_refill = now

    def acquire(self) -> None:
        """Block until a token is available, then consume it."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long until the next token arrives.
                wait = (1.0 - self._tokens) / self._rate_per_second

            time.sleep(wait)


def calculate_content_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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


class DatasetPageCache:
    def __init__(self, scope: str, cache_key: str, cache_root: Optional[Path] = None):
        if cache_root is None:
            cache_root = Path(__file__).resolve().parents[2] / ".cache" / "dataset_pages"
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        self.cache_dir = cache_root / scope / digest
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def page_path(self, page_number: int) -> Path:
        return self.cache_dir / f"page_{page_number:06d}.json"

    def write_page(self, page_number: int, payload) -> Path:
        path = self.page_path(page_number)
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return path

    def list_page_paths(self) -> list[Path]:
        return sorted(self.cache_dir.glob("page_*.json"))

    def exists(self) -> bool:
        return any(self.cache_dir.glob("page_*.json"))

    def clear(self) -> None:
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir, ignore_errors=True)
