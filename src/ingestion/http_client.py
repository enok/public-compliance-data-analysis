"""
Shared HTTP client with retry logic for API ingestion.

This module provides a unified approach to making HTTP requests with:
- Exponential backoff retry logic
- Configurable timeouts
- Proper error handling for different HTTP status codes
- URL building without encoding slashes (for APIs that reject %2F)
"""

import os
import time
import logging
import requests
import json
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, Union

logger = logging.getLogger(__name__)


class HTTPClient:
    """
    HTTP client with exponential backoff retry logic.
    
    Handles common HTTP error scenarios:
    - 400 Bad Request: Fail immediately (client error, won't fix itself)
    - 401/403 Unauthorized: Raise RuntimeError (credentials issue)
    - 429 Rate Limit: Aggressive backoff and retry
    - Timeouts/Network errors: Exponential backoff and retry
    """
    
    def __init__(
        self,
        max_retries: int = 10,
        timeout: int = 180,
        user_agent: str = "public-compliance-data-analysis/1.0",
        enable_cache: bool = True,
        cache_ttl_seconds: int = 300,
        cache_dir: Optional[str] = None,
        use_session: bool = False,
    ):
        """
        Initialize HTTP client.
        
        :param max_retries: Maximum number of retry attempts.
        :param timeout: Request timeout in seconds.
        :param user_agent: User-Agent header value.
        :param use_session: Use requests.Session() to maintain cookies across requests (for WAF/session-based APIs).
        """
        self.max_retries = max_retries
        self.timeout = timeout
        self.user_agent = user_agent
        self.enable_cache = enable_cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.use_session = use_session
        self.session = requests.Session() if use_session else None

        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path(__file__).resolve().parents[2] / ".cache" / "http_client"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _mask_api_key(self, headers: Dict[str, str], key_name: str) -> Dict[str, str]:
        """Mask API key in headers for logging."""
        log_headers = dict(headers)
        if os.getenv("LOG_HTTP_UNSAFE") != "1":
            # Mask the specified key_name
            if key_name in log_headers:
                api_key = log_headers.get(key_name)
                if api_key and len(api_key) > 10:
                    log_headers[key_name] = f"{api_key[:6]}...{api_key[-4:]} (len={len(api_key)})"
                elif api_key:
                    log_headers[key_name] = "***"
            
            # Also mask Authorization header (Bearer tokens)
            if "Authorization" in log_headers:
                auth_value = log_headers["Authorization"]
                if auth_value and auth_value.startswith("Bearer "):
                    token = auth_value[7:]  # Remove "Bearer " prefix
                    if len(token) > 10:
                        log_headers["Authorization"] = f"Bearer {token[:6]}...{token[-4:]} (len={len(token)})"
                    elif token:
                        log_headers["Authorization"] = "Bearer ***"
                elif auth_value:
                    log_headers["Authorization"] = "***"
        return log_headers

    def _cache_key(self, prepared_url: str, headers: Dict[str, str]) -> str:
        payload = {
            "url": prepared_url,
            "headers": {k.lower(): v for k, v in headers.items()},
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        return digest

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        cache_path = self.cache_dir / f"{key}.json"
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return None

        ts = payload.get("ts")
        if not isinstance(ts, (int, float)):
            return None

        if (time.time() - ts) > self.cache_ttl_seconds:
            return None

        return payload

    def _cache_put_ok(self, key: str, response: requests.Response, prepared_url: str) -> None:
        if response.status_code in (401, 403, 429):
            return

        payload = {
            "ok": True,
            "ts": time.time(),
            "url": prepared_url,
            "final_url": getattr(response, "url", prepared_url),
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "text": response.text,
        }

        cache_path = self.cache_dir / f"{key}.json"
        try:
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except Exception:
            return
    
    def fetch(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        api_key_header: Optional[str] = None,
        return_json: bool = True
    ) -> Optional[Union[Dict, str]]:
        """
        Fetch data from URL with exponential backoff retry.
        
        :param url: Base URL to fetch.
        :param params: Query parameters (will not URL-encode slashes).
        :param headers: Additional headers.
        :param api_key_header: Header name containing API key (for log masking).
        :param return_json: If True, parse response as JSON; otherwise return text.
        :return: Parsed JSON dict/list, text string, or None on failure.
        """
        request_headers = {
            "Accept": "*/*",
            "User-Agent": self.user_agent,
        }
        if headers:
            request_headers.update(headers)

        prepared = requests.Request("GET", url, params=params, headers=request_headers).prepare()
        full_url = prepared.url

        cache_key = None
        if self.enable_cache:
            try:
                cache_key = self._cache_key(full_url, request_headers)
                cached = self._cache_get(cache_key)
                if cached is not None:
                    if cached.get("ok") is not True:
                        return None

                    logger.info("🗄️ HTTP cache hit | url=%s", full_url)
                    cached_text = cached.get("text", "")
                    cached_headers = cached.get("headers", {}) or {}

                    if return_json:
                        content_type = (cached_headers.get("Content-Type") or cached_headers.get("content-type") or "")
                        if "application/json" in content_type or "text/json" in content_type:
                            if cached_text and str(cached_text).strip():
                                return json.loads(cached_text)
                        return None

                    return cached_text
            except Exception:
                cache_key = None

        log_headers = self._mask_api_key(request_headers, api_key_header) if api_key_header else request_headers
        logger.info(
            "🔎 HTTP request | method=GET | url=%s | headers=%s",
            full_url,
            log_headers,
        )
        
        for attempt in range(self.max_retries):
            try:
                if self.session:
                    # Allow redirects to handle WAF challenges, session will maintain cookies
                    response = self.session.get(url, params=params, headers=request_headers, timeout=self.timeout, allow_redirects=True)
                else:
                    response = requests.get(url, params=params, headers=request_headers, timeout=self.timeout, allow_redirects=True)
                final_url = getattr(response, "url", full_url)
                
                if response.status_code in (401, 403):
                    raise RuntimeError(
                        f"Authorization failed ({response.status_code}). "
                        f"Check API credentials. Response: {response.text[:500]}"
                    )
                
                if response.status_code == 400:
                    logger.error(f"❌ 400 Bad Request - API rejected parameters. URL: {full_url}")
                    logger.error(f"   Response: {response.text[:500]}")
                    return None
                
                if response.status_code == 429:
                    wait_time = 2 ** (attempt + 2)
                    logger.warning(f"⏳ Rate limit hit (429). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                
                if return_json:
                    # Check for empty response before parsing JSON
                    if not response.content or not response.text.strip():
                        logger.warning(
                            "⚠️ Empty response received from API | status=%s | url=%s | final_url=%s",
                            response.status_code,
                            full_url,
                            final_url,
                        )
                        return None
                    
                    # Check Content-Type is JSON
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' not in content_type and 'text/json' not in content_type:
                        logger.warning(
                            "⚠️ Non-JSON response | status=%s | content_type=%s | url=%s | final_url=%s",
                            response.status_code,
                            content_type,
                            full_url,
                            final_url,
                        )
                        logger.warning("   Response preview: %s", response.text[:200])
                        # Don't retry - this is a server-side issue
                        return None
                    
                    parsed = response.json()

                    if cache_key and response.status_code == 200:
                        self._cache_put_ok(cache_key, response, full_url)
                    return parsed

                text = response.text
                if cache_key and response.status_code == 200 and text and text.strip():
                    self._cache_put_ok(cache_key, response, full_url)
                return text
                
            except RuntimeError:
                raise
            except requests.exceptions.JSONDecodeError as e:
                # Empty or invalid JSON - don't retry, it won't fix itself
                logger.error(f"❌ Invalid JSON response from API. URL: {full_url}")
                logger.error(f"   Response preview: {response.text[:200] if response else 'N/A'}")
                return None
            except Exception as e:
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"⚠️ Attempt {attempt+1}/{self.max_retries} failed. Retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
        
        logger.error(f"❌ All {self.max_retries} attempts failed for URL: {full_url}")
        return None
