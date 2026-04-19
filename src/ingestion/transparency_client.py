import os
import boto3
import json
import logging
import time
import re
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.ingestion.http_client import HTTPClient
from src.config.runtime_config import get_api_base_url, get_aws_profile, get_s3_bucket_name
from src.ingestion.ingestion_utils import (
    CONTENT_SHA256_METADATA_KEY,
    DatasetPageCache,
    SkipMarkerCache,
    TokenBucketRateLimiter,
    calculate_content_digest,
)

# Load environment variables from .env file
load_dotenv()

# Configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default window (in months) after which a monthly federal-transfers dataset
# is treated as frozen. Any `completed` month whose end is older than this is
# trusted without a live CGU probe. Override with TRANSPARENCY_FROZEN_MONTHS_BACK.
DEFAULT_FROZEN_MONTHS_BACK = 6

# Default thread-pool size for the bulk S3 metadata prefetch at ingestion start.
# Override with TRANSPARENCY_METADATA_PREFETCH_WORKERS.
DEFAULT_METADATA_PREFETCH_WORKERS = 16

# Default number of parallel month-workers. At ~0.35 req/sec per worker this
# gives ~2.8 req/sec (168 req/min) with 8 workers — comfortable under CGU's
# 300 req/min cap. Override with TRANSPARENCY_MAX_WORKERS.
DEFAULT_MAX_WORKERS = 8

# Default aggregate CGU request cap enforced by the token-bucket limiter.
# Set below the hard CGU limit (300/min) to leave room for retries and bursts.
# Override with TRANSPARENCY_REQUESTS_PER_MINUTE.
DEFAULT_REQUESTS_PER_MINUTE = 280

class TransparencyIngestor:
    def __init__(self, bucket_name, config_path):
        """
        Initializes the ingestor for the Transparency Portal data (Bronze II).
        :param bucket_name: S3 Bucket for the Bronze Layer.
        :param config_path: Path to the transparency_metadata.json file.
        """
        self.session = self._build_session()
        self.s3 = self.session.client('s3')
        self.bucket = bucket_name
        self.config = self._load_config(config_path)
        
        # Try to get API key from AWS Secrets Manager first, fallback to environment variable
        self.api_key = self._get_api_key()
        
        if not self.api_key:
            logger.warning("⚠️ TRANSPARENCY_API_KEY not found in Secrets Manager or environment. API calls will fail with 401/403.")

        self.rate_limit_delay = self.config.get('rate_limit', {}).get('delay_between_requests', 3.5)
        self.max_pages = self.config.get('pagination', {}).get('max_pages', 1000)
        self.page_size = self.config.get('pagination', {}).get('page_size', 500)

        try:
            _rpm = float(os.getenv("TRANSPARENCY_REQUESTS_PER_MINUTE", "").strip() or DEFAULT_REQUESTS_PER_MINUTE)
        except ValueError:
            _rpm = DEFAULT_REQUESTS_PER_MINUTE
        self._rate_limiter = TokenBucketRateLimiter(requests_per_minute=_rpm)
        logger.info(
            "🚦 Rate limiter initialised: %.0f req/min (TRANSPARENCY_REQUESTS_PER_MINUTE)",
            _rpm,
        )
        
        # Shared HTTP client with retry logic
        # Note: Session not required - API redirects to error page when data unavailable
        self.http_client = HTTPClient(
            max_retries=10,
            timeout=180,
            user_agent="public-compliance-data-analysis/transparency",
            use_session=False
        )

        # Thesis Documentation: Log of raw data sources
        self.source_log = Path(__file__).parent.parent.parent / "docs" / "data_sources.log"
        os.makedirs(self.source_log.parent, exist_ok=True)

        self.skip_cache = SkipMarkerCache(scope="transparency", ttl_seconds=300)

    def _get_selected_datasets(self) -> list[dict]:
        selected_names = os.getenv("TRANSPARENCY_DATASET_NAMES", "").strip()
        datasets = self.config["datasets"]
        if not selected_names:
            return datasets

        selected_set = {name.strip() for name in selected_names.split(",") if name.strip()}
        filtered = [ds for ds in datasets if ds.get("name") in selected_set]
        missing = sorted(selected_set - {ds.get("name") for ds in filtered})
        if missing:
            logger.warning(
                "⚠️ Unknown Transparency dataset filter entries ignored: %s",
                ", ".join(missing),
            )

        logger.info(
            "📋 Transparency dataset filter active: %s",
            ", ".join(ds["name"] for ds in filtered) or "<empty>",
        )
        return filtered

    def _force_refresh_enabled(self) -> bool:
        return os.getenv("TRANSPARENCY_FORCE_REFRESH", "0").strip() == "1"

    def _should_skip_checkpointed_dataset(self, name: str, existing_metadata: Optional[dict]) -> bool:
        if not existing_metadata or self._force_refresh_enabled():
            return False

        status = existing_metadata.get("status")
        if status == "no_data":
            logger.info(
                "⏭️ Skipping %s - checkpointed previously as no-data. "
                "Set TRANSPARENCY_FORCE_REFRESH=1 to recheck.",
                name,
            )
            return True

        return False

    def _build_partial_cache_key(self, s3_key: str, url: str, base_params: dict) -> str:
        payload = {
            "s3_key": s3_key,
            "url": url,
            "params": base_params,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _build_session(self):
        profile = (get_aws_profile() or "").strip()
        if profile:
            logger.info("✓ Using AWS profile: %s", profile)
            session = boto3.Session(profile_name=profile)
        else:
            session = boto3.Session()

        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError(
                "AWS credentials not found for Transparency ingestion. "
                "Configure environment variables or AWS_PROFILE before running the pipeline."
            )

        frozen = credentials.get_frozen_credentials()
        if not frozen.access_key or not frozen.secret_key:
            raise RuntimeError(
                "Incomplete AWS credentials detected for Transparency ingestion. "
                "Provide access key and secret key (and session token when required)."
            )

        return session

    def _get_api_key(self) -> Optional[str]:
        """
        Retrieve Transparency API key from AWS Secrets Manager or environment variable.
        
        Priority:
        1. AWS Secrets Manager (production)
        2. Environment variable (local development)
        
        Returns:
            API key string or None if not found
        """
        import json
        
        # Try AWS Secrets Manager first (for MWAA/production)
        try:
            secrets_client = self.session.client('secretsmanager')
            secret_name = f"mba-thesis/transparency-api-key-{os.getenv('ENVIRONMENT', 'dev')}"
            
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response['SecretString'])
            api_key = secret_data.get('TRANSPARENCY_API_KEY')
            
            if api_key:
                logger.info(f"✓ API key retrieved from Secrets Manager: {secret_name}")
                return api_key.strip()
        except Exception as e:
            logger.debug(f"Could not retrieve from Secrets Manager: {e}")
        
        # Fallback to environment variable (for local development)
        raw_api_key = os.getenv("TRANSPARENCY_API_KEY")
        if raw_api_key:
            logger.info("✓ API key retrieved from environment variable")
            return raw_api_key.strip()
        
        return None
    
    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        config['api_base_url'] = get_api_base_url(
            'transparency_base_url',
            default=config.get('api_base_url'),
        )
        return config

    def _calculate_content_digest(self, content):
        return calculate_content_digest(content)

    def _should_expand_months(self, endpoint: str, base_params: dict) -> bool:
        return (
            endpoint == "despesas/recursos-recebidos"
            and isinstance(base_params, dict)
            and base_params.get("mesAnoInicio")
            and base_params.get("mesAnoFim")
        )

    def _get_month_range_override(self) -> tuple[Optional[datetime], Optional[datetime]]:
        start_raw = os.getenv("TRANSPARENCY_START_MA", "").strip()
        end_raw = os.getenv("TRANSPARENCY_END_MA", "").strip()

        start = self._parse_mes_ano(start_raw) if start_raw else None
        end = self._parse_mes_ano(end_raw) if end_raw else None

        if start and end and start > end:
            raise ValueError(
                "Invalid Transparency month override: TRANSPARENCY_START_MA "
                "must be earlier than or equal to TRANSPARENCY_END_MA."
            )

        return start, end

    def _resolve_month_range(self, start_mes_ano: str, end_mes_ano: str) -> tuple[datetime, datetime]:
        configured_start = self._parse_mes_ano(start_mes_ano)
        configured_end = self._parse_mes_ano(end_mes_ano)
        override_start, override_end = self._get_month_range_override()

        start = max(configured_start, override_start) if override_start else configured_start
        end = min(configured_end, override_end) if override_end else configured_end

        if start > end:
            raise ValueError(
                "Transparency month override does not overlap the configured range."
            )

        if start != configured_start or end != configured_end:
            logger.info(
                "📆 Transparency month override active: %s to %s (configured: %s to %s)",
                self._format_mes_ano(start),
                self._format_mes_ano(end),
                self._format_mes_ano(configured_start),
                self._format_mes_ano(configured_end),
            )

        return start, end

    def _expand_monthly_datasets(self, ds: dict, base_params: dict):
        filename = ds["filename"]
        stem, ext = filename.rsplit(".", 1)
        base_stem = re.sub(r"_\d{4}$", "", stem)

        start_dt, end_dt = self._resolve_month_range(
            base_params["mesAnoInicio"],
            base_params["mesAnoFim"],
        )
        for month_dt in self._iter_months_inclusive(
            self._format_mes_ano(start_dt),
            self._format_mes_ano(end_dt),
        ):
            month_str = self._format_mes_ano(month_dt)
            ym_suffix = month_dt.strftime("%Y_%m")

            month_params = dict(base_params)
            month_params["mesAnoInicio"] = month_str
            month_params["mesAnoFim"] = month_str

            month_name = f"{base_stem}_{ym_suffix}"
            month_filename = f"{base_stem}_{ym_suffix}.{ext}"

            yield {
                "name": month_name,
                "endpoint": ds["endpoint"],
                "params": month_params,
                "filename": month_filename,
                "requires_pagination": ds.get("requires_pagination", False),
            }

    def _parse_mes_ano(self, mes_ano: str) -> datetime:
        return datetime.strptime(mes_ano, "%m/%Y")

    def _format_mes_ano(self, dt: datetime) -> str:
        return dt.strftime("%m/%Y")

    def _iter_months_inclusive(self, start_mes_ano: str, end_mes_ano: str):
        start = self._parse_mes_ano(start_mes_ano)
        end = self._parse_mes_ano(end_mes_ano)

        current = datetime(start.year, start.month, 1)
        last = datetime(end.year, end.month, 1)

        while current <= last:
            yield current
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)

    def _frozen_months_back(self) -> int:
        """
        Months-back cutoff for treating a monthly dataset as historically frozen.

        Any month whose `mesAnoFim` is older than `today - N months` (with N read
        from TRANSPARENCY_FROZEN_MONTHS_BACK, default 6) is considered immutable:
        CGU does not backfill federal-transfer rows that far into the past, so
        trusting an existing `completed` checkpoint is safe and saves one live
        API round-trip per frozen month (~2s each).

        Set TRANSPARENCY_FROZEN_MONTHS_BACK=0 to disable the optimization and
        restore the original probe-every-month behavior.
        """
        raw = os.getenv("TRANSPARENCY_FROZEN_MONTHS_BACK", "").strip()
        if not raw:
            return DEFAULT_FROZEN_MONTHS_BACK
        try:
            value = int(raw)
        except ValueError:
            logger.warning(
                "⚠️ Invalid TRANSPARENCY_FROZEN_MONTHS_BACK=%r, falling back to default %d",
                raw,
                DEFAULT_FROZEN_MONTHS_BACK,
            )
            return DEFAULT_FROZEN_MONTHS_BACK
        return max(0, value)

    def _is_month_historical(
        self,
        base_params: dict,
        *,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True when the dataset's `mesAnoFim` is older than the frozen cutoff."""
        if self._force_refresh_enabled():
            return False

        months_back = self._frozen_months_back()
        if months_back <= 0:
            return False

        mes_ano_fim = (base_params or {}).get("mesAnoFim")
        if not mes_ano_fim:
            return False

        try:
            end_dt = self._parse_mes_ano(mes_ano_fim)
        except (ValueError, TypeError):
            return False

        today = now or datetime.utcnow()
        # Month-of-end vs. month-of-today, compared in absolute month units.
        end_months = end_dt.year * 12 + end_dt.month
        today_months = today.year * 12 + today.month
        return (today_months - end_months) >= months_back

    def _prefetch_metadata_index(
        self,
        prefix: str = "bronze/transparency/.metadata/",
    ) -> dict:
        """
        Bulk-load every Transparency metadata checkpoint into an in-memory dict.

        Replaces N scattered `GetObject` calls (one per dataset) with one
        `ListObjectsV2` scan + a parallel fan-out of `GetObject`s. For a full
        2010-2022 federal-transfers run that's ~156 objects fetched in ~1-2s
        instead of 5-15s serial.

        Returns:
            Dict keyed by S3 object key -> parsed metadata JSON. Unreadable or
            malformed objects are silently omitted (the caller falls back to a
            live fetch, matching the pre-prefetch behavior).
        """
        results: dict[str, dict] = {}
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            keys: list[str] = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key")
                    if key and key.endswith(".meta.json"):
                        keys.append(key)
        except ClientError as exc:
            logger.warning(
                "⚠️ Metadata prefetch list failed (%s). Falling back to per-dataset fetches.",
                exc,
            )
            return results

        if not keys:
            return results

        try:
            requested_workers = int(
                os.getenv(
                    "TRANSPARENCY_METADATA_PREFETCH_WORKERS",
                    str(DEFAULT_METADATA_PREFETCH_WORKERS),
                )
            )
        except ValueError:
            requested_workers = DEFAULT_METADATA_PREFETCH_WORKERS
        workers = max(1, min(requested_workers, len(keys)))

        def fetch_one(key: str):
            try:
                resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                payload = json.loads(resp["Body"].read().decode("utf-8"))
                return key, payload
            except (ClientError, ValueError, json.JSONDecodeError) as exc:
                logger.debug("metadata prefetch miss for %s: %s", key, exc)
                return key, None

        logger.info(
            "📚 Prefetching %d Transparency metadata checkpoint(s) with %d worker(s)...",
            len(keys), workers,
        )
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="transparency-meta",
        ) as pool:
            for key, meta in pool.map(fetch_one, keys):
                if meta is not None:
                    results[key] = meta
        logger.info(
            "📚 Metadata prefetch complete: %d/%d checkpoint(s) cached in memory.",
            len(results), len(keys),
        )
        return results

    def _normalize_endpoint(self, endpoint: str) -> str:
        return str(endpoint or "").strip().lstrip("/")

    def _build_url(self, base_url: str, endpoint: str) -> str:
        base = str(base_url or "").rstrip("/")
        ep = self._normalize_endpoint(endpoint)
        return f"{base}/{ep}" if ep else base

    def _file_is_valid(self, s3_key, local_digest):
        """Check if file exists in S3 and matches the stored SHA-256 digest."""
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            metadata = response.get("Metadata", {})
            return metadata.get(CONTENT_SHA256_METADATA_KEY) == local_digest
        except ClientError:
            return False
    
    def _get_metadata(self, s3_key):
        """Retrieve metadata file from S3 (page hashes and last page number)."""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return None
    
    def _save_metadata(self, s3_key, metadata):
        """Save metadata file to S3."""
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json.dumps(metadata, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json; charset=utf-8'
        )
    
    def _find_last_page(self, url, base_params, max_attempts=20):
        """Binary search to find the last page with data."""
        # Start with exponential search to find upper bound
        page = 1
        last_valid_page = 0
        
        # Exponential search: 1, 2, 4, 8, 16, 32...
        while page <= self.max_pages:
            params = base_params.copy()
            params['pagina'] = page
            
            try:
                data = self.fetch_with_retry(url, params)
                if data and (not isinstance(data, list) or data):
                    last_valid_page = page
                    page *= 2
                else:
                    # Found empty page, upper bound is between last_valid_page and page
                    break
            except Exception:
                break
            
            time.sleep(self.rate_limit_delay)
        
        if last_valid_page == 0:
            return 0
        
        # Binary search between last_valid_page and page
        lower = last_valid_page
        upper = min(page, self.max_pages)
        
        while lower < upper - 1:
            mid = (lower + upper) // 2
            params = base_params.copy()
            params['pagina'] = mid
            
            try:
                data = self.fetch_with_retry(url, params)
                if data and (not isinstance(data, list) or data):
                    lower = mid
                else:
                    upper = mid
            except Exception:
                upper = mid
            
            time.sleep(self.rate_limit_delay)
        
        return lower

    def fetch_with_retry(self, url, params):
        """Fetches data from Transparency API using shared HTTP client."""
        self._rate_limiter.acquire()
        headers = {}
        if self.api_key:
            headers["chave-api-dados"] = self.api_key
            # Note: Swagger UI only uses chave-api-dados header, not Authorization Bearer
            # Removing Authorization header to match Swagger's working curl command

        return self.http_client.fetch(
            url,
            params=params,
            headers=headers,
            api_key_header="chave-api-dados",
            return_json=True
        )

    def log_source(self, name, url, params):
        """Append the exact API URL to a log file for Thesis reproducibility/documentation."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        param_str = json.dumps(params)
        with open(self.source_log, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] Dataset: {name}\nURL: {url}\nParams: {param_str}\n{'-'*50}\n")

    def _run_single_ingestion(
        self,
        name,
        url,
        base_params,
        filename,
        requires_pagination,
        prefetched_metadata: Optional[dict] = None,
    ):
        """
        Run ingestion for a single dataset.

        :param prefetched_metadata: Optional dict {s3_key -> metadata} from
            :meth:`_prefetch_metadata_index`. When provided, avoids a per-dataset
            S3 GetObject round-trip. Pass None to use the original fallback.
        :return: True if successful, False if failed
        """
        s3_key = f"bronze/transparency/{filename}"
        metadata_key = f"bronze/transparency/.metadata/{filename}.meta.json"

        if self.skip_cache.get(s3_key) == "skipped_up_to_date":
            logger.info(f"⏭️ Skipping {name} - recently skipped (local cache).")
            return True

        if prefetched_metadata is not None and metadata_key in prefetched_metadata:
            existing_metadata = prefetched_metadata[metadata_key]
        else:
            existing_metadata = self._get_metadata(metadata_key)
        if self._should_skip_checkpointed_dataset(name, existing_metadata):
            return True

        partial_cache_key = self._build_partial_cache_key(s3_key, url, base_params)
        page_cache = DatasetPageCache(scope="transparency", cache_key=partial_cache_key)
        last_page = 0
        start_page = 1
        total_records = 0
        cached_page_paths = []

        if existing_metadata and existing_metadata.get('status') == 'completed':
            last_page = existing_metadata.get('last_page', 0)

            # Fast-fast path: historical (frozen) months are skipped without a live
            # CGU probe. Saves one ~1-3s round-trip per month on every re-run.
            if self._is_month_historical(base_params):
                logger.info(
                    "❄️ %s is historical (mesAnoFim=%s, cutoff=%d months). "
                    "Trusting completed checkpoint (%d pages); skipping CGU probe.",
                    name,
                    base_params.get("mesAnoFim"),
                    self._frozen_months_back(),
                    last_page,
                )
                self.skip_cache.set(s3_key, "skipped_up_to_date")
                with open(self.source_log, 'a', encoding='utf-8') as log:
                    log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                    log.write(f"URL: {url}\n")
                    log.write(f"Params: {json.dumps(base_params)}\n")
                    log.write("Status: SKIPPED (frozen historical month, no probe)\n")
                    log.write(f"Pages: {last_page}\n")
                    log.write(f"Records: {existing_metadata.get('total_records', 'unknown')}\n")
                    log.write("-" * 50 + "\n")
                return True

            logger.info(f"📋 Found existing metadata: {last_page} pages previously fetched")
            logger.info(f"🔍 Checking for new pages beyond page {last_page}...")

            check_params = base_params.copy()
            check_params['pagina'] = last_page + 1
            test_chunk = self.fetch_with_retry(url, check_params)

            if not test_chunk or (isinstance(test_chunk, list) and not test_chunk):
                logger.info(f"✓ Dataset {name} is up-to-date (no new pages). Skipping.")
                self.skip_cache.set(s3_key, "skipped_up_to_date")
                with open(self.source_log, 'a', encoding='utf-8') as log:
                    log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                    log.write(f"URL: {url}\n")
                    log.write(f"Params: {json.dumps(base_params)}\n")
                    log.write(f"Status: SKIPPED (up-to-date, no new pages)\n")
                    log.write(f"Pages: {last_page}\n")
                    log.write(f"Records: {existing_metadata.get('total_records', 'unknown')}\n")
                    log.write("-" * 50 + "\n")
                return True

            logger.info(f"🆕 New data found at page {last_page + 1}! Fetching new pages only...")
            start_page = last_page + 1
        elif existing_metadata and existing_metadata.get('status') == 'in_progress':
            if page_cache.exists():
                last_page = existing_metadata.get('last_page', 0)
                start_page = existing_metadata.get('resume_from_page', last_page + 1)
                total_records = existing_metadata.get('partial_records', 0)
                cached_page_paths = page_cache.list_page_paths()
                logger.info(
                    "📋 Resuming %s from page %s using %s cached page(s).",
                    name,
                    start_page,
                    len(cached_page_paths),
                )
            else:
                logger.warning(
                    "⚠️ Metadata for %s indicates in-progress state, but local page cache is missing. Restarting from page 1.",
                    name,
                )
        else:
            logger.info(f"📋 No existing metadata found")
            start_page = 1

        page = start_page
        pages_processed_this_attempt = 0
        _prev_page_hash: Optional[str] = None
        while True:
            current_params = base_params.copy()
            current_params['pagina'] = page

            logger.info(f"📄 Fetching page {page} for {name} (total so far: {total_records} records)...")
            data_chunk = self.fetch_with_retry(url, current_params)

            if data_chunk is None:
                logger.warning(
                    "⚠️ Request failed while fetching page %s for %s. Preserving checkpoint for resume.",
                    page,
                    name,
                )
                metadata = {
                    'status': 'in_progress',
                    'last_page': page - 1,
                    'resume_from_page': page,
                    'fetched_pages': len(page_cache.list_page_paths()),
                    'partial_records': total_records,
                    'partial_cache_key': partial_cache_key,
                    'last_checkpoint_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'completed': False,
                    'params': base_params,
                }
                self._save_metadata(metadata_key, metadata)
                return False

            if isinstance(data_chunk, list) and not data_chunk:
                if page == start_page and total_records == 0 and not cached_page_paths:
                    logger.info(
                        "✓ Dataset %s returned an empty first page. Saving no-data checkpoint.",
                        name,
                    )
                    metadata = {
                        'status': 'no_data',
                        'last_page': 0,
                        'total_records': 0,
                        'last_checked': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'completed': False,
                        'params': base_params,
                    }
                    self._save_metadata(metadata_key, metadata)
                    page_cache.clear()
                    with open(self.source_log, 'a', encoding='utf-8') as log:
                        log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                        log.write(f"URL: {url}\n")
                        log.write(f"Params: {json.dumps(base_params)}\n")
                        log.write("Status: SKIPPED (checkpointed no-data)\n")
                        log.write("-" * 50 + "\n")
                    return True

                logger.info(f"✓ Empty page received for {name}. Pagination complete.")
                break

            if isinstance(data_chunk, list):
                page_payload = data_chunk
                total_records += len(data_chunk)
                logger.info(f"  → Collected {len(data_chunk)} records (Total: {total_records})")
            else:
                page_payload = [data_chunk]
                total_records += 1

            # Detect API loop: CGU returns the same page content indefinitely for
            # some historical endpoints instead of an empty page. Stop pagination
            # when the current page hash matches the previous page hash.
            _cur_page_hash = hashlib.sha256(
                json.dumps(page_payload, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if _prev_page_hash is not None and _cur_page_hash == _prev_page_hash:
                logger.warning(
                    "🔁 Duplicate page content detected at page %s for %s "
                    "(API loop — CGU repeating same response). "
                    "Treating as end of pagination.",
                    page,
                    name,
                )
                # Roll back the duplicate page: don't count it as new data.
                if isinstance(data_chunk, list):
                    total_records -= len(data_chunk)
                else:
                    total_records -= 1
                page -= 1
                break
            _prev_page_hash = _cur_page_hash

            page_cache.write_page(page, page_payload)
            metadata = {
                'status': 'in_progress',
                'last_page': page,
                'resume_from_page': page + 1,
                'fetched_pages': len(page_cache.list_page_paths()),
                'partial_records': total_records,
                'partial_cache_key': partial_cache_key,
                'last_checkpoint_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'completed': False,
                'params': base_params,
            }
            self._save_metadata(metadata_key, metadata)
            pages_processed_this_attempt += 1

            if not requires_pagination:
                break

            page += 1
            if pages_processed_this_attempt >= self.max_pages:
                logger.warning(
                    "🛑 Reached MAX_PAGES (%s) in this run for %s. Preserving checkpoint at page %s.",
                    self.max_pages,
                    name,
                    page - 1,
                )
                return False

            time.sleep(self.rate_limit_delay)

        page_paths = page_cache.list_page_paths()
        if not page_paths:
            logger.error(f"❌ No data collected for {name}.")
            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write(f"Status: FAILED (no data returned)\n")
                log.write("-" * 50 + "\n")
            return False

        logger.info(f"✓ Pagination complete: {len(page_paths)} page(s), {total_records} total records")
        logger.info(f"📦 Merging {len(page_paths)} page(s) for {name}...")

        all_data = []
        for page_path in page_paths:
            with open(page_path, 'r', encoding='utf-8') as f:
                page_data = json.load(f)
                all_data.extend(page_data)

        content_text = json.dumps(all_data, ensure_ascii=False)
        local_digest = self._calculate_content_digest(content_text)

        metadata = {
            'status': 'completed',
            'last_page': page - 1,
            'total_records': total_records,
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
            'completed': True,
            'params': base_params,
        }
        self._save_metadata(metadata_key, metadata)
        logger.info(f"💾 Saved metadata: {page - 1} pages, marked as complete")

        if self._file_is_valid(s3_key, local_digest):
            logger.info(f"⏭️ Skipping {name} - already matches S3 version.")
            page_cache.clear()
            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write("Status: SKIPPED (already in S3, matches SHA-256)\n")
                log.write(f"Pages: {len(page_paths)}\n")
                log.write(f"Records: {total_records}\n")
                log.write("-" * 50 + "\n")
            return True

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content_text.encode('utf-8'),
                ContentType='application/json; charset=utf-8',
                Metadata={CONTENT_SHA256_METADATA_KEY: local_digest},
            )
            logger.info(f"✅ Landed in Bronze: {s3_key} ({len(all_data)} records)")
            page_cache.clear()

            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write(f"Status: SUCCESS\n")
                log.write(f"Pages: {len(page_paths)}\n")
                log.write(f"Records: {total_records}\n")
                log.write(f"S3 Key: {s3_key}\n")
                log.write("-" * 50 + "\n")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to upload {name} to S3: {e}")
            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write(f"Status: FAILED (S3 upload error)\n")
                log.write(f"Error: {str(e)}\n")
                log.write(f"Pages fetched: {len(page_paths)}\n")
                log.write(f"Records fetched: {total_records}\n")
                log.write("-" * 50 + "\n")
            return False

    def run_full_ingestion(self, max_retry_rounds=2):
        """
        Iterates through the metadata and performs the raw data dump to S3.
        Handles pagination for Transparency Portal APIs.
        Retries failed datasets in subsequent rounds.
        
        :param max_retry_rounds: Maximum number of retry rounds for failed datasets (default: 2)
        """
        if not self.api_key:
            raise RuntimeError(
                "TRANSPARENCY_API_KEY is required for Transparency Portal ingestion. "
                "Set it in your environment (or .env) and retry."
            )
        base_url = self.config['api_base_url']
        
        # Track failed datasets for retry
        failed_datasets = []

        # Bulk prefetch every Transparency metadata checkpoint in one S3 list +
        # parallel GETs. Turns the 156x serial S3 round-trips in the fast path
        # into one bulk fetch and in-memory dict lookups.
        prefetched_metadata = self._prefetch_metadata_index()

        # First round: process all datasets
        logger.info("=" * 60)
        logger.info("🔄 ROUND 1: Initial ingestion attempt")
        logger.info("=" * 60)
        
        selected_datasets = self._get_selected_datasets()

        for ds in selected_datasets:
            base_name = ds['name']
            endpoint = self._normalize_endpoint(ds['endpoint'])
            params = ds.get('params', {})
            filename = ds['filename']

            url = self._build_url(base_url, endpoint)
            base_params = params.copy() if isinstance(params, dict) else {}
            requires_pagination = ds.get('requires_pagination', False)

            if self._should_expand_months(endpoint, base_params):
                month_dss = list(self._expand_monthly_datasets(ds, base_params))

                try:
                    requested_workers = int(os.getenv("TRANSPARENCY_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))
                except ValueError:
                    requested_workers = DEFAULT_MAX_WORKERS
                max_workers = max(1, min(requested_workers, len(month_dss)))

                if max_workers > 1 and len(month_dss) > 1:
                    logger.info(
                        "🧵 Processing %d monthly datasets for %s with %d parallel workers "
                        "(TRANSPARENCY_MAX_WORKERS=%d)",
                        len(month_dss), base_name, max_workers, requested_workers,
                    )
                    logger.info(f"🔗 Source URL: {url}")
                    with ThreadPoolExecutor(
                        max_workers=max_workers,
                        thread_name_prefix="transparency",
                    ) as pool:
                        futures = {
                            pool.submit(
                                self._run_single_ingestion,
                                m["name"], url, m["params"],
                                m["filename"], m["requires_pagination"],
                                prefetched_metadata,
                            ): m
                            for m in month_dss
                        }
                        for fut in as_completed(futures):
                            m = futures[fut]
                            try:
                                success = fut.result()
                            except Exception as exc:
                                logger.error(f"❌ Worker for {m['name']} raised: {exc}")
                                success = False
                            if not success:
                                failed_datasets.append(m)
                else:
                    for month_ds in month_dss:
                        logger.info(f"🚀 Processing {month_ds['name']}...")
                        logger.info(f"🔗 Source URL: {url}")
                        success = self._run_single_ingestion(
                            month_ds["name"],
                            url,
                            month_ds["params"],
                            month_ds["filename"],
                            month_ds["requires_pagination"],
                            prefetched_metadata,
                        )
                        if not success:
                            failed_datasets.append(month_ds)
            else:
                logger.info(f"🚀 Processing {base_name}...")
                logger.info(f"🔗 Source URL: {url}")

                success = self._run_single_ingestion(
                    base_name, url, base_params, filename, requires_pagination,
                    prefetched_metadata,
                )

                if not success:
                    failed_datasets.append(ds)

        # Retry rounds for failed datasets
        retry_round = 2
        while failed_datasets and retry_round <= max_retry_rounds:
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"🔄 ROUND {retry_round}: Retrying {len(failed_datasets)} failed dataset(s)")
            logger.info("=" * 60)

            current_failed = failed_datasets.copy()
            failed_datasets = []

            # Retry rounds skip the prefetched index on purpose: by now some
            # checkpoints may have been written by the first round, and a live
            # S3 GetObject reflects the freshest state for the (small) retry set.
            for ds in current_failed:
                base_name = ds['name']
                endpoint = self._normalize_endpoint(ds['endpoint'])
                params = ds.get('params', {})
                filename = ds['filename']

                url = self._build_url(base_url, endpoint)
                base_params = params.copy() if isinstance(params, dict) else {}
                requires_pagination = ds.get('requires_pagination', False)

                logger.info(f"🔁 Retrying {base_name}...")
                logger.info(f"🔗 Source URL: {url}")

                success = self._run_single_ingestion(base_name, url, base_params, filename, requires_pagination)

                if not success:
                    failed_datasets.append(ds)

            retry_round += 1
        
        # Final summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 INGESTION SUMMARY")
        logger.info("=" * 60)
        total_datasets = len(selected_datasets)
        successful_datasets = total_datasets - len(failed_datasets)
        logger.info(f"✅ Successful: {successful_datasets}/{total_datasets}")
        
        if failed_datasets:
            logger.warning(f"❌ Failed: {len(failed_datasets)}/{total_datasets}")
            logger.warning("Failed datasets:")
            for ds in failed_datasets:
                logger.warning(f"  - {ds['name']}")
        else:
            logger.info("🎉 All datasets ingested successfully!")
        logger.info("=" * 60)
        return not failed_datasets

if __name__ == "__main__":
    # AWS Configuration
    BUCKET_NAME = get_s3_bucket_name(default="enok-mba-thesis-datalake")
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "transparency_metadata.json"

    ingestor = TransparencyIngestor(BUCKET_NAME, CONFIG_FILE)
    raise SystemExit(0 if ingestor.run_full_ingestion() else 1)
