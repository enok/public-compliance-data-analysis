import os
import boto3
import json
import logging
import time
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Handle both direct execution and module import
try:
    from src.ingestion.http_client import HTTPClient
    from src.ingestion.ingestion_utils import SkipMarkerCache, calculate_md5
except ModuleNotFoundError:
    from http_client import HTTPClient
    from ingestion_utils import SkipMarkerCache, calculate_md5

# Load environment variables from .env file
load_dotenv()

# Configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransparencyIngestor:
    def __init__(self, bucket_name, config_path):
        """
        Initializes the ingestor for the Transparency Portal data (Bronze II).
        :param bucket_name: S3 Bucket for the Bronze Layer.
        :param config_path: Path to the transparency_metadata.json file.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.config = self._load_config(config_path)
        raw_api_key = os.getenv("TRANSPARENCY_API_KEY")
        self.api_key = raw_api_key.strip() if raw_api_key else None
        
        if not self.api_key:
            logger.warning("⚠️ TRANSPARENCY_API_KEY environment variable not found. API calls will fail with 401/403.")

        self.rate_limit_delay = self.config.get('rate_limit', {}).get('delay_between_requests', 3.5)
        self.max_pages = self.config.get('pagination', {}).get('max_pages', 1000)
        self.page_size = self.config.get('pagination', {}).get('page_size', 500)
        
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

    def _load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _calculate_md5(self, content):
        return calculate_md5(content)

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

    def _normalize_endpoint(self, endpoint: str) -> str:
        return str(endpoint or "").strip().lstrip("/")

    def _build_url(self, base_url: str, endpoint: str) -> str:
        base = str(base_url or "").rstrip("/")
        ep = self._normalize_endpoint(endpoint)
        return f"{base}/{ep}" if ep else base

    def _file_is_valid(self, s3_key, local_md5):
        """Check if file exists in S3 and matches MD5 to avoid redundant ingestion."""
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            s3_md5 = response['ETag'].strip('"')
            return s3_md5 == local_md5
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
            except:
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
            except:
                upper = mid
            
            time.sleep(self.rate_limit_delay)
        
        return lower

    def fetch_with_retry(self, url, params):
        """Fetches data from Transparency API using shared HTTP client."""
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

    def _run_single_ingestion(self, name, url, base_params, filename, requires_pagination):
        """
        Run ingestion for a single dataset.
        
        :return: True if successful, False if failed
        """
        s3_key = f"bronze/transparency/{filename}"
        metadata_key = f"bronze/transparency/.metadata/{filename}.meta.json"

        if self.skip_cache.get(s3_key) == "skipped_up_to_date":
            logger.info(f"⏭️ Skipping {name} - recently skipped (local cache).")
            return True

        existing_metadata = self._get_metadata(metadata_key)
        last_page = 0
        start_page = 1

        if existing_metadata and existing_metadata.get('completed', False):
            last_page = existing_metadata.get('last_page', 0)
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
        else:
            logger.info(f"📋 No existing metadata found")
            start_page = 1

        page = start_page
        temp_files = []
        total_records = 0

        try:
            while True:
                current_params = base_params.copy()
                current_params['pagina'] = page

                logger.info(f"📄 Fetching page {page} for {name} (total so far: {total_records} records)...")
                data_chunk = self.fetch_with_retry(url, current_params)

                if not data_chunk:
                    if page > last_page:
                        logger.info(f"✓ No new pages beyond page {last_page}. Pagination complete.")
                    else:
                        logger.warning(f"⚠️ No data returned for page {page} of {name}. Stopping pagination.")
                    break

                if isinstance(data_chunk, list):
                    if not data_chunk:
                        logger.info(f"✓ Empty page received for {name}. Pagination complete.")
                        break

                    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.json')
                    json.dump(data_chunk, temp_file, ensure_ascii=False)
                    temp_file.close()
                    temp_files.append(temp_file.name)

                    total_records += len(data_chunk)
                    logger.info(f"  → Collected {len(data_chunk)} records (Total: {total_records})")
                else:
                    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.json')
                    json.dump([data_chunk], temp_file, ensure_ascii=False)
                    temp_file.close()
                    temp_files.append(temp_file.name)

                    total_records += 1
                    if not requires_pagination:
                        break

                if not requires_pagination:
                    break

                page += 1
                if page > self.max_pages:
                    logger.warning(f"🛑 Reached MAX_PAGES ({self.max_pages}) for {name}. Stopping.")
                    break

                time.sleep(self.rate_limit_delay)

            if not temp_files:
                logger.error(f"❌ No data collected for {name}.")
                with open(self.source_log, 'a', encoding='utf-8') as log:
                    log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                    log.write(f"URL: {url}\n")
                    log.write(f"Params: {json.dumps(base_params)}\n")
                    log.write(f"Status: FAILED (no data returned)\n")
                    log.write("-" * 50 + "\n")
                return False

            logger.info(f"✓ Pagination complete: {len(temp_files)} page(s), {total_records} total records")
            logger.info(f"📦 Merging {len(temp_files)} page(s) for {name}...")

            all_data = []
            for temp_path in temp_files:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    page_data = json.load(f)
                    all_data.extend(page_data)

            content_text = json.dumps(all_data, ensure_ascii=False)
            local_md5 = self._calculate_md5(content_text)

            metadata = {
                'last_page': page - 1,
                'total_records': total_records,
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'completed': True
            }
            self._save_metadata(metadata_key, metadata)
            logger.info(f"💾 Saved metadata: {page - 1} pages, marked as complete")

        finally:
            for temp_path in temp_files:
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temp file {temp_path}: {e}")

        if self._file_is_valid(s3_key, local_md5):
            logger.info(f"⏭️ Skipping {name} - already matches S3 version.")
            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write(f"Status: SKIPPED (already in S3, matches MD5)\n")
                log.write(f"Pages: {len(temp_files)}\n")
                log.write(f"Records: {total_records}\n")
                log.write("-" * 50 + "\n")
            return

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content_text.encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )
            logger.info(f"✅ Landed in Bronze: {s3_key} ({len(all_data)} records)")

            with open(self.source_log, 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Dataset: {name}\n")
                log.write(f"URL: {url}\n")
                log.write(f"Params: {json.dumps(base_params)}\n")
                log.write(f"Status: SUCCESS\n")
                log.write(f"Pages: {len(temp_files)}\n")
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
                log.write(f"Pages fetched: {len(temp_files)}\n")
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
        
        # First round: process all datasets
        logger.info("=" * 60)
        logger.info("🔄 ROUND 1: Initial ingestion attempt")
        logger.info("=" * 60)
        
        for ds in self.config['datasets']:
            base_name = ds['name']
            endpoint = self._normalize_endpoint(ds['endpoint'])
            params = ds.get('params', {})
            filename = ds['filename']

            url = self._build_url(base_url, endpoint)
            base_params = params.copy() if isinstance(params, dict) else {}
            requires_pagination = ds.get('requires_pagination', False)

            # Month-by-month expansion for date range endpoints
            if (
                endpoint == "despesas/recursos-recebidos"
                and isinstance(base_params, dict)
                and base_params.get("mesAnoInicio")
                and base_params.get("mesAnoFim")
                and base_params.get("mesAnoInicio") != base_params.get("mesAnoFim")
            ):
                start_ma = base_params["mesAnoInicio"]
                end_ma = base_params["mesAnoFim"]
                
                # Extract base name without year for simplified naming pattern
                # federal_transfers_2013.json -> federal_transfers
                stem, ext = filename.rsplit(".", 1)
                # Remove trailing year if present (e.g., federal_transfers_2013 -> federal_transfers)
                import re
                base_stem = re.sub(r'_\d{4}$', '', stem)

                for month_dt in self._iter_months_inclusive(start_ma, end_ma):
                    month_str = self._format_mes_ano(month_dt)
                    ym_suffix = month_dt.strftime("%Y_%m")

                    month_params = dict(base_params)
                    month_params["mesAnoInicio"] = month_str
                    month_params["mesAnoFim"] = month_str

                    # Simplified naming: federal_transfers_YYYY_MM.json
                    month_name = f"{base_stem}_{ym_suffix}"
                    month_filename = f"{base_stem}_{ym_suffix}.{ext}"

                    logger.info(f"🚀 Processing {month_name}...")
                    logger.info(f"🔗 Source URL: {url}")
                    
                    success = self._run_single_ingestion(month_name, url, month_params, month_filename, requires_pagination)
                    
                    if not success:
                        # Store the monthly dataset for retry
                        monthly_ds = {
                            "name": month_name,
                            "endpoint": ds["endpoint"],
                            "params": month_params,
                            "filename": month_filename,
                            "requires_pagination": requires_pagination
                        }
                        failed_datasets.append(monthly_ds)
            else:
                logger.info(f"🚀 Processing {base_name}...")
                logger.info(f"🔗 Source URL: {url}")
                
                success = self._run_single_ingestion(base_name, url, base_params, filename, requires_pagination)
                
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
        total_datasets = len(self.config['datasets'])
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

if __name__ == "__main__":
    # AWS Configuration
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "transparency_metadata.json"

    ingestor = TransparencyIngestor(BUCKET_NAME, CONFIG_FILE)
    ingestor.run_full_ingestion()