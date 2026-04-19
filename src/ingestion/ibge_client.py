import os
import boto3
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from botocore.exceptions import ClientError

from src.ingestion.http_client import HTTPClient
from src.config.runtime_config import get_api_base_url, get_s3_bucket_name
from src.ingestion.ingestion_utils import (
    CONTENT_SHA256_METADATA_KEY,
    SkipMarkerCache,
    calculate_content_digest,
    s3_object_exists,
)

# Configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IBGEIngestor:
    def __init__(self, bucket_name, config_path):
        """
        Initializes the ingestor for the MBA Thesis Data Lake.
        :param bucket_name: S3 Bucket for the Bronze Layer.
        :param config_path: Path to the ibge_metadata.json file.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.config = self._load_config(config_path)
        self.http_client = HTTPClient(
            max_retries=10,
            timeout=180,
            user_agent="public-compliance-data-analysis/ibge"
        )

        # Thesis Documentation: Log of raw data sources
        self.source_log = Path(__file__).parent.parent.parent / "docs" / "data_sources.log"
        os.makedirs(self.source_log.parent, exist_ok=True)

        self.skip_cache = SkipMarkerCache(scope="ibge", ttl_seconds=300)
        self.fast_skip_if_exists = os.getenv("IBGE_FAST_SKIP_IF_EXISTS", "1") == "1"

    def _get_selected_datasets(self):
        selected_names = os.getenv("IBGE_DATASET_NAMES", "").strip()
        if not selected_names:
            return self.config['datasets']

        selected_set = {name.strip() for name in selected_names.split(",") if name.strip()}
        datasets = [ds for ds in self.config['datasets'] if ds.get('name') in selected_set]

        missing = sorted(selected_set - {ds.get('name') for ds in datasets})
        if missing:
            logger.warning("⚠️ Unknown IBGE dataset filter entries ignored: %s", ", ".join(missing))

        logger.info("📋 IBGE dataset filter active: %s", ", ".join(ds['name'] for ds in datasets))
        return datasets

    def _load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        config['api_base_url'] = get_api_base_url(
            'ibge_sidra_base_url',
            default=config.get('api_base_url'),
        )
        for dataset in config.get('datasets', []):
            if dataset.get('name') == 'ipca_monthly':
                dataset['url'] = get_api_base_url(
                    'bcb_ipca_monthly_url',
                    default=dataset.get('url'),
                )
        return config

    def _build_dataset_url(self, base_url: str, ds: dict) -> str:
        if ds.get('url'):
            return ds['url']

        table = ds['table_id']
        var = ds.get('variable', 'allxp')
        period = str(ds['period']).replace(" ", "%20")
        classif = ds.get('classifications', '')

        url = f"{base_url}/t/{table}/n6/all/v/{var}/p/{period}"
        if classif:
            url += f"/{classif}"
        url += "?formato=json"
        return url

    def _build_s3_key(self, ds: dict) -> str:
        bronze_prefix = ds.get('bronze_prefix', 'bronze/ibge').rstrip('/')
        return f"{bronze_prefix}/{ds['filename']}"

    def _ingest_dataset(self, base_url: str, ds: dict) -> None:
        s3_key = self._build_s3_key(ds)
        url = self._build_dataset_url(base_url, ds)

        logger.info(f"🚀 Processing {ds['name']}...")
        logger.info(f"🔗 Source URL: {url}")

        if self.skip_cache.get(s3_key) == "skip-match":
            logger.info(f"⏭️ Skipping {ds['name']} - recently skipped (local cache).")
            return

        if self.fast_skip_if_exists and s3_object_exists(self.s3, self.bucket, s3_key):
            logger.info(f"⏭️ Skipping {ds['name']} - already exists in S3 (fast skip).")
            self.skip_cache.set(s3_key, "skip-match")
            return

        content_text = self.fetch_with_retry(url)
        if not content_text:
            logger.error(f"❌ Critical: Fetch failed for {ds['name']} after maximum retries.")
            return

        try:
            data_check = json.loads(content_text)
            if len(data_check) < 2:
                logger.warning(f"⚠️ {ds['name']} returned only headers. Verify table parameters.")
        except json.JSONDecodeError:
            logger.error(f"❌ Invalid JSON received for {ds['name']}. Skipping.")
            return

        local_digest = calculate_content_digest(content_text)
        if self._file_is_valid(s3_key, local_digest):
            logger.info(f"⏭️ Skipping {ds['name']} - already matches S3 version.")
            self.skip_cache.set(s3_key, "skip-match")
            return

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content_text.encode('utf-8'),
                ContentType='application/json; charset=utf-8',
                Metadata={CONTENT_SHA256_METADATA_KEY: local_digest},
            )
            logger.info(f"✅ Landed in Bronze: {s3_key}")
            self.log_source(ds['name'], url)
        except Exception as e:
            logger.error(f"❌ Failed to upload {ds['name']} to S3: {e}")

    def _file_is_valid(self, s3_key, local_digest):
        """Check if file exists in S3 and matches the stored SHA-256 digest."""
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            metadata = response.get("Metadata", {})
            return metadata.get(CONTENT_SHA256_METADATA_KEY) == local_digest
        except ClientError:
            return False

    def fetch_with_retry(self, url):
        """Fetches data from SIDRA API using shared HTTP client."""
        return self.http_client.fetch(url, return_json=False)

    def log_source(self, name, url):
        """Append the exact API URL to a log file for Thesis reproducibility/documentation."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.source_log, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] Dataset: {name}\nURL: {url}\n{'-'*50}\n")

    def run_full_ingestion(self):
        """
        Iterates through the configured metadata and performs the raw data dump to S3.

        Datasets are fetched in parallel via a thread pool. Each task is an independent
        HTTP GET + S3 PUT, so concurrency is safe. Controlled via the IBGE_MAX_WORKERS
        env var (default: min(6, number_of_datasets)). Set to 1 to force serial mode.
        """
        base_url = self.config['api_base_url']
        datasets = self._get_selected_datasets()

        if not datasets:
            return

        try:
            requested_workers = int(os.getenv("IBGE_MAX_WORKERS", "6"))
        except ValueError:
            requested_workers = 6
        max_workers = max(1, min(requested_workers, len(datasets)))

        if max_workers == 1 or len(datasets) == 1:
            for ds in datasets:
                self._ingest_dataset(base_url, ds)
            return

        logger.info(f"🧵 Running IBGE ingestion with {max_workers} parallel workers "
                    f"({len(datasets)} dataset(s))")
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ibge") as pool:
            futures = {
                pool.submit(self._ingest_dataset, base_url, ds): ds.get("name", "<unknown>")
                for ds in datasets
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"❌ Worker for {name} raised: {exc}")

if __name__ == "__main__":
    # AWS Configuration - Update bucket name as needed
    BUCKET_NAME = get_s3_bucket_name(default="enok-mba-thesis-datalake")
    # Assuming standard directory structure for your project
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "ibge_metadata.json"

    ingestor = IBGEIngestor(BUCKET_NAME, CONFIG_FILE)
    ingestor.run_full_ingestion()
