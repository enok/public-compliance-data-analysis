import os
import boto3
import json
import logging
import time
import hashlib
from pathlib import Path
from botocore.exceptions import ClientError

# Handle both direct execution and module import
try:
    from src.ingestion.http_client import HTTPClient
    from src.ingestion.ingestion_utils import SkipMarkerCache, calculate_md5, s3_object_exists
except ModuleNotFoundError:
    from http_client import HTTPClient
    from ingestion_utils import SkipMarkerCache, calculate_md5, s3_object_exists

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

    def _load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _file_is_valid(self, s3_key, local_md5):
        """Check if file exists in S3 and matches MD5 to avoid redundant ingestion."""
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            s3_md5 = response['ETag'].strip('"')
            return s3_md5 == local_md5
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
        Iterates through the 8-entry metadata and performs the raw data dump to S3.
        Covers the 4 pillars (População, Saneamento, Alfabetização, Rendimento) for both 2010 and 2022.
        """
        base_url = self.config['api_base_url']

        for ds in self.config['datasets']:
            # Metadata extraction
            table = ds['table_id']
            # Variables default to 'allxp' if not specified [cite: 2, 4, 9]
            var = ds.get('variable', 'allxp')
            # Handle spaces in periods like 'last 1' for 2022 tables [cite: 7]
            period = str(ds['period']).replace(" ", "%20")
            classif = ds.get('classifications', '')

            s3_key = f"bronze/ibge/{ds['filename']}"

            # SIDRA URL Structure: /t/<table>/n6/all/v/<var>/p/<period>/<classifications>?formato=json
            # n6/all ensures we fetch all 5570+ Brazilian municipalities [cite: 2, 6, 8]
            url = f"{base_url}/t/{table}/n6/all/v/{var}/p/{period}"

            if classif:
                url += f"/{classif}"

            url += "?formato=json"

            logger.info(f"🚀 Processing {ds['name']}...")
            logger.info(f"🔗 Source URL: {url}")

            if self.skip_cache.get(s3_key) == "skipped_s3_match":
                logger.info(f"⏭️ Skipping {ds['name']} - recently skipped (local cache).")
                continue

            if self.fast_skip_if_exists:
                if s3_object_exists(self.s3, self.bucket, s3_key):
                    logger.info(f"⏭️ Skipping {ds['name']} - already exists in S3 (fast skip).")
                    self.skip_cache.set(s3_key, "skipped_s3_match")
                    continue

            content_text = self.fetch_with_retry(url)

            if content_text:
                # Basic JSON validation to ensure we didn't get an empty response or HTML error page
                try:
                    data_check = json.loads(content_text)
                    if len(data_check) < 2:
                        logger.warning(f"⚠️ {ds['name']} returned only headers. Verify table parameters.")
                except json.JSONDecodeError:
                    logger.error(f"❌ Invalid JSON received for {ds['name']}. Skipping.")
                    continue

                local_md5 = calculate_md5(content_text)

                if self._file_is_valid(s3_key, local_md5):
                    logger.info(f"⏭️ Skipping {ds['name']} - already matches S3 version.")
                    self.skip_cache.set(s3_key, "skipped_s3_match")
                    continue

                try:
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=s3_key,
                        Body=content_text.encode('utf-8'),
                        ContentType='application/json; charset=utf-8'
                    )
                    logger.info(f"✅ Landed in Bronze: {s3_key}")
                    self.log_source(ds['name'], url)
                except Exception as e:
                    logger.error(f"❌ Failed to upload {ds['name']} to S3: {e}")
            else:
                logger.error(f"❌ Critical: Fetch failed for {ds['name']} after maximum retries.")

if __name__ == "__main__":
    # AWS Configuration - Update bucket name as needed
    BUCKET_NAME = "enok-mba-thesis-datalake"
    # Assuming standard directory structure for your project
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "ibge_metadata.json"

    ingestor = IBGEIngestor(BUCKET_NAME, CONFIG_FILE)
    ingestor.run_full_ingestion()