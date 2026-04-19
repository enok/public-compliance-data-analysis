"""
Local Bronze Downloader - Downloads raw data directly to local filesystem.

This module provides a local-only Bronze ingestion path for notebooks that need
to bootstrap data without AWS credentials. It reuses the IBGE and Transparency
metadata configs but writes to local Parquet/JSON files instead of S3.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from src.ingestion.http_client import HTTPClient
from src.config.runtime_config import get_api_base_url

logger = logging.getLogger(__name__)


class LocalBronzeDownloader:
    """Downloads Bronze data to local filesystem (no AWS required)."""

    def __init__(
        self,
        local_data_dir: Path,
        ibge_config_path: Path,
        transparency_config_path: Path,
        transparency_api_key: Optional[str] = None,
    ):
        """
        Initialize local downloader.

        :param local_data_dir: Root directory for data (creates bronze/, silver/, gold/).
        :param ibge_config_path: Path to config/ibge_metadata.json.
        :param transparency_config_path: Path to config/transparency_metadata.json.
        :param transparency_api_key: API key for Transparency Portal (optional).
        """
        self.local_data_dir = Path(local_data_dir)
        self.bronze_dir = self.local_data_dir / "bronze"
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

        with open(ibge_config_path, "r", encoding="utf-8") as f:
            self.ibge_config = json.load(f)
        self.ibge_config["api_base_url"] = get_api_base_url(
            "ibge_sidra_base_url", default=self.ibge_config.get("api_base_url")
        )

        with open(transparency_config_path, "r", encoding="utf-8") as f:
            self.transparency_config = json.load(f)

        self.transparency_api_key = transparency_api_key or os.environ.get(
            "TRANSPARENCY_API_KEY"
        )

        self.http = HTTPClient(
            max_retries=5,
            timeout=180,
            user_agent="public-compliance-data-analysis/local-bronze",
        )

    def _build_ibge_url(self, ds: dict) -> str:
        if ds.get("url"):
            return ds["url"]
        base = self.ibge_config["api_base_url"]
        table = ds["table_id"]
        var = ds.get("variable", "allxp")
        period = str(ds["period"]).replace(" ", "%20")
        classif = ds.get("classifications", "")
        url = f"{base}/t/{table}/n6/all/v/{var}/p/{period}"
        if classif:
            url += f"/{classif}"
        url += "?formato=json"
        return url

    def _local_ibge_path(self, ds: dict) -> Path:
        prefix = ds.get("bronze_prefix", "bronze/ibge")
        # Strip bronze/ prefix since we handle it via self.bronze_dir
        if prefix.startswith("bronze/"):
            prefix = prefix[len("bronze/"):]
        return self.bronze_dir / prefix / ds["filename"]

    def download_ibge_dataset(self, dataset_name: str, force: bool = False) -> bool:
        """Download a single IBGE dataset to local filesystem."""
        datasets = [
            ds for ds in self.ibge_config.get("datasets", [])
            if ds.get("name") == dataset_name
        ]
        if not datasets:
            logger.warning(f"IBGE dataset '{dataset_name}' not found in config")
            return False

        ds = datasets[0]
        local_path = self._local_ibge_path(ds)

        if local_path.exists() and not force:
            logger.info(f"[SKIP] {dataset_name} already exists: {local_path}")
            return True

        url = self._build_ibge_url(ds)
        logger.info(f"[DOWNLOAD] {dataset_name} from {url}")

        try:
            content = self.http.fetch(url, return_json=False)
            if not content:
                logger.error(f"[FAIL] {dataset_name}: empty response")
                return False

            # Validate JSON
            json.loads(content)

            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(content, encoding="utf-8")
            logger.info(f"[OK] Saved {dataset_name} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"[FAIL] {dataset_name}: {e}")
            return False

    def download_all_ibge(self, force: bool = False) -> dict:
        """Download all IBGE datasets. Returns {name: success_bool}."""
        results = {}
        for ds in self.ibge_config.get("datasets", []):
            name = ds.get("name")
            if name:
                results[name] = self.download_ibge_dataset(name, force=force)
        return results

    def check_bronze_availability(self) -> dict:
        """Check which Bronze datasets are available locally."""
        status = {"ibge": {}, "transparency": {}}

        for ds in self.ibge_config.get("datasets", []):
            name = ds.get("name")
            if name:
                local_path = self._local_ibge_path(ds)
                status["ibge"][name] = local_path.exists()

        return status
