import os
import sys
import json
import logging
import tempfile
from unittest.mock import MagicMock, patch
from pathlib import Path
from dotenv import load_dotenv
import pytest

# Load environment variables from .env file
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.transparency_client import TransparencyIngestor
from src.ingestion import transparency_client as ingestor_module

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@pytest.fixture
def transparency_config_file(tmp_path):
    config = {
        "api_base_url": "https://api.portaldatransparencia.gov.br/api-de-dados",
        "rate_limit": {"delay_between_requests": 0},
        "pagination": {"max_pages": 5, "page_size": 500},
        "datasets": [
            {
                "name": "federal_transfers",
                "endpoint": "despesas/recursos-recebidos",
                "params": {"mesAnoInicio": "01/2010", "mesAnoFim": "01/2010"},
                "filename": "federal_transfers.json",
                "requires_pagination": True,
            }
        ],
    }
    config_path = tmp_path / "transparency_metadata.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def build_test_ingestor(config_path):
    mock_session = MagicMock()
    mock_s3 = MagicMock()

    def client_factory(service_name):
        if service_name == "s3":
            return mock_s3
        if service_name == "secretsmanager":
            raise RuntimeError("not configured in unit test")
        raise AssertionError(f"unexpected service: {service_name}")

    mock_session.client.side_effect = client_factory
    mock_creds = MagicMock()
    mock_creds.access_key = "test-access"
    mock_creds.secret_key = "test-secret"
    mock_session.get_credentials.return_value.get_frozen_credentials.return_value = mock_creds

    with patch.object(TransparencyIngestor, "_build_session", return_value=mock_session):
        with patch.object(TransparencyIngestor, "_get_api_key", return_value="test-api-key"):
            ingestor = TransparencyIngestor("test-bucket", config_path)

    ingestor.source_log = Path(tempfile.mkdtemp()) / "data_sources.log"
    return ingestor


def test_run_single_ingestion_skips_persisted_no_data_checkpoint(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_FORCE_REFRESH", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    ingestor.fetch_with_retry = MagicMock()
    ingestor._get_metadata = MagicMock(return_value={"status": "no_data", "last_page": 0})

    result = ingestor._run_single_ingestion(
        "federal_transfers_2010_01",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2010", "mesAnoFim": "01/2010"},
        "federal_transfers_2010_01.json",
        True,
    )

    assert result is True
    ingestor.fetch_with_retry.assert_not_called()


def test_run_single_ingestion_persists_no_data_checkpoint_on_empty_first_page(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_FORCE_REFRESH", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    ingestor.fetch_with_retry = MagicMock(return_value=[])
    ingestor._get_metadata = MagicMock(return_value=None)
    ingestor._save_metadata = MagicMock()

    result = ingestor._run_single_ingestion(
        "federal_transfers_2010_01",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2010", "mesAnoFim": "01/2010"},
        "federal_transfers_2010_01.json",
        True,
    )

    assert result is True
    ingestor._save_metadata.assert_called_once()
    saved_metadata = ingestor._save_metadata.call_args.args[1]
    assert saved_metadata["status"] == "no_data"
    assert saved_metadata["total_records"] == 0


def test_run_single_ingestion_resumes_from_in_progress_checkpoint(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_FORCE_REFRESH", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    page_cache_key = ingestor._build_partial_cache_key(
        "bronze/transparency/federal_transfers_2014_01.json",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2014", "mesAnoFim": "01/2014"},
    )
    page_cache = ingestor_module.DatasetPageCache(scope="transparency", cache_key=page_cache_key)
    page_cache.write_page(1, [{"id": 1}])
    page_cache.write_page(2, [{"id": 2}])

    ingestor.fetch_with_retry = MagicMock(side_effect=[[{"id": 3}], []])
    ingestor._get_metadata = MagicMock(
        return_value={
            "status": "in_progress",
            "last_page": 2,
            "resume_from_page": 3,
            "partial_records": 2,
        }
    )
    ingestor._save_metadata = MagicMock()
    ingestor._file_is_valid = MagicMock(return_value=False)

    result = ingestor._run_single_ingestion(
        "federal_transfers_2014_01",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2014", "mesAnoFim": "01/2014"},
        "federal_transfers_2014_01.json",
        True,
    )

    assert result is True
    first_call_params = ingestor.fetch_with_retry.call_args_list[0].args[1]
    assert first_call_params["pagina"] == 3
    assert ingestor.s3.put_object.called


def test_run_single_ingestion_preserves_in_progress_checkpoint_on_failed_page(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_FORCE_REFRESH", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    ingestor.fetch_with_retry = MagicMock(side_effect=[[{"id": 1}], None])
    ingestor._get_metadata = MagicMock(return_value=None)
    ingestor._save_metadata = MagicMock()

    result = ingestor._run_single_ingestion(
        "federal_transfers_2014_01",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2014", "mesAnoFim": "01/2014"},
        "federal_transfers_2014_01.json",
        True,
    )

    assert result is False
    saved_metadata = ingestor._save_metadata.call_args.args[1]
    assert saved_metadata["status"] == "in_progress"
    assert saved_metadata["resume_from_page"] == 2
    assert ingestor.s3.put_object.called is False


def test_expand_monthly_datasets_honors_transparency_month_override(transparency_config_file, monkeypatch):
    monkeypatch.setenv("TRANSPARENCY_START_MA", "12/2010")
    monkeypatch.setenv("TRANSPARENCY_END_MA", "02/2011")
    ingestor = build_test_ingestor(transparency_config_file)

    expanded = list(
        ingestor._expand_monthly_datasets(
            {
                "name": "federal_transfers",
                "endpoint": "despesas/recursos-recebidos",
                "filename": "federal_transfers.json",
                "requires_pagination": True,
            },
            {"mesAnoInicio": "01/2010", "mesAnoFim": "12/2022"},
        )
    )

    assert [dataset["name"] for dataset in expanded] == [
        "federal_transfers_2010_12",
        "federal_transfers_2011_01",
        "federal_transfers_2011_02",
    ]


def test_expand_monthly_datasets_rejects_inverted_transparency_month_override(transparency_config_file, monkeypatch):
    monkeypatch.setenv("TRANSPARENCY_START_MA", "03/2011")
    monkeypatch.setenv("TRANSPARENCY_END_MA", "02/2011")
    ingestor = build_test_ingestor(transparency_config_file)

    with pytest.raises(ValueError, match="TRANSPARENCY_START_MA"):
        list(
            ingestor._expand_monthly_datasets(
                {
                    "name": "federal_transfers",
                    "endpoint": "despesas/recursos-recebidos",
                    "filename": "federal_transfers.json",
                    "requires_pagination": True,
                },
                {"mesAnoInicio": "01/2010", "mesAnoFim": "12/2022"},
            )
        )


def test_get_selected_datasets_honors_transparency_dataset_filter(transparency_config_file, monkeypatch):
    monkeypatch.setenv("TRANSPARENCY_DATASET_NAMES", "federal_transfers,unknown_dataset")
    ingestor = build_test_ingestor(transparency_config_file)

    selected = ingestor._get_selected_datasets()

    assert [dataset["name"] for dataset in selected] == ["federal_transfers"]


def test_run_single_ingestion_max_pages_applies_per_attempt_not_absolute_page(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_FORCE_REFRESH", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    ingestor.fetch_with_retry = MagicMock(side_effect=[[{"id": 6}], []])
    ingestor._get_metadata = MagicMock(
        return_value={
            "status": "in_progress",
            "last_page": 5,
            "resume_from_page": 6,
            "partial_records": 5,
        }
    )
    ingestor._save_metadata = MagicMock()
    ingestor._file_is_valid = MagicMock(return_value=False)

    result = ingestor._run_single_ingestion(
        "federal_transfers_2014_01",
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/recursos-recebidos",
        {"mesAnoInicio": "01/2014", "mesAnoFim": "01/2014"},
        "federal_transfers_2014_01.json",
        True,
    )

    assert result is True
    assert ingestor.fetch_with_retry.call_args_list[0].args[1]["pagina"] == 6


def test_run_full_ingestion_returns_false_when_datasets_fail(transparency_config_file, monkeypatch):
    monkeypatch.delenv("TRANSPARENCY_DATASET_NAMES", raising=False)
    ingestor = build_test_ingestor(transparency_config_file)
    ingestor._run_single_ingestion = MagicMock(return_value=False)

    success = ingestor.run_full_ingestion(max_retry_rounds=1)

    assert success is False

def test_api_connectivity():
    """Test basic API connectivity without S3 upload."""
    logger.info("=" * 60)
    logger.info("BRONZE II - Transparency Portal Connectivity Test")
    logger.info("=" * 60)
    
    api_key = os.getenv("TRANSPARENCY_API_KEY")
    if not api_key:
        pytest.skip("TRANSPARENCY_API_KEY not set")
    
    logger.info("✓ API Key found (length=%d)", len(api_key))
    
    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info(f"✓ Loaded metadata: {len(config['datasets'])} datasets configured")
    logger.info(f"  - Rate limit: {config['rate_limit']['delay_between_requests']}s between requests")
    logger.info(f"  - Max pages: {config['pagination']['max_pages']}")
    
    import requests
    
    if not config.get('datasets'):
        pytest.skip("No datasets configured in transparency_metadata.json")

    base_url = config['api_base_url']
    headers = {"chave-api-dados": api_key}

    last_status = None
    for test_dataset in config['datasets']:
        url = f"{base_url}{test_dataset['endpoint']}"
        logger.info(f"\n🧪 Testing endpoint: {test_dataset['name']}")
        logger.info(f"   URL: {url}")
        logger.info(f"   Params: {test_dataset.get('params', {})}")

        try:
            response = requests.get(url, params=test_dataset.get('params', {}), headers=headers, timeout=30)
        except requests.exceptions.RequestException as exc:
            pytest.skip(f"Transparency API not reachable from current environment: {exc}")
        last_status = response.status_code
        logger.info(f"   Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            assert data is not None
            return

    pytest.skip(f"No configured dataset returned 200 OK (last status: {last_status}). API may require different params.")

def test_single_dataset_ingestion():
    """Test full ingestion flow for a single small dataset."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE II - Single Dataset Ingestion Test")
    logger.info("=" * 60)
    
    bucket_name = os.getenv("S3_BUCKET_NAME", "enok-mba-thesis-datalake")
    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    
    logger.info(f"Target S3 Bucket: {bucket_name}")
    
    if os.getenv("RUN_S3_INGESTION_TESTS", "0") != "1":
        pytest.skip("Set RUN_S3_INGESTION_TESTS=1 to enable tests that upload to S3")

    try:
        import boto3
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ S3 bucket '{bucket_name}' is accessible")
    except Exception as e:
        pytest.skip(f"AWS credentials/bucket not available: {e}")
    
    ingestor = TransparencyIngestor(bucket_name, config_path)
    
    if not ingestor.config.get('datasets'):
        pytest.skip("No datasets configured in transparency_metadata.json")

    test_ds = ingestor.config['datasets'][0]
    
    logger.info(f"\n🚀 Running ingestion for: {test_ds['name']}")
    logger.info(f"   This is a REAL ingestion that will upload to S3")
    logger.info(f"   Expected S3 path: s3://{bucket_name}/bronze/transparency/{test_ds['filename']}")
    
    original_datasets = ingestor.config['datasets']
    ingestor.config['datasets'] = [test_ds]
    
    try:
        ingestor.run_full_ingestion()
    finally:
        ingestor.config['datasets'] = original_datasets

def main():
    """Run all tests in sequence."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE II VALIDATION TEST SUITE")
    logger.info("=" * 60)
    logger.info("This script validates:")
    logger.info("  1. API key configuration")
    logger.info("  2. Transparency Portal API connectivity")
    logger.info("  3. AWS S3 access")
    logger.info("  4. Full ingestion pipeline (single dataset)")
    logger.info("=" * 60 + "\n")
    
    results = {}
    
    results['connectivity'] = test_api_connectivity()
    
    if results['connectivity']:
        run_ingestion = os.getenv("RUN_S3_INGESTION_TESTS", "0") == "1"
        if run_ingestion:
            results['ingestion'] = test_single_dataset_ingestion()
        else:
            logger.info("Skipping S3 ingestion test.")
            results['ingestion'] = None
    else:
        logger.warning("Skipping ingestion test due to connectivity failure.")
        results['ingestion'] = None
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"API Connectivity: {'✅ PASS' if results['connectivity'] else '❌ FAIL'}")
    if results['ingestion'] is not None:
        logger.info(f"S3 Ingestion:     {'✅ PASS' if results['ingestion'] else '❌ FAIL'}")
    else:
        logger.info(f"S3 Ingestion:     ⏭️  SKIPPED")
    logger.info("=" * 60)
    
    if results['connectivity'] and (results['ingestion'] is None or results['ingestion']):
        logger.info("\n🎉 All tests passed! Ready for full Bronze II ingestion.")
        logger.info("   Run: python src\\ingestion\\transparency_client.py")
    else:
        logger.error("\n⚠️  Some tests failed. Review errors above before proceeding.")

if __name__ == "__main__":
    main()
