import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.transparency_client import TransparencyIngestor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_connectivity():
    """Test basic API connectivity without S3 upload."""
    logger.info("=" * 60)
    logger.info("BRONZE II - Transparency Portal Connectivity Test")
    logger.info("=" * 60)
    
    api_key = os.getenv("TRANSPARENCY_API_KEY")
    if not api_key:
        logger.error("❌ TRANSPARENCY_API_KEY not set. Please configure your environment.")
        logger.info("   Set it with: $env:TRANSPARENCY_API_KEY = 'your-key-here'")
        return False
    
    logger.info(f"✓ API Key found: {api_key[:10]}...{api_key[-4:]}")
    
    config_path = Path(__file__).parent.parent / "config" / "transparency_metadata.json"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info(f"✓ Loaded metadata: {len(config['datasets'])} datasets configured")
    logger.info(f"  - Rate limit: {config['rate_limit']['delay_between_requests']}s between requests")
    logger.info(f"  - Max pages: {config['pagination']['max_pages']}")
    
    import requests
    
    test_dataset = config['datasets'][5]
    base_url = config['api_base_url']
    url = f"{base_url}{test_dataset['endpoint']}"
    
    logger.info(f"\n🧪 Testing endpoint: {test_dataset['name']}")
    logger.info(f"   URL: {url}")
    logger.info(f"   Params: {test_dataset['params']}")
    
    try:
        headers = {"chave-api-dados": api_key}
        response = requests.get(url, params=test_dataset['params'], headers=headers, timeout=30)
        
        logger.info(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"   ✅ SUCCESS: Received {len(data) if isinstance(data, list) else 1} records")
            if isinstance(data, list) and len(data) > 0:
                logger.info(f"   Sample keys: {list(data[0].keys())[:5]}")
            return True
        elif response.status_code == 429:
            logger.error(f"   ❌ RATE LIMIT HIT (429). Wait before retrying.")
            return False
        else:
            logger.error(f"   ❌ FAILED: {response.text[:200]}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ EXCEPTION: {e}")
        return False

def test_single_dataset_ingestion():
    """Test full ingestion flow for a single small dataset."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE II - Single Dataset Ingestion Test")
    logger.info("=" * 60)
    
    bucket_name = os.getenv("S3_BUCKET_NAME", "enok-mba-thesis-datalake")
    config_path = Path(__file__).parent.parent / "config" / "transparency_metadata.json"
    
    logger.info(f"Target S3 Bucket: {bucket_name}")
    
    try:
        import boto3
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ S3 bucket '{bucket_name}' is accessible")
    except Exception as e:
        logger.error(f"❌ S3 bucket check failed: {e}")
        logger.info("   Verify AWS credentials with: aws s3 ls")
        return False
    
    ingestor = TransparencyIngestor(bucket_name, config_path)
    
    test_dataset_index = 5
    test_ds = ingestor.config['datasets'][test_dataset_index]
    
    logger.info(f"\n🚀 Running ingestion for: {test_ds['name']}")
    logger.info(f"   This is a REAL ingestion that will upload to S3")
    logger.info(f"   Expected S3 path: s3://{bucket_name}/bronze/transparency/{test_ds['filename']}")
    
    original_datasets = ingestor.config['datasets']
    ingestor.config['datasets'] = [test_ds]
    
    try:
        ingestor.run_full_ingestion()
        logger.info("\n✅ TEST INGESTION COMPLETED SUCCESSFULLY")
        logger.info(f"   Check S3: aws s3 ls s3://{bucket_name}/bronze/transparency/")
        return True
    except Exception as e:
        logger.error(f"\n❌ TEST INGESTION FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
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
        user_input = input("\n⚠️  Proceed with S3 ingestion test? This will upload data. (yes/no): ")
        if user_input.lower() in ['yes', 'y']:
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
