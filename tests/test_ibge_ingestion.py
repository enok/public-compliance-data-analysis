import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.ibge_client import IBGEIngestor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_connectivity():
    """Test basic IBGE SIDRA API connectivity without S3 upload."""
    logger.info("=" * 60)
    logger.info("BRONZE I - IBGE SIDRA API Connectivity Test")
    logger.info("=" * 60)
    
    config_path = Path(__file__).parent.parent / "config" / "ibge_metadata.json"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info(f"✓ Loaded metadata: {len(config['datasets'])} datasets configured")
    
    import requests
    
    # Test with first dataset (pop_2010)
    test_dataset = config['datasets'][0]
    base_url = config['api_base_url']
    
    table = test_dataset['table_id']
    var = test_dataset.get('variable', 'allxp')
    period = str(test_dataset['period']).replace(" ", "%20")
    classif = test_dataset.get('classifications', '')
    
    # Use single municipality probe (Alta Floresta D'Oeste - RO)
    url = f"{base_url}/t/{table}/n6/1100015/v/{var}/p/{period}"
    if classif:
        url += f"/{classif}"
    url += "?formato=json"
    
    logger.info(f"\n🧪 Testing endpoint: {test_dataset['name']}")
    logger.info(f"   URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        logger.info(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 1:
                logger.info(f"   ✅ SUCCESS: Received {len(data)} records")
                logger.info(f"   Sample keys: {list(data[0].keys())[:5]}")
                return True
            else:
                logger.error(f"   ⚠️ WARNING: Empty result")
                return False
        else:
            logger.error(f"   ❌ FAILED: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ EXCEPTION: {e}")
        return False

def test_s3_access():
    """Test S3 bucket access."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE I - S3 Access Test")
    logger.info("=" * 60)
    
    bucket_name = "enok-mba-thesis-datalake"
    
    try:
        import boto3
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ S3 bucket '{bucket_name}' is accessible")
        
        # List existing Bronze I files
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='bronze/ibge/')
        if 'Contents' in response:
            logger.info(f"✓ Found {len(response['Contents'])} existing files in bronze/ibge/")
            for obj in response['Contents'][:3]:
                logger.info(f"   - {obj['Key']} ({obj['Size']} bytes)")
        else:
            logger.info("ℹ️  No existing files in bronze/ibge/ (first run)")
        
        return True
    except Exception as e:
        logger.error(f"❌ S3 access failed: {e}")
        logger.info("   Verify AWS credentials with: aws sts get-caller-identity")
        return False

def test_single_dataset_ingestion():
    """Test full ingestion flow for a single dataset."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE I - Single Dataset Ingestion Test")
    logger.info("=" * 60)
    
    bucket_name = "enok-mba-thesis-datalake"
    config_path = Path(__file__).parent.parent / "config" / "ibge_metadata.json"
    
    logger.info(f"Target S3 Bucket: {bucket_name}")
    
    ingestor = IBGEIngestor(bucket_name, config_path)
    
    # Test with first dataset (pop_2010 - smallest/fastest)
    test_dataset_index = 0
    test_ds = ingestor.config['datasets'][test_dataset_index]
    
    logger.info(f"\n🚀 Running ingestion for: {test_ds['name']}")
    logger.info(f"   This is a REAL ingestion that will upload to S3")
    logger.info(f"   Expected S3 path: s3://{bucket_name}/bronze/ibge/{test_ds['filename']}")
    
    # Temporarily limit to single dataset
    original_datasets = ingestor.config['datasets']
    ingestor.config['datasets'] = [test_ds]
    
    try:
        ingestor.run_full_ingestion()
        logger.info("\n✅ TEST INGESTION COMPLETED SUCCESSFULLY")
        logger.info(f"   Check S3: aws s3 ls s3://{bucket_name}/bronze/ibge/")
        return True
    except Exception as e:
        logger.error(f"\n❌ TEST INGESTION FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        ingestor.config['datasets'] = original_datasets

def test_all_endpoints():
    """Test all 8 IBGE endpoints with probe queries."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE I - All Endpoints Validation")
    logger.info("=" * 60)
    
    config_path = Path(__file__).parent.parent / "config" / "ibge_metadata.json"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    base_url = config['api_base_url']
    results = []
    
    import requests
    
    logger.info(f"\n{'Dataset':<30} | {'Status':<10} | {'Details'}")
    logger.info("-" * 70)
    
    for ds in config['datasets']:
        name = ds['name']
        table = ds['table_id']
        var = ds.get('variable', 'allxp')
        period = str(ds['period']).replace(" ", "%20")
        classif = ds.get('classifications', '')
        
        # Probe with single municipality
        url = f"{base_url}/t/{table}/n6/1100015/v/{var}/p/{period}"
        if classif:
            url += f"/{classif}"
        url += "?formato=json"
        
        try:
            response = requests.get(url, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 1:
                    status = "✅ PASS"
                    details = f"{len(data)} records"
                else:
                    status = "⚠️ WARN"
                    details = "Empty result"
            else:
                status = "❌ FAIL"
                details = f"HTTP {response.status_code}"
        except Exception as e:
            status = "❌ FAIL"
            details = str(e)[:30]
        
        logger.info(f"{name:<30} | {status:<10} | {details}")
        results.append((name, status == "✅ PASS"))
    
    passed = sum(1 for _, success in results if success)
    logger.info("-" * 70)
    logger.info(f"Results: {passed}/{len(results)} endpoints passed")
    
    return passed == len(results)

def main():
    """Run all tests in sequence."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE I VALIDATION TEST SUITE")
    logger.info("=" * 60)
    logger.info("This script validates:")
    logger.info("  1. IBGE SIDRA API connectivity")
    logger.info("  2. AWS S3 access")
    logger.info("  3. All 8 endpoint configurations")
    logger.info("  4. Full ingestion pipeline (single dataset)")
    logger.info("=" * 60 + "\n")
    
    results = {}
    
    # Test 1: API Connectivity
    results['api_connectivity'] = test_api_connectivity()
    
    # Test 2: S3 Access
    results['s3_access'] = test_s3_access()
    
    # Test 3: All Endpoints
    if results['api_connectivity']:
        results['all_endpoints'] = test_all_endpoints()
    else:
        logger.warning("Skipping endpoint validation due to connectivity failure.")
        results['all_endpoints'] = None
    
    # Test 4: Single Dataset Ingestion
    if results['api_connectivity'] and results['s3_access']:
        user_input = input("\n⚠️  Proceed with S3 ingestion test? This will upload data. (yes/no): ")
        if user_input.lower() in ['yes', 'y']:
            results['ingestion'] = test_single_dataset_ingestion()
        else:
            logger.info("Skipping S3 ingestion test.")
            results['ingestion'] = None
    else:
        logger.warning("Skipping ingestion test due to previous failures.")
        results['ingestion'] = None
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"API Connectivity:  {'✅ PASS' if results['api_connectivity'] else '❌ FAIL'}")
    logger.info(f"S3 Access:         {'✅ PASS' if results['s3_access'] else '❌ FAIL'}")
    if results['all_endpoints'] is not None:
        logger.info(f"All Endpoints:     {'✅ PASS' if results['all_endpoints'] else '❌ FAIL'}")
    else:
        logger.info(f"All Endpoints:     ⏭️  SKIPPED")
    if results['ingestion'] is not None:
        logger.info(f"S3 Ingestion:      {'✅ PASS' if results['ingestion'] else '❌ FAIL'}")
    else:
        logger.info(f"S3 Ingestion:      ⏭️  SKIPPED")
    logger.info("=" * 60)
    
    all_passed = all(v for v in results.values() if v is not None)
    
    if all_passed:
        logger.info("\n🎉 All tests passed! Ready for full Bronze I ingestion.")
        logger.info("   Run: python src\\ingestion\\ibge_client.py")
    else:
        logger.error("\n⚠️  Some tests failed. Review errors above before proceeding.")

if __name__ == "__main__":
    main()
