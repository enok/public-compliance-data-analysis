import unittest
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys
from botocore.exceptions import ClientError

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ingestion.ibge_client import IBGEIngestor


class TestIBGEIngestor(unittest.TestCase):
    """Unit tests for IBGEIngestor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary config file
        self.temp_dir = tempfile.mkdtemp()
        self.config_data = {
            "api_base_url": "https://apisidra.ibge.gov.br/values",
            "datasets": [
                {
                    "name": "test_pop_2010",
                    "table_id": "1378",
                    "period": "all",
                    "variable": "allxp",
                    "classifications": "c1/0",
                    "filename": "test_pop.json"
                },
                {
                    "name": "test_pop_2022",
                    "table_id": "4714",
                    "period": "last 1",
                    "variable": "93",
                    "classifications": "",
                    "filename": "test_pop_2022.json"
                }
            ]
        }
        
        self.config_path = os.path.join(self.temp_dir, "test_config.json")
        with open(self.config_path, 'w') as f:
            json.dump(self.config_data, f)
        
        # Mock S3 client
        self.mock_s3 = MagicMock()
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_init(self, mock_boto3):
        """Test IBGEIngestor initialization."""
        mock_boto3.return_value = self.mock_s3
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        self.assertEqual(ingestor.bucket, "test-bucket")
        self.assertEqual(ingestor.config, self.config_data)
        mock_boto3.assert_called_once_with('s3')
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_load_config(self, mock_boto3):
        """Test configuration loading."""
        mock_boto3.return_value = self.mock_s3
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        self.assertIn("api_base_url", ingestor.config)
        self.assertIn("datasets", ingestor.config)
        self.assertEqual(len(ingestor.config["datasets"]), 2)
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_file_is_valid_exists_and_matches(self, mock_boto3):
        """Test file validation when file exists and MD5 matches."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock S3 head_object to return matching ETag
        self.mock_s3.head_object.return_value = {
            'ETag': '"abc123"'
        }
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor._file_is_valid("test/key.json", "abc123")
        
        self.assertTrue(result)
        self.mock_s3.head_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="test/key.json"
        )
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_file_is_valid_exists_but_different(self, mock_boto3):
        """Test file validation when file exists but MD5 differs."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock S3 head_object to return different ETag
        self.mock_s3.head_object.return_value = {
            'ETag': '"different123"'
        }
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor._file_is_valid("test/key.json", "abc123")
        
        self.assertFalse(result)
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_file_is_valid_not_exists(self, mock_boto3):
        """Test file validation when file doesn't exist."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock S3 head_object to raise ClientError
        from botocore.exceptions import ClientError
        self.mock_s3.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor._file_is_valid("test/key.json", "abc123")
        
        self.assertFalse(result)
    
    @patch('src.ingestion.ibge_client.boto3.client')
    @patch('src.ingestion.http_client.requests.get')
    def test_fetch_with_retry_success(self, mock_get, mock_boto3):
        """Test successful API fetch."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"data": "test"}'
        mock_response.headers = {'Content-Type': 'application/json'}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor.fetch_with_retry("http://test.url")
        
        self.assertEqual(result, '{"data": "test"}')
        mock_get.assert_called_once()
    
    @patch('src.ingestion.ibge_client.boto3.client')
    @patch('src.ingestion.http_client.requests.get')
    @patch('src.ingestion.http_client.time.sleep')
    def test_fetch_with_retry_eventual_success(self, mock_sleep, mock_get, mock_boto3):
        """Test API fetch with retry that eventually succeeds."""
        mock_boto3.return_value = self.mock_s3
        
        # First call fails, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.headers = {}
        mock_response_fail.raise_for_status.side_effect = Exception("Timeout")
        
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.text = '{"data": "test"}'
        mock_response_success.headers = {'Content-Type': 'application/json'}
        mock_response_success.raise_for_status = Mock()
        
        mock_get.side_effect = [mock_response_fail, mock_response_success]
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor.fetch_with_retry("http://test.url")
        
        self.assertEqual(result, '{"data": "test"}')
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()  # Should sleep before retry
    
    @patch('src.ingestion.ibge_client.boto3.client')
    @patch('src.ingestion.http_client.requests.get')
    @patch('src.ingestion.http_client.time.sleep')
    def test_fetch_with_retry_max_retries(self, mock_sleep, mock_get, mock_boto3):
        """Test API fetch exhausts max retries."""
        mock_boto3.return_value = self.mock_s3
        
        # All calls fail
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.headers = {}
        mock_response.raise_for_status.side_effect = Exception("Timeout")
        mock_get.return_value = mock_response
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        result = ingestor.fetch_with_retry("http://test.url")
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 10)  # Now uses HTTPClient's default of 10
    
    @patch('src.ingestion.ibge_client.boto3.client')
    def test_log_source(self, mock_boto3):
        """Test source logging functionality."""
        mock_boto3.return_value = self.mock_s3
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        # Log a source
        ingestor.log_source("test_dataset", "http://test.url")
        
        # Verify log file was created and contains entry
        self.assertTrue(ingestor.source_log.exists())
        
        with open(ingestor.source_log, 'r') as f:
            content = f.read()
            self.assertIn("test_dataset", content)
            self.assertIn("http://test.url", content)
    
    @patch('src.ingestion.ibge_client.boto3.client')
    @patch('src.ingestion.http_client.requests.get')
    def test_run_full_ingestion_skips_existing(self, mock_get, mock_boto3):
        """Test that ingestion skips files that already exist with matching MD5."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        test_content = '[{"header": "test"}, {"data": "value"}]'
        mock_response.text = test_content
        mock_response.headers = {'Content-Type': 'application/json'}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        
        # Calculate expected MD5
        import hashlib
        expected_md5 = hashlib.md5(test_content.encode('utf-8')).hexdigest()
        
        # Mock S3 to return matching ETag
        self.mock_s3.head_object.return_value = {
            'ETag': f'"{expected_md5}"'
        }
        
        ingestor.run_full_ingestion()
        
        # put_object should NOT be called since files already exist
        self.mock_s3.put_object.assert_not_called()
    
    @patch('src.ingestion.ibge_client.boto3.client')
    @patch('src.ingestion.http_client.requests.get')
    def test_run_full_ingestion_invalid_json(self, mock_get, mock_boto3):
        """Test handling of invalid JSON response."""
        mock_boto3.return_value = self.mock_s3
        
        # Mock invalid JSON response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "Not valid JSON"
        mock_response.headers = {'Content-Type': 'text/plain'}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        ingestor = IBGEIngestor("test-bucket", self.config_path)
        ingestor.run_full_ingestion()
        
        # Should not attempt S3 upload for invalid JSON
        self.mock_s3.put_object.assert_not_called()


class TestIBGEIngestorIntegration(unittest.TestCase):
    """Integration tests for IBGEIngestor (require network/AWS)."""
    
    @unittest.skip("Integration test - requires network access")
    def test_real_api_call(self):
        """Test actual API call to IBGE SIDRA (skipped by default)."""
        import requests
        
        # Test with single municipality
        url = "https://apisidra.ibge.gov.br/values/t/1378/n6/1100015/v/allxp/p/all?formato=json"
        response = requests.get(url, timeout=30)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)


if __name__ == '__main__':
    unittest.main()