"""
Unit tests for smart caching functionality in Silver layer transformers.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

import pandas as pd


class TestSmartCaching:
    """Tests for smart caching and metadata tracking."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "test_schema": {
                    "columns": {
                        "id": {"type": "string"},
                        "value": {"type": "integer"}
                    }
                }
            },
            "state_mapping": {"35": "São Paulo"},
            "region_mapping": {"3": {"name": "Sudeste", "states": ["35"]}}
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    @pytest.fixture
    def transformer(self, mock_schema_config):
        """Create a concrete transformer for testing."""
        from src.processing.base_transformer import BaseTransformer
        
        class ConcreteTransformer(BaseTransformer):
            def transform(self):
                return True
            def get_source_datasets(self):
                return []
        
        with patch('boto3.client') as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            transformer = ConcreteTransformer("test-bucket", mock_schema_config)
            transformer.s3 = mock_s3
            return transformer

    def test_get_bronze_file_hash_exists(self, transformer):
        """Test getting MD5 hash from existing bronze file."""
        transformer.s3.head_object.return_value = {
            'ETag': '"abc123def456"'
        }
        
        result = transformer._get_bronze_file_hash('bronze/test/file.json')
        
        assert result == "abc123def456"
        transformer.s3.head_object.assert_called_once_with(
            Bucket="test-bucket",
            Key='bronze/test/file.json'
        )

    def test_get_bronze_file_hash_not_found(self, transformer):
        """Test getting hash for non-existent file."""
        transformer.s3.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        result = transformer._get_bronze_file_hash('bronze/test/missing.json')
        
        assert result is None

    def test_save_and_get_silver_metadata(self, transformer):
        """Test saving and retrieving silver metadata."""
        metadata = {
            'source_files': {
                'bronze/test/file1.json': 'hash1',
                'bronze/test/file2.json': 'hash2'
            },
            'last_updated': '2026-02-01 12:00:00',
            'record_count': 100
        }
        
        # Test save
        transformer._save_silver_metadata('silver/test/.metadata.json', metadata)
        
        transformer.s3.put_object.assert_called_once()
        call_args = transformer.s3.put_object.call_args
        assert call_args[1]['Bucket'] == 'test-bucket'
        assert call_args[1]['Key'] == 'silver/test/.metadata.json'
        assert call_args[1]['ContentType'] == 'application/json; charset=utf-8'
        
        # Verify saved content
        saved_content = json.loads(call_args[1]['Body'].decode('utf-8'))
        assert saved_content == metadata

    def test_get_silver_metadata_exists(self, transformer):
        """Test retrieving existing silver metadata."""
        metadata = {
            'source_files': {'bronze/test/file.json': 'hash1'},
            'last_updated': '2026-02-01 12:00:00',
            'record_count': 50
        }
        
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        result = transformer._get_silver_metadata('silver/test/.metadata.json')
        
        assert result == metadata

    def test_get_silver_metadata_not_found(self, transformer):
        """Test retrieving non-existent metadata."""
        transformer.s3.get_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'GetObject'
        )
        
        result = transformer._get_silver_metadata('silver/test/.metadata.json')
        
        assert result is None

    def test_check_sources_changed_no_metadata(self, transformer):
        """Test source change detection with no existing metadata (first run)."""
        transformer.s3.get_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'GetObject'
        )
        
        source_keys = ['bronze/test/file1.json', 'bronze/test/file2.json']
        
        has_changes, changed_files = transformer._check_sources_changed(
            'silver/test/.metadata.json',
            source_keys
        )
        
        assert has_changes is True
        assert changed_files == source_keys

    def test_check_sources_changed_no_changes(self, transformer):
        """Test source change detection when nothing changed."""
        metadata = {
            'source_files': {
                'bronze/test/file1.json': 'hash1',
                'bronze/test/file2.json': 'hash2'
            }
        }
        
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        # Mock head_object to return same hashes
        def head_object_side_effect(Bucket, Key):
            if Key == 'bronze/test/file1.json':
                return {'ETag': '"hash1"'}
            elif Key == 'bronze/test/file2.json':
                return {'ETag': '"hash2"'}
        
        transformer.s3.head_object.side_effect = head_object_side_effect
        
        source_keys = ['bronze/test/file1.json', 'bronze/test/file2.json']
        
        has_changes, changed_files = transformer._check_sources_changed(
            'silver/test/.metadata.json',
            source_keys
        )
        
        assert has_changes is False
        assert changed_files == []

    def test_check_sources_changed_file_modified(self, transformer):
        """Test source change detection when a file was modified."""
        metadata = {
            'source_files': {
                'bronze/test/file1.json': 'hash1_old',
                'bronze/test/file2.json': 'hash2'
            }
        }
        
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        # Mock head_object - file1 has new hash
        def head_object_side_effect(Bucket, Key):
            if Key == 'bronze/test/file1.json':
                return {'ETag': '"hash1_new"'}
            elif Key == 'bronze/test/file2.json':
                return {'ETag': '"hash2"'}
        
        transformer.s3.head_object.side_effect = head_object_side_effect
        
        source_keys = ['bronze/test/file1.json', 'bronze/test/file2.json']
        
        has_changes, changed_files = transformer._check_sources_changed(
            'silver/test/.metadata.json',
            source_keys
        )
        
        assert has_changes is True
        assert 'bronze/test/file1.json' in changed_files
        assert 'bronze/test/file2.json' not in changed_files

    def test_check_sources_changed_new_file(self, transformer):
        """Test source change detection when a new file appears."""
        metadata = {
            'source_files': {
                'bronze/test/file1.json': 'hash1'
            }
        }
        
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        # Mock head_object
        def head_object_side_effect(Bucket, Key):
            if Key == 'bronze/test/file1.json':
                return {'ETag': '"hash1"'}
            elif Key == 'bronze/test/file2.json':
                return {'ETag': '"hash2_new"'}
        
        transformer.s3.head_object.side_effect = head_object_side_effect
        
        source_keys = ['bronze/test/file1.json', 'bronze/test/file2.json']
        
        has_changes, changed_files = transformer._check_sources_changed(
            'silver/test/.metadata.json',
            source_keys
        )
        
        assert has_changes is True
        assert 'bronze/test/file2.json' in changed_files


class TestTransparencySmartCaching:
    """Tests for smart caching in TransparencyTransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {},
            "state_mapping": {"35": "São Paulo"},
            "region_mapping": {"3": {"name": "Sudeste", "states": ["35"]}}
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    @pytest.fixture
    def transformer(self, mock_schema_config):
        """Create TransparencyTransformer for testing."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client') as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
            transformer.s3 = mock_s3
            return transformer

    def test_should_skip_processing_no_output(self, transformer):
        """Test skip check when output doesn't exist."""
        transformer.s3.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        should_skip, reason = transformer._should_skip_processing(
            'silver/test/data.parquet',
            'silver/test/.metadata.json',
            ['bronze/test/file.json']
        )
        
        assert should_skip is False
        assert "output does not exist" in reason

    def test_should_skip_processing_no_changes(self, transformer):
        """Test skip check when output exists and no changes."""
        # Output exists
        transformer.s3.head_object.return_value = {}
        
        # Metadata exists with matching hashes
        metadata = {
            'source_files': {'bronze/test/file.json': 'hash1'}
        }
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        # Source file hash matches
        def head_object_side_effect(Bucket, Key):
            if Key == 'silver/test/data.parquet':
                return {}
            elif Key == 'bronze/test/file.json':
                return {'ETag': '"hash1"'}
        
        transformer.s3.head_object.side_effect = head_object_side_effect
        
        should_skip, reason = transformer._should_skip_processing(
            'silver/test/data.parquet',
            'silver/test/.metadata.json',
            ['bronze/test/file.json']
        )
        
        assert should_skip is True
        assert "no source files changed" in reason

    def test_should_skip_processing_with_changes(self, transformer):
        """Test skip check when output exists but sources changed."""
        # Output exists
        def head_object_side_effect(Bucket, Key):
            if Key == 'silver/test/data.parquet':
                return {}
            elif Key == 'bronze/test/file.json':
                return {'ETag': '"hash_new"'}
        
        transformer.s3.head_object.side_effect = head_object_side_effect
        
        # Metadata exists with old hash
        metadata = {
            'source_files': {'bronze/test/file.json': 'hash_old'}
        }
        transformer.s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
        }
        
        should_skip, reason = transformer._should_skip_processing(
            'silver/test/data.parquet',
            'silver/test/.metadata.json',
            ['bronze/test/file.json']
        )
        
        assert should_skip is False
        assert "changed" in reason


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
