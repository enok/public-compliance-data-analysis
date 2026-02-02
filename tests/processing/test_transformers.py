"""
Unit tests for Silver layer transformers.
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


class TestBaseTransformer:
    """Tests for BaseTransformer utility methods."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "test_schema": {
                    "columns": {
                        "id": {"type": "string"},
                        "value": {"type": "integer"},
                        "rate": {"type": "float", "nullable": True}
                    }
                }
            },
            "state_mapping": {
                "11": "Rondônia",
                "35": "São Paulo",
                "33": "Rio de Janeiro"
            },
            "region_mapping": {
                "1": {"name": "Norte", "states": ["11"]},
                "3": {"name": "Sudeste", "states": ["33", "35"]}
            }
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
        
        with patch('boto3.client'):
            return ConcreteTransformer("test-bucket", mock_schema_config)

    def test_extract_municipality_code_valid(self, transformer):
        """Test valid municipality code extraction."""
        assert transformer._extract_municipality_code("3550308") == "3550308"  # São Paulo
        assert transformer._extract_municipality_code("1100015") == "1100015"  # Rondônia
        assert transformer._extract_municipality_code(3304557) == "3304557"  # Rio de Janeiro

    def test_extract_municipality_code_invalid(self, transformer):
        """Test invalid municipality code handling."""
        assert transformer._extract_municipality_code("123") is None  # Too short
        assert transformer._extract_municipality_code("12345678") is None  # Too long
        assert transformer._extract_municipality_code("9900001") is None  # Invalid state
        assert transformer._extract_municipality_code(None) is None
        assert transformer._extract_municipality_code("") is None

    def test_extract_municipality_code_with_decimal(self, transformer):
        """Test municipality code with decimal."""
        assert transformer._extract_municipality_code("3550308.0") == "3550308"

    def test_extract_state_code(self, transformer):
        """Test state code extraction from municipality code."""
        assert transformer._extract_state_code("3550308") == "35"
        assert transformer._extract_state_code("1100015") == "11"

    def test_get_state_name(self, transformer):
        """Test state name lookup."""
        assert transformer._get_state_name("35") == "São Paulo"
        assert transformer._get_state_name("11") == "Rondônia"
        assert transformer._get_state_name("99") == "Unknown"

    def test_get_region_code(self, transformer):
        """Test region code from state code."""
        assert transformer._get_region_code("35") == "3"  # São Paulo -> Sudeste
        assert transformer._get_region_code("11") == "1"  # Rondônia -> Norte

    def test_safe_int(self, transformer):
        """Test safe integer conversion."""
        assert transformer._safe_int("123") == 123
        assert transformer._safe_int(456) == 456
        assert transformer._safe_int("1,234") == 1234
        assert transformer._safe_int("") is None
        assert transformer._safe_int("-") is None
        assert transformer._safe_int(None) is None
        assert transformer._safe_int("abc") is None

    def test_safe_float(self, transformer):
        """Test safe float conversion."""
        assert transformer._safe_float("123.45") == 123.45
        assert transformer._safe_float(67.89) == 67.89
        assert transformer._safe_float("123,45") == 123.45  # Brazilian format
        assert transformer._safe_float("") is None
        assert transformer._safe_float("-") is None
        assert transformer._safe_float(None) is None

    def test_parse_date(self, transformer):
        """Test date parsing."""
        from datetime import datetime
        
        result = transformer._parse_date("2022-01-15")
        assert result == datetime(2022, 1, 15)
        
        result = transformer._parse_date("15/01/2022")
        assert result == datetime(2022, 1, 15)
        
        assert transformer._parse_date("") is None
        assert transformer._parse_date(None) is None
        assert transformer._parse_date("invalid") is None

    def test_validate_schema(self, transformer):
        """Test schema validation and enforcement."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C'],
            'value': ['1', '2', '3'],
            'extra_col': [1, 2, 3]
        })
        
        result = transformer.validate_schema(df, 'test_schema')
        
        # Should only have defined columns
        assert list(result.columns) == ['id', 'value', 'rate']
        # Should have converted types
        assert result['value'].dtype == 'Int64'
        # Nullable column should exist with nulls
        assert result['rate'].isna().all()


class TestIBGETransformer:
    """Tests for IBGETransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "municipalities": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "municipality_name": {"type": "string"},
                        "state_code": {"type": "string"},
                        "state_name": {"type": "string"},
                        "region_code": {"type": "string"},
                        "region_name": {"type": "string"}
                    }
                },
                "census_population": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "year": {"type": "integer"},
                        "total_population": {"type": "integer"},
                        "urban_population": {"type": "integer", "nullable": True},
                        "rural_population": {"type": "integer", "nullable": True}
                    }
                }
            },
            "state_mapping": {
                "11": "Rondônia",
                "35": "São Paulo"
            },
            "region_mapping": {
                "1": {"name": "Norte", "states": ["11"]},
                "3": {"name": "Sudeste", "states": ["35"]}
            }
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    def test_bronze_files_defined(self, mock_schema_config):
        """Test that all expected Bronze files are defined."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        expected_files = [
            'pop_2010', 'pop_2022',
            'sanitation_2010', 'sanitation_2022',
            'literacy_2010', 'literacy_2022',
            'income_2010', 'income_2022'
        ]
        
        for file_key in expected_files:
            assert file_key in transformer.BRONZE_FILES

    def test_get_source_datasets(self, mock_schema_config):
        """Test source datasets list."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        datasets = transformer.get_source_datasets()
        assert len(datasets) == 8
        assert 'pop_2010' in datasets
        assert 'income_2022' in datasets


class TestTransparencyTransformer:
    """Tests for TransparencyTransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "compliance_sanctions": {
                    "columns": {
                        "sanction_id": {"type": "string"},
                        "registry_type": {"type": "string"},
                        "sanctioned_entity": {"type": "string"},
                        "entity_type": {"type": "string"},
                        "cpf_cnpj": {"type": "string"},
                        "sanction_type": {"type": "string", "nullable": True}
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

    def test_mask_document_cpf(self, mock_schema_config):
        """Test CPF masking."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        # Valid CPF - shows last 5 digits (indices 6-10)
        result = transformer._mask_document("12345678901", "CPF")
        assert result == "***.***789-01"
        
        # With formatting
        result = transformer._mask_document("123.456.789-01", "CPF")
        assert result == "***.***789-01"

    def test_mask_document_cnpj(self, mock_schema_config):
        """Test CNPJ masking."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        # Valid CNPJ - shows last 6 digits (indices 8-13)
        result = transformer._mask_document("12345678000199", "CNPJ")
        assert result == "**.***.***/0001-99"

    def test_determine_entity_type(self, mock_schema_config):
        """Test entity type determination."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        assert transformer._determine_entity_type("12345678901") == "PF"
        assert transformer._determine_entity_type("12345678000199") == "PJ"
        assert transformer._determine_entity_type("123") == "UNKNOWN"
        assert transformer._determine_entity_type(None) == "UNKNOWN"

    def test_uf_to_state_code(self, mock_schema_config):
        """Test UF to state code conversion."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        assert transformer._uf_to_state_code("SP") == "35"
        assert transformer._uf_to_state_code("RJ") == "33"
        assert transformer._uf_to_state_code("sp") == "35"  # lowercase
        assert transformer._uf_to_state_code("XX") is None  # invalid


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
