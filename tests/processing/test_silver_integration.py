"""
Integration tests for Silver layer transformers.

Tests end-to-end transformation logic with realistic data samples.
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime

import pandas as pd
from botocore.exceptions import ClientError


class TestIBGETransformerIntegration:
    """Integration tests for IBGETransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a complete schema config."""
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
                    },
                    "primary_key": "municipality_code"
                },
                "census_population": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "year": {"type": "integer"},
                        "total_population": {"type": "integer"},
                        "urban_population": {"type": "integer", "nullable": True},
                        "rural_population": {"type": "integer", "nullable": True}
                    },
                    "primary_key": ["municipality_code", "year"]
                },
                "census_sanitation": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "year": {"type": "integer"},
                        "total_households": {"type": "integer"},
                        "households_with_water": {"type": "integer", "nullable": True},
                        "households_with_sewage": {"type": "integer", "nullable": True},
                        "households_with_garbage_collection": {"type": "integer", "nullable": True},
                        "water_coverage_pct": {"type": "float", "nullable": True},
                        "sewage_coverage_pct": {"type": "float", "nullable": True}
                    },
                    "primary_key": ["municipality_code", "year"]
                },
                "census_literacy": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "year": {"type": "integer"},
                        "population_15_plus": {"type": "integer", "nullable": True},
                        "literate_population": {"type": "integer", "nullable": True},
                        "literacy_rate": {"type": "float", "nullable": True}
                    },
                    "primary_key": ["municipality_code", "year"]
                },
                "census_income": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "year": {"type": "integer"},
                        "avg_income": {"type": "float"},
                        "median_income": {"type": "float", "nullable": True},
                        "population_with_income": {"type": "integer", "nullable": True}
                    },
                    "primary_key": ["municipality_code", "year"]
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
    def sample_ibge_data(self):
        """Create sample IBGE SIDRA API response."""
        return [
            {
                "D1C": "3550308",
                "D1N": "São Paulo",
                "D2C": "93",
                "D2N": "População residente",
                "D3C": "2010",
                "D3N": "2010",
                "V": "11253503",
                "MN": "Pessoas"
            },
            {
                "D1C": "3304557",
                "D1N": "Rio de Janeiro",
                "D2C": "93",
                "D2N": "População residente",
                "D3C": "2010",
                "D3N": "2010",
                "V": "6320446",
                "MN": "Pessoas"
            },
            {
                "D1C": "1100015",
                "D1N": "Alta Floresta d'Oeste",
                "D2C": "93",
                "D2N": "População residente",
                "D3C": "2010",
                "D3N": "2010",
                "V": "22945",
                "MN": "Pessoas"
            }
        ]

    def test_parse_ibge_json(self, mock_schema_config, sample_ibge_data):
        """Test parsing IBGE SIDRA JSON format."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        df = transformer._parse_ibge_json(sample_ibge_data, 2010)
        
        assert len(df) == 2
        assert 'year' in df.columns
        assert df['year'].iloc[0] == 2010

    def test_extract_sidra_data(self, mock_schema_config, sample_ibge_data):
        """Test extracting data from SIDRA row."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        row = sample_ibge_data[0]
        parsed = transformer._extract_sidra_data(row)
        
        assert parsed is not None
        assert parsed['municipality_code'] == "3550308"
        assert parsed['municipality_name'] == "São Paulo"
        assert parsed['value'] == "11253503"

    def test_extract_sidra_data_invalid_code(self, mock_schema_config):
        """Test handling invalid municipality code."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        row = {
            "D1C": "999",
            "D1N": "Invalid",
            "V": "123"
        }
        
        parsed = transformer._extract_sidra_data(row)
        assert parsed is None

    def test_municipalities_extraction_logic(self, mock_schema_config, sample_ibge_data):
        """Test that municipalities are extracted correctly from SIDRA data."""
        from src.processing.ibge_transformer import IBGETransformer
        
        with patch('boto3.client'):
            transformer = IBGETransformer("test-bucket", mock_schema_config)
        
        municipalities = {}
        
        for row in sample_ibge_data[1:]:
            parsed = transformer._extract_sidra_data(row)
            if parsed and parsed['municipality_code'] not in municipalities:
                muni_code = parsed['municipality_code']
                muni_name = parsed['municipality_name']
                state_code = transformer._extract_state_code(muni_code)
                region_code = transformer._get_region_code(state_code)
                
                municipalities[muni_code] = {
                    'municipality_code': muni_code,
                    'municipality_name': muni_name,
                    'state_code': state_code,
                    'state_name': transformer._get_state_name(state_code),
                    'region_code': region_code,
                    'region_name': transformer._get_region_name(region_code)
                }
        
        assert len(municipalities) == 2
        assert '3304557' in municipalities
        assert '1100015' in municipalities
        assert municipalities['3304557']['state_code'] == '33'
        assert municipalities['1100015']['state_code'] == '11'


class TestTransparencyTransformerIntegration:
    """Integration tests for TransparencyTransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a complete schema config."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "federal_transfers": {
                    "columns": {
                        "municipality_code": {"type": "string", "nullable": True},
                        "year": {"type": "integer"},
                        "month": {"type": "integer"},
                        "transfer_amount": {"type": "float"},
                        "transfer_type": {"type": "string"},
                        "source_agency": {"type": "string"}
                    },
                    "primary_key": ["year", "month", "transfer_type", "transfer_amount"]
                },
                "compliance_sanctions": {
                    "columns": {
                        "sanction_id": {"type": "string"},
                        "registry_type": {"type": "string"},
                        "sanctioned_entity": {"type": "string"},
                        "entity_type": {"type": "string"},
                        "cpf_cnpj": {"type": "string"},
                        "sanction_type": {"type": "string", "nullable": True},
                        "sanction_start_date": {"type": "date", "nullable": True},
                        "sanction_end_date": {"type": "date", "nullable": True},
                        "sanctioning_agency": {"type": "string", "nullable": True},
                        "state_code": {"type": "string", "nullable": True},
                        "municipality_code": {"type": "string", "nullable": True}
                    },
                    "primary_key": ["sanction_id", "registry_type"]
                }
            },
            "state_mapping": {"35": "São Paulo", "33": "Rio de Janeiro"},
            "region_mapping": {"3": {"name": "Sudeste", "states": ["33", "35"]}}
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    @pytest.fixture
    def sample_transfer_data(self):
        """Create sample federal transfer data."""
        return [
            {
                "valor": 100000.50,
                "tipoTransferencia": "Convênio",
                "orgaoSuperior": {"nome": "Ministério da Saúde"},
                "municipio": {"codigoIBGE": "3550308"}
            },
            {
                "valorRecebido": 50000.00,
                "tipo": "Emenda Parlamentar",
                "unidadeGestora": "Ministério da Educação"
            }
        ]

    @pytest.fixture
    def sample_sanctions_data(self):
        """Create sample sanctions data."""
        return [
            {
                "sancionado": "Empresa XYZ Ltda",
                "cpfCnpjSancionado": "12345678000199",
                "tipoSancao": "Impedimento de licitar",
                "dataInicioSancao": "2020-01-15",
                "dataFimSancao": "2025-01-15",
                "orgaoSancionador": "CGU",
                "ufSancionado": "SP"
            },
            {
                "sancionado": "João da Silva",
                "cpfCnpjSancionado": "12345678901",
                "tipoSancao": "Inidoneidade",
                "dataInicioSancao": "2021-06-01",
                "orgaoSancionador": "TCU"
            }
        ]

    def test_mask_cpf(self, mock_schema_config):
        """Test CPF masking."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        result = transformer._mask_document("12345678901", "CPF")
        assert result == "***.***789-01"
        assert "123" not in result
        assert "789-01" in result

    def test_mask_cnpj(self, mock_schema_config):
        """Test CNPJ masking."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        result = transformer._mask_document("12345678000199", "CNPJ")
        assert result == "**.***.***/0001-99"
        assert "12345678" not in result
        assert "0001-99" in result

    def test_determine_entity_type(self, mock_schema_config):
        """Test entity type determination."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        assert transformer._determine_entity_type("12345678901") == "PF"
        assert transformer._determine_entity_type("12345678000199") == "PJ"
        assert transformer._determine_entity_type("123") == "UNKNOWN"

    def test_uf_to_state_code(self, mock_schema_config):
        """Test UF to state code conversion."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        assert transformer._uf_to_state_code("SP") == "35"
        assert transformer._uf_to_state_code("sp") == "35"
        assert transformer._uf_to_state_code("RJ") == "33"
        assert transformer._uf_to_state_code("XX") is None

    def test_extract_nested_value(self, mock_schema_config):
        """Test nested value extraction."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client'):
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
        
        record = {
            "orgao": {
                "nome": "Ministério da Saúde",
                "sigla": "MS"
            },
            "valor": 1000
        }
        
        assert transformer._extract_nested_value(record, "orgao.nome") == "Ministério da Saúde"
        assert transformer._extract_nested_value(record, "orgao.sigla") == "MS"
        assert transformer._extract_nested_value(record, "valor") == 1000
        assert transformer._extract_nested_value(record, "missing") is None

    def test_list_federal_transfer_files(self, mock_schema_config):
        """Test discovering federal transfer monthly files."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        with patch('boto3.client') as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            transformer = TransparencyTransformer("test-bucket", mock_schema_config)
            transformer.s3 = mock_s3
            
            mock_s3.list_objects_v2.return_value = {
                'Contents': [
                    {'Key': 'bronze/transparency/federal_transfers_2013_01.json'},
                    {'Key': 'bronze/transparency/federal_transfers_2013_02.json'},
                    {'Key': 'bronze/transparency/federal_transfers_2014_01.json'},
                    {'Key': 'bronze/transparency/ceis_compliance.json'},
                    {'Key': 'bronze/transparency/federal_transfers_2013_01.meta.json'}
                ]
            }
            
            files = transformer._list_federal_transfer_files()
            
            assert len(files) == 3
            assert 'bronze/transparency/federal_transfers_2013_01.json' in files
            assert 'bronze/transparency/federal_transfers_2013_02.json' in files
            assert 'bronze/transparency/federal_transfers_2014_01.json' in files
            assert 'bronze/transparency/ceis_compliance.json' not in files
            assert 'bronze/transparency/federal_transfers_2013_01.meta.json' not in files


class TestSmartCachingIntegration:
    """Integration tests for smart caching functionality."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a minimal schema config."""
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

    def test_should_skip_first_run(self, mock_schema_config):
        """Test that first run is not skipped."""
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
            
            mock_s3.head_object.side_effect = ClientError(
                {'Error': {'Code': '404'}}, 'HeadObject'
            )
            
            should_skip, reason = transformer._should_skip_processing(
                'silver/test/data.parquet',
                'silver/test/_metadata.json',
                ['bronze/test/file.json']
            )
            
            assert should_skip is False
            assert "output does not exist" in reason

    def test_should_skip_unchanged(self, mock_schema_config):
        """Test that unchanged data is skipped."""
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
            
            metadata = {
                'source_files': {'bronze/test/file.json': 'hash123'}
            }
            
            def head_object_side_effect(Bucket, Key):
                if Key == 'silver/test/data.parquet':
                    return {}
                elif Key == 'bronze/test/file.json':
                    return {'ETag': '"hash123"'}
            
            mock_s3.head_object.side_effect = head_object_side_effect
            mock_s3.get_object.return_value = {
                'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
            }
            
            should_skip, reason = transformer._should_skip_processing(
                'silver/test/data.parquet',
                'silver/test/_metadata.json',
                ['bronze/test/file.json']
            )
            
            assert should_skip is True
            assert "no source files changed" in reason

    def test_should_not_skip_changed(self, mock_schema_config):
        """Test that changed data is not skipped."""
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
            
            metadata = {
                'source_files': {'bronze/test/file.json': 'hash_old'}
            }
            
            def head_object_side_effect(Bucket, Key):
                if Key == 'silver/test/data.parquet':
                    return {}
                elif Key == 'bronze/test/file.json':
                    return {'Metadata': {'content-sha256': 'hash_new'}}
            
            mock_s3.head_object.side_effect = head_object_side_effect
            mock_s3.get_object.return_value = {
                'Body': MagicMock(read=lambda: json.dumps(metadata).encode('utf-8'))
            }
            
            should_skip, reason = transformer._should_skip_processing(
                'silver/test/data.parquet',
                'silver/test/_metadata.json',
                ['bronze/test/file.json']
            )
            
            assert should_skip is False
            assert "changed" in reason


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
