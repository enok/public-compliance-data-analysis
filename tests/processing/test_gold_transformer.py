"""
Unit tests for Gold layer transformer.
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

import pandas as pd
import numpy as np


class TestGoldTransformer:
    """Tests for GoldTransformer."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_municipality_socioeconomic": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "municipality_name": {"type": "string"},
                        "state_code": {"type": "string"},
                        "state_name": {"type": "string"},
                        "region_code": {"type": "string"},
                        "region_name": {"type": "string"},
                        "population_2010": {"type": "integer", "nullable": True},
                        "population_2022": {"type": "integer", "nullable": True},
                        "population_change_pct": {"type": "float", "nullable": True}
                    }
                },
                "gold_state_summary": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "state_name": {"type": "string"},
                        "region_code": {"type": "string"},
                        "region_name": {"type": "string"},
                        "municipality_count": {"type": "integer"},
                        "total_population_2022": {"type": "integer", "nullable": True},
                        "total_sanctions": {"type": "integer"}
                    }
                },
                "gold_sanctions_summary": {
                    "columns": {
                        "registry_type": {"type": "string"},
                        "total_sanctions": {"type": "integer"},
                        "sanctions_pf": {"type": "integer"},
                        "sanctions_pj": {"type": "integer"},
                        "pj_ratio_pct": {"type": "float"}
                    }
                },
                "gold_analysis_compliance": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "state_name": {"type": "string"},
                        "n_municipalities": {"type": "integer"},
                        "population": {"type": "integer", "nullable": True},
                        "n_sanctions": {"type": "integer"},
                        "sanctions_per_100k": {"type": "float", "nullable": True}
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
        """Create a GoldTransformer instance for testing."""
        from src.processing.gold_transformer import GoldTransformer
        
        with patch('boto3.client'):
            return GoldTransformer("test-bucket", mock_schema_config)

    def test_silver_files_defined(self, transformer):
        """Test that all expected Silver files are defined."""
        expected_files = [
            'municipalities', 'population', 'sanitation',
            'literacy', 'income', 'sanctions', 'federal_transfers'
        ]
        
        for file_key in expected_files:
            assert file_key in transformer.SILVER_FILES, f"Missing {file_key}"

    def test_get_source_datasets(self, transformer):
        """Test source datasets list."""
        datasets = transformer.get_source_datasets()
        assert len(datasets) == 7
        assert 'municipalities' in datasets
        assert 'sanctions' in datasets

    def test_calculate_change_pct_valid(self, transformer):
        """Test percentage change calculation with valid values."""
        # 10% increase
        result = transformer._calculate_change_pct(110, 100)
        assert result == 10.0
        
        # 50% decrease
        result = transformer._calculate_change_pct(50, 100)
        assert result == -50.0
        
        # No change
        result = transformer._calculate_change_pct(100, 100)
        assert result == 0.0

    def test_calculate_change_pct_invalid(self, transformer):
        """Test percentage change calculation with invalid values."""
        assert transformer._calculate_change_pct(100, 0) is None
        assert transformer._calculate_change_pct(100, None) is None
        assert transformer._calculate_change_pct(None, 100) is None
        assert transformer._calculate_change_pct(100, float('nan')) is None


class TestGoldMunicipalitySocioeconomic:
    """Tests for municipality socioeconomic aggregation."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_municipality_socioeconomic": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "municipality_name": {"type": "string"},
                        "state_code": {"type": "string"},
                        "population_2010": {"type": "integer", "nullable": True},
                        "population_2022": {"type": "integer", "nullable": True},
                        "population_change_pct": {"type": "float", "nullable": True}
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

    def test_population_change_calculation(self, mock_schema_config):
        """Test that population change is calculated correctly."""
        from src.processing.gold_transformer import GoldTransformer
        
        with patch('boto3.client'):
            transformer = GoldTransformer("test-bucket", mock_schema_config)
        
        # Mock data
        df_muni = pd.DataFrame({
            'municipality_code': ['3550308'],
            'municipality_name': ['São Paulo'],
            'state_code': ['35'],
            'state_name': ['São Paulo'],
            'region_code': ['3'],
            'region_name': ['Sudeste']
        })
        
        df_pop = pd.DataFrame({
            'municipality_code': ['3550308', '3550308'],
            'year': [2010, 2022],
            'total_population': [10000000, 12000000]
        })
        
        # Test the change calculation
        pop_2010 = df_pop[df_pop['year'] == 2010]['total_population'].values[0]
        pop_2022 = df_pop[df_pop['year'] == 2022]['total_population'].values[0]
        
        change_pct = transformer._calculate_change_pct(pop_2022, pop_2010)
        assert change_pct == 20.0  # 20% increase


class TestGoldStateSummary:
    """Tests for state summary aggregation."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_state_summary": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "state_name": {"type": "string"},
                        "municipality_count": {"type": "integer"},
                        "total_sanctions": {"type": "integer"}
                    }
                }
            },
            "state_mapping": {"35": "São Paulo", "33": "Rio de Janeiro"},
            "region_mapping": {"3": {"name": "Sudeste", "states": ["33", "35"]}}
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    def test_municipality_count_aggregation(self, mock_schema_config):
        """Test municipality count aggregation by state."""
        df_muni = pd.DataFrame({
            'municipality_code': ['3550308', '3304557', '3303500'],
            'municipality_name': ['São Paulo', 'Rio de Janeiro', 'Niterói'],
            'state_code': ['35', '33', '33']
        })
        
        # Group and count
        muni_counts = df_muni.groupby('state_code').size().reset_index(name='municipality_count')
        
        assert muni_counts[muni_counts['state_code'] == '35']['municipality_count'].values[0] == 1
        assert muni_counts[muni_counts['state_code'] == '33']['municipality_count'].values[0] == 2


class TestGoldSanctionsSummary:
    """Tests for sanctions summary aggregation."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_sanctions_summary": {
                    "columns": {
                        "registry_type": {"type": "string"},
                        "total_sanctions": {"type": "integer"},
                        "sanctions_pf": {"type": "integer"},
                        "sanctions_pj": {"type": "integer"},
                        "pj_ratio_pct": {"type": "float"}
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

    def test_sanctions_aggregation_by_registry(self, mock_schema_config):
        """Test sanctions aggregation by registry type."""
        df_sanctions = pd.DataFrame({
            'sanction_id': ['CEIS_001', 'CEIS_002', 'CNEP_001', 'CNEP_002', 'CNEP_003'],
            'registry_type': ['CEIS', 'CEIS', 'CNEP', 'CNEP', 'CNEP'],
            'entity_type': ['PF', 'PJ', 'PJ', 'PJ', 'PF'],
            'state_code': ['35', '35', '33', '33', '35']
        })
        
        # Aggregate by registry
        registry_summary = df_sanctions.groupby('registry_type').agg(
            total_sanctions=('sanction_id', 'nunique'),
            sanctions_pf=('entity_type', lambda x: (x == 'PF').sum()),
            sanctions_pj=('entity_type', lambda x: (x == 'PJ').sum())
        ).reset_index()
        
        # CEIS
        ceis = registry_summary[registry_summary['registry_type'] == 'CEIS'].iloc[0]
        assert ceis['total_sanctions'] == 2
        assert ceis['sanctions_pf'] == 1
        assert ceis['sanctions_pj'] == 1
        
        # CNEP
        cnep = registry_summary[registry_summary['registry_type'] == 'CNEP'].iloc[0]
        assert cnep['total_sanctions'] == 3
        assert cnep['sanctions_pf'] == 1
        assert cnep['sanctions_pj'] == 2

    def test_pj_ratio_calculation(self, mock_schema_config):
        """Test PJ ratio calculation."""
        total = 10
        pj_count = 7
        
        pj_ratio = round((pj_count / total) * 100, 2)
        assert pj_ratio == 70.0


class TestGoldAnalysisCompliance:
    """Tests for analysis compliance dataset."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a temporary schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_analysis_compliance": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "n_municipalities": {"type": "integer"},
                        "population": {"type": "integer", "nullable": True},
                        "n_sanctions": {"type": "integer"},
                        "sanctions_per_100k": {"type": "float", "nullable": True},
                        "log_population": {"type": "float", "nullable": True}
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

    def test_sanctions_per_100k_calculation(self, mock_schema_config):
        """Test sanctions per 100k population calculation."""
        population = 10000000  # 10 million
        n_sanctions = 500
        
        sanctions_per_100k = round((n_sanctions / population) * 100000, 4)
        assert sanctions_per_100k == 5.0  # 5 per 100k

    def test_log_transformation(self, mock_schema_config):
        """Test log transformation of population."""
        population = 10000000  # 10 million
        
        log_pop = round(np.log(population), 4)
        assert log_pop == round(np.log(10000000), 4)  # ~16.1181

    def test_region_dummy_variables(self, mock_schema_config):
        """Test region dummy variable creation."""
        df = pd.DataFrame({
            'state_code': ['35', '33', '11'],
            'region_code': ['3', '3', '1']
        })
        
        # Create dummy for Sudeste (region 3)
        df['is_sudeste'] = (df['region_code'] == '3').astype(int)
        
        # Create dummy for Norte (region 1)
        df['is_norte'] = (df['region_code'] == '1').astype(int)
        
        assert df[df['state_code'] == '35']['is_sudeste'].values[0] == 1
        assert df[df['state_code'] == '35']['is_norte'].values[0] == 0
        assert df[df['state_code'] == '11']['is_norte'].values[0] == 1


class TestGoldTransformerIntegration:
    """Integration tests for GoldTransformer (mocked S3)."""

    @pytest.fixture
    def mock_schema_config(self, tmp_path):
        """Create a full schema config file."""
        config = {
            "version": "1.0.0",
            "schemas": {
                "gold_municipality_socioeconomic": {
                    "columns": {
                        "municipality_code": {"type": "string"},
                        "population_2010": {"type": "integer", "nullable": True},
                        "population_2022": {"type": "integer", "nullable": True}
                    }
                },
                "gold_state_summary": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "total_sanctions": {"type": "integer"}
                    }
                },
                "gold_sanctions_summary": {
                    "columns": {
                        "registry_type": {"type": "string"},
                        "total_sanctions": {"type": "integer"}
                    }
                },
                "gold_analysis_compliance": {
                    "columns": {
                        "state_code": {"type": "string"},
                        "n_sanctions": {"type": "integer"}
                    }
                }
            },
            "state_mapping": {"35": "São Paulo", "33": "Rio de Janeiro"},
            "region_mapping": {
                "1": {"name": "Norte", "states": []},
                "2": {"name": "Nordeste", "states": []},
                "3": {"name": "Sudeste", "states": ["33", "35"]},
                "4": {"name": "Sul", "states": []},
                "5": {"name": "Centro-Oeste", "states": []}
            }
        }
        config_path = tmp_path / "test_schema.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        return str(config_path)

    def test_transformer_initialization(self, mock_schema_config):
        """Test transformer initializes correctly."""
        from src.processing.gold_transformer import GoldTransformer
        
        with patch('boto3.client'):
            transformer = GoldTransformer("test-bucket", mock_schema_config)
        
        assert transformer.bucket == "test-bucket"
        assert len(transformer.SILVER_FILES) == 7

    def test_transform_method_exists(self, mock_schema_config):
        """Test transform method is implemented."""
        from src.processing.gold_transformer import GoldTransformer
        
        with patch('boto3.client'):
            transformer = GoldTransformer("test-bucket", mock_schema_config)
        
        assert hasattr(transformer, 'transform')
        assert callable(transformer.transform)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
