"""
Silver Layer Configuration Validation Tests

Validates silver_schemas.json configuration completeness and correctness.
"""

import pytest
import json
from pathlib import Path


class TestSilverSchemaConfig:
    """Test silver_schemas.json configuration."""

    @pytest.fixture
    def config_path(self):
        """Get path to silver_schemas.json."""
        return Path(__file__).resolve().parents[2] / "config" / "silver_schemas.json"

    @pytest.fixture
    def config(self, config_path):
        """Load silver_schemas.json."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def test_config_file_exists(self, config_path):
        """Test that silver_schemas.json exists."""
        assert config_path.exists(), "silver_schemas.json not found"

    def test_config_has_version(self, config):
        """Test that config has version field."""
        assert 'version' in config, "Config missing 'version' field"
        assert config['version'], "Version field is empty"

    def test_config_has_schemas(self, config):
        """Test that config has schemas section."""
        assert 'schemas' in config, "Config missing 'schemas' section"
        assert isinstance(config['schemas'], dict), "Schemas must be a dictionary"
        assert len(config['schemas']) > 0, "Schemas section is empty"

    def test_required_schemas_present(self, config):
        """Test that all required schemas are defined."""
        required_schemas = [
            'municipalities',
            'census_population',
            'census_sanitation',
            'census_literacy',
            'census_income',
            'inflation_index',
            'federal_transfers',
            'compliance_sanctions',
        ]
        
        schemas = config['schemas']
        for schema_name in required_schemas:
            assert schema_name in schemas, f"Required schema '{schema_name}' not found"

    def test_schema_structure(self, config):
        """Test that each schema has required fields."""
        schemas = config['schemas']
        
        for schema_name, schema_def in schemas.items():
            assert 'columns' in schema_def, f"Schema '{schema_name}' missing 'columns'"
            assert isinstance(schema_def['columns'], dict), f"Schema '{schema_name}' columns must be dict"
            assert len(schema_def['columns']) > 0, f"Schema '{schema_name}' has no columns"
            
            if 'output_path' in schema_def:
                assert schema_def['output_path'].startswith('silver/') or schema_def['output_path'].startswith('gold/'), \
                    f"Schema '{schema_name}' output_path must start with 'silver/' or 'gold/'"

    def test_column_definitions(self, config):
        """Test that column definitions are valid."""
        valid_types = ['string', 'integer', 'float', 'date', 'boolean']
        schemas = config['schemas']
        
        for schema_name, schema_def in schemas.items():
            columns = schema_def.get('columns', {})
            
            for col_name, col_def in columns.items():
                assert isinstance(col_def, dict), f"Column '{col_name}' in '{schema_name}' must be dict"
                assert 'type' in col_def, f"Column '{col_name}' in '{schema_name}' missing 'type'"
                assert col_def['type'] in valid_types, \
                    f"Column '{col_name}' in '{schema_name}' has invalid type: {col_def['type']}"

    def test_primary_keys_defined(self, config):
        """Test that schemas have primary keys defined."""
        schemas = config['schemas']
        
        for schema_name, schema_def in schemas.items():
            if schema_name.startswith('gold_'):
                continue
            
            assert 'primary_key' in schema_def, f"Schema '{schema_name}' missing 'primary_key'"
            pk = schema_def['primary_key']
            
            if isinstance(pk, list):
                assert len(pk) > 0, f"Schema '{schema_name}' has empty primary_key list"
                for key_col in pk:
                    assert key_col in schema_def['columns'], \
                        f"Primary key column '{key_col}' not in '{schema_name}' columns"
            else:
                assert pk in schema_def['columns'], \
                    f"Primary key '{pk}' not in '{schema_name}' columns"

    def test_state_mapping_complete(self, config):
        """Test that state_mapping has all Brazilian states."""
        assert 'state_mapping' in config, "Config missing 'state_mapping'"
        state_mapping = config['state_mapping']
        
        expected_states = 27
        assert len(state_mapping) == expected_states, \
            f"Expected {expected_states} states, found {len(state_mapping)}"
        
        expected_codes = [
            '11', '12', '13', '14', '15', '16', '17',
            '21', '22', '23', '24', '25', '26', '27', '28', '29',
            '31', '32', '33', '35',
            '41', '42', '43',
            '50', '51', '52', '53'
        ]
        
        for code in expected_codes:
            assert code in state_mapping, f"State code '{code}' missing from state_mapping"
            assert state_mapping[code], f"State code '{code}' has empty name"

    def test_region_mapping_complete(self, config):
        """Test that region_mapping has all Brazilian regions."""
        assert 'region_mapping' in config, "Config missing 'region_mapping'"
        region_mapping = config['region_mapping']
        
        expected_regions = ['1', '2', '3', '4', '5']
        for region_code in expected_regions:
            assert region_code in region_mapping, f"Region code '{region_code}' missing"
            
            region_info = region_mapping[region_code]
            assert 'name' in region_info, f"Region '{region_code}' missing 'name'"
            assert 'states' in region_info, f"Region '{region_code}' missing 'states'"
            assert len(region_info['states']) > 0, f"Region '{region_code}' has no states"

    def test_region_states_coverage(self, config):
        """Test that all states are assigned to regions."""
        state_mapping = config['state_mapping']
        region_mapping = config['region_mapping']
        
        states_in_regions = set()
        for region_info in region_mapping.values():
            states_in_regions.update(region_info['states'])
        
        all_state_codes = set(state_mapping.keys())
        
        assert states_in_regions == all_state_codes, \
            f"States in regions don't match state_mapping. Missing: {all_state_codes - states_in_regions}"

    def test_municipalities_schema(self, config):
        """Test municipalities dimension schema."""
        schema = config['schemas']['municipalities']
        
        required_columns = [
            'municipality_code',
            'municipality_name',
            'state_code',
            'state_name',
            'region_code',
            'region_name'
        ]
        
        columns = schema['columns']
        for col in required_columns:
            assert col in columns, f"municipalities schema missing column '{col}'"

    def test_fact_tables_have_municipality_code(self, config):
        """Test that fact tables have municipality_code or appropriate keys."""
        fact_tables = [
            'census_population',
            'census_sanitation',
            'census_literacy',
            'census_income'
        ]
        
        schemas = config['schemas']
        for table_name in fact_tables:
            assert table_name in schemas, f"Fact table '{table_name}' not found"
            columns = schemas[table_name]['columns']
            assert 'municipality_code' in columns, \
                f"Fact table '{table_name}' missing 'municipality_code'"

    def test_temporal_tables_have_year(self, config):
        """Test that temporal tables have year column."""
        temporal_tables = [
            'census_population',
            'census_sanitation',
            'census_literacy',
            'census_income',
            'federal_transfers'
        ]
        
        schemas = config['schemas']
        for table_name in temporal_tables:
            if table_name in schemas:
                columns = schemas[table_name]['columns']
                assert 'year' in columns, f"Temporal table '{table_name}' missing 'year'"

    def test_sanctions_schema(self, config):
        """Test compliance_sanctions schema."""
        schema = config['schemas']['compliance_sanctions']
        
        required_columns = [
            'sanction_id',
            'registry_type',
            'sanctioned_entity',
            'entity_type',
            'sanction_type',
            'sanction_start_date',
            'sanctioning_agency'
        ]
        
        columns = schema['columns']
        for col in required_columns:
            assert col in columns, f"compliance_sanctions schema missing column '{col}'"

    def test_nullable_columns_marked(self, config):
        """Test that nullable columns are properly marked."""
        schemas = config['schemas']
        
        for schema_name, schema_def in schemas.items():
            columns = schema_def.get('columns', {})
            
            for col_name, col_def in columns.items():
                if 'nullable' in col_def:
                    assert isinstance(col_def['nullable'], bool), \
                        f"Column '{col_name}' in '{schema_name}' nullable must be boolean"


class TestSilverSchemaConsistency:
    """Test consistency between schemas and actual implementation."""

    @pytest.fixture
    def config_path(self):
        """Get path to silver_schemas.json."""
        return Path(__file__).resolve().parents[2] / "config" / "silver_schemas.json"

    @pytest.fixture
    def config(self, config_path):
        """Load silver_schemas.json."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def test_ibge_bronze_files_match_schema(self, config):
        """Test that IBGE transformer bronze files match schema definitions."""
        from src.processing.ibge_transformer import IBGETransformer
        
        expected_datasets = [
            'pop_2010', 'pop_2022',
            'sanitation_2010', 'sanitation_2022',
            'literacy_2010', 'literacy_2022',
            'income_2010', 'income_2022',
            'inflation'
        ]
        
        bronze_files = IBGETransformer.BRONZE_FILES
        for dataset in expected_datasets:
            assert dataset in bronze_files, f"IBGE transformer missing dataset '{dataset}'"

    def test_transparency_sanctions_files_match_schema(self, config):
        """Test that Transparency transformer sanctions files match schema."""
        from src.processing.transparency_transformer import TransparencyTransformer
        
        expected_registries = ['ceis', 'cnep', 'cepim']
        
        sanctions_files = TransparencyTransformer.SANCTIONS_FILES
        for registry in expected_registries:
            key = f"{registry}_sanctions"
            assert key in sanctions_files, f"Transparency transformer missing '{key}'"

    def test_output_paths_consistent(self, config):
        """Test that output paths follow consistent naming."""
        schemas = config['schemas']
        
        for schema_name, schema_def in schemas.items():
            if 'output_path' not in schema_def:
                continue
            
            output_path = schema_def['output_path']
            
            if schema_name == 'municipalities':
                assert output_path.startswith('silver/dim_'), \
                    f"Dimension table '{schema_name}' should use 'dim_' prefix"
            elif schema_name.startswith('census_') or schema_name in ['federal_transfers', 'compliance_sanctions']:
                assert output_path.startswith('silver/fact_'), \
                    f"Fact table '{schema_name}' should use 'fact_' prefix"
            elif schema_name.startswith('gold_'):
                assert output_path.startswith('gold/'), \
                    f"Gold table '{schema_name}' should start with 'gold/'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
