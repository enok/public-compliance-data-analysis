"""
IBGE Data Transformer for Silver Layer

Transforms IBGE Census data from Bronze to Silver layer:
- Population (2010, 2022)
- Sanitation (2010, 2022)
- Literacy (2010, 2022)
- Income (2010, 2022)

Also builds the municipalities dimension table.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

from src.processing.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class IBGETransformer(BaseTransformer):
    """Transformer for IBGE Census data (Bronze I → Silver)."""

    BRONZE_FILES = {
        'pop_2010': 'bronze/ibge/census_2010_pop.json',
        'pop_2022': 'bronze/ibge/census_2022_pop.json',
        'sanitation_2010': 'bronze/ibge/census_2010_sanitation.json',
        'sanitation_2022': 'bronze/ibge/census_2022_sanitation.json',
        'literacy_2010': 'bronze/ibge/census_2010_literacy.json',
        'literacy_2022': 'bronze/ibge/census_2022_literacy.json',
        'income_2010': 'bronze/ibge/census_2010_income.json',
        'income_2022': 'bronze/ibge/census_2022_income.json',
    }

    def get_source_datasets(self) -> List[str]:
        """Return list of source dataset names."""
        return list(self.BRONZE_FILES.keys())

    def transform(self) -> bool:
        """Execute all IBGE transformations."""
        logger.info("🚀 Starting IBGE Silver layer transformation...")
        
        success = True
        
        # Build municipalities dimension first (needed by other tables)
        if not self._transform_municipalities():
            success = False
        
        # Transform fact tables
        if not self._transform_population():
            success = False
        
        if not self._transform_sanitation():
            success = False
        
        if not self._transform_literacy():
            success = False
        
        if not self._transform_income():
            success = False
        
        if success:
            logger.info("✅ IBGE Silver layer transformation complete!")
        else:
            logger.warning("⚠️ IBGE Silver layer transformation completed with errors")
        
        return success

    def _parse_ibge_json(self, data: List[Dict], year: int) -> pd.DataFrame:
        """
        Parse IBGE SIDRA API JSON response into DataFrame.
        
        SIDRA returns data where:
        - First row is typically header metadata
        - Each row has keys like 'D1C', 'D2C', etc. for dimensions
        - 'V' contains the value
        - Municipality code is typically in one of the D*C fields
        
        :param data: Raw JSON data from SIDRA API.
        :param year: Census year for this data.
        :return: DataFrame with parsed data.
        """
        if not data or len(data) < 2:
            logger.warning(f"⚠️ Empty or insufficient IBGE data for year {year}")
            return pd.DataFrame()
        
        # Skip header row (first element contains column metadata)
        records = data[1:]
        
        df = pd.DataFrame(records)
        df['year'] = year
        
        return df

    def _extract_sidra_data(self, row: Dict) -> Optional[Dict]:
        """
        Extract data from a SIDRA API row.
        
        SIDRA format for n6/all (municipality level) queries:
        - D1C: Municipality Code (always 7 digits)
        - D1N: Municipality Name
        - D2C: Variable Code
        - D2N: Variable Name
        - D3C: Year Code
        - D3N: Year
        - V: Value
        - MN: Unit of measurement
        
        :param row: A single data row from SIDRA response.
        :return: Dict with municipality_code, municipality_name, value, or None.
        """
        muni_code = self._extract_municipality_code(row.get('D1C'))
        if not muni_code:
            return None
        
        return {
            'municipality_code': muni_code,
            'municipality_name': row.get('D1N', ''),
            'value': row.get('V'),
            'variable_code': row.get('D2C'),
            'variable_name': row.get('D2N'),
            'year': row.get('D3N') or row.get('D3C'),
            'unit': row.get('MN')
        }

    def _transform_municipalities(self) -> bool:
        """
        Build municipalities dimension table from population data.
        
        Output: silver/dim_municipalities/data.parquet
        """
        logger.info("📊 Building municipalities dimension table...")
        
        # Check if we can skip processing
        output_key = 'silver/dim_municipalities/data.parquet'
        metadata_key = 'silver/dim_municipalities/_metadata.json'
        source_keys = [self.BRONZE_FILES['pop_2010'], self.BRONZE_FILES['pop_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping municipalities dimension: {reason}")
            return True
        
        municipalities = {}
        
        # Use population data as primary source for municipality list
        for year in [2010, 2022]:
            key = f'pop_{year}'
            bronze_key = self.BRONZE_FILES[key]
            data = self._read_bronze_json(bronze_key)
            
            if not data:
                continue
            
            for row in data[1:]:  # Skip header
                parsed = self._extract_sidra_data(row)
                if parsed and parsed['municipality_code'] not in municipalities:
                    muni_code = parsed['municipality_code']
                    muni_name = parsed['municipality_name']
                    state_code = self._extract_state_code(muni_code)
                    region_code = self._get_region_code(state_code)
                    
                    municipalities[muni_code] = {
                        'municipality_code': muni_code,
                        'municipality_name': muni_name or f"Municipality {muni_code}",
                        'state_code': state_code,
                        'state_name': self._get_state_name(state_code),
                        'region_code': region_code,
                        'region_name': self._get_region_name(region_code)
                    }
        
        if not municipalities:
            logger.error("❌ No municipalities extracted from IBGE data")
            self.log_processing('dim_municipalities', 'FAILED', 0, 0, 
                              list(self.BRONZE_FILES.values()), 'silver/dim_municipalities/data.parquet',
                              'No municipalities found')
            return False
        
        df = pd.DataFrame(list(municipalities.values()))
        df = self.validate_schema(df, 'municipalities')
        
        # Sort by code for consistency
        df = df.sort_values('municipality_code').reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        
        # Also write JSON for easier inspection
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata for smart caching
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_bronze_file_hash(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('dim_municipalities', 'SUCCESS' if success else 'FAILED',
                          0, len(df), list(self.BRONZE_FILES.values()), output_key)
        
        logger.info(f"✅ Municipalities dimension: {len(df)} municipalities")
        return success

    def _transform_population(self) -> bool:
        """
        Transform population data from Census 2010 and 2022.
        
        Output: silver/fact_population/data.parquet
        """
        logger.info("📊 Transforming population data...")
        
        # Check if we can skip processing
        output_key = 'silver/fact_population/data.parquet'
        metadata_key = 'silver/fact_population/_metadata.json'
        source_keys = [self.BRONZE_FILES['pop_2010'], self.BRONZE_FILES['pop_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping population: {reason}")
            return True
        
        all_records = []
        total_input = 0
        
        for year in [2010, 2022]:
            key = f'pop_{year}'
            bronze_key = self.BRONZE_FILES[key]
            
            data = self._read_bronze_json(bronze_key)
            if not data:
                logger.warning(f"⚠️ No population data for {year}")
                continue
            
            total_input += len(data) - 1  # Exclude header
            
            # Extract one record per municipality (SIDRA returns one row per municipality)
            for row in data[1:]:  # Skip header
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_int(parsed['value'])
                if value is None:
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': year,
                    'total_population': value,
                    'urban_population': None,
                    'rural_population': None
                })
        
        if not all_records:
            logger.error("❌ No population records extracted")
            self.log_processing('census_population', 'FAILED', total_input, 0,
                              source_keys, 'silver/fact_population/data.parquet',
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'census_population')
        
        # Remove duplicates (keep first occurrence)
        df = df.drop_duplicates(subset=['municipality_code', 'year'], keep='first')
        df = df.sort_values(['municipality_code', 'year']).reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata for smart caching
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_bronze_file_hash(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('census_population', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        logger.info(f"✅ Population: {len(df)} records ({df['year'].nunique()} years)")
        return success

    def _transform_sanitation(self) -> bool:
        """
        Transform sanitation data from Census 2010 and 2022.
        
        Output: silver/fact_sanitation/data.parquet
        """
        logger.info("📊 Transforming sanitation data...")
        
        # Check if we can skip processing
        output_key = 'silver/fact_sanitation/data.parquet'
        metadata_key = 'silver/fact_sanitation/_metadata.json'
        source_keys = [self.BRONZE_FILES['sanitation_2010'], self.BRONZE_FILES['sanitation_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping sanitation: {reason}")
            return True
        
        all_records = []
        total_input = 0
        
        for year in [2010, 2022]:
            key = f'sanitation_{year}'
            bronze_key = self.BRONZE_FILES[key]
            
            data = self._read_bronze_json(bronze_key)
            if not data:
                logger.warning(f"⚠️ No sanitation data for {year}")
                continue
            
            total_input += len(data) - 1
            
            # Extract one record per municipality
            for row in data[1:]:
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_int(parsed['value'])
                if value is None:
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': year,
                    'total_households': value,
                    'households_with_water': None,
                    'households_with_sewage': None,
                    'households_with_garbage_collection': None,
                    'water_coverage_pct': None,
                    'sewage_coverage_pct': None
                })
        
        if not all_records:
            logger.error("❌ No sanitation records extracted")
            self.log_processing('census_sanitation', 'FAILED', total_input, 0,
                              source_keys, 'silver/fact_sanitation/data.parquet',
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'census_sanitation')
        
        df = df.drop_duplicates(subset=['municipality_code', 'year'], keep='first')
        df = df.sort_values(['municipality_code', 'year']).reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata for smart caching
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_bronze_file_hash(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('census_sanitation', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        logger.info(f"✅ Sanitation: {len(df)} records")
        return success

    def _transform_literacy(self) -> bool:
        """
        Transform literacy data from Census 2010 and 2022.
        
        Output: silver/fact_literacy/data.parquet
        """
        logger.info("📊 Transforming literacy data...")
        
        # Check if we can skip processing
        output_key = 'silver/fact_literacy/data.parquet'
        metadata_key = 'silver/fact_literacy/_metadata.json'
        source_keys = [self.BRONZE_FILES['literacy_2010'], self.BRONZE_FILES['literacy_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping literacy: {reason}")
            return True
        
        all_records = []
        total_input = 0
        
        for year in [2010, 2022]:
            key = f'literacy_{year}'
            bronze_key = self.BRONZE_FILES[key]
            
            data = self._read_bronze_json(bronze_key)
            if not data:
                logger.warning(f"⚠️ No literacy data for {year}")
                continue
            
            total_input += len(data) - 1
            
            # Extract literacy data - 2022 has rate directly, 2010 has counts
            for row in data[1:]:
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_float(parsed['value'])
                if value is None:
                    continue
                
                record = {
                    'municipality_code': parsed['municipality_code'],
                    'year': year,
                    'population_15_plus': None,
                    'literate_population': None,
                    'literacy_rate': None
                }
                
                # For 2022 (table 9543), V contains literacy rate %
                # For 2010 (table 3540), V contains population counts
                if year == 2022:
                    record['literacy_rate'] = value
                else:
                    record['population_15_plus'] = int(value)
                
                all_records.append(record)
        
        if not all_records:
            logger.error("❌ No literacy records extracted")
            self.log_processing('census_literacy', 'FAILED', total_input, 0,
                              source_keys, 'silver/fact_literacy/data.parquet',
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'census_literacy')
        
        df = df.drop_duplicates(subset=['municipality_code', 'year'], keep='first')
        df = df.sort_values(['municipality_code', 'year']).reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata for smart caching
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_bronze_file_hash(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('census_literacy', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        logger.info(f"✅ Literacy: {len(df)} records")
        return success

    def _transform_income(self) -> bool:
        """
        Transform income data from Census 2010 and 2022.
        
        Output: silver/fact_income/data.parquet
        """
        logger.info("📊 Transforming income data...")
        
        # Check if we can skip processing
        output_key = 'silver/fact_income/data.parquet'
        metadata_key = 'silver/fact_income/_metadata.json'
        source_keys = [self.BRONZE_FILES['income_2010'], self.BRONZE_FILES['income_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping income: {reason}")
            return True
        
        all_records = []
        total_input = 0
        
        for year in [2010, 2022]:
            key = f'income_{year}'
            bronze_key = self.BRONZE_FILES[key]
            
            data = self._read_bronze_json(bronze_key)
            if not data:
                logger.warning(f"⚠️ No income data for {year}")
                continue
            
            total_input += len(data) - 1
            
            # Extract income data - V contains average income in R$
            for row in data[1:]:
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_float(parsed['value'])
                if value is None:
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': year,
                    'avg_income': value,
                    'median_income': None,
                    'population_with_income': None
                })
        
        if not all_records:
            logger.error("❌ No income records extracted")
            self.log_processing('census_income', 'FAILED', total_input, 0,
                              source_keys, 'silver/fact_income/data.parquet',
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'census_income')
        
        df = df.drop_duplicates(subset=['municipality_code', 'year'], keep='first')
        df = df.sort_values(['municipality_code', 'year']).reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata for smart caching
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_bronze_file_hash(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('census_income', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        logger.info(f"✅ Income: {len(df)} records")
        return success


if __name__ == "__main__":
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "silver_schemas.json"
    
    transformer = IBGETransformer(BUCKET_NAME, str(CONFIG_FILE))
    transformer.transform()
