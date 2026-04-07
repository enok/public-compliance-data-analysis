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
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

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
        'inflation': 'bronze/economic/ipca_monthly.json',
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
        
        # Build municipality lookup table for cross-referencing
        if not self._transform_municipality_lookup():
            success = False
        
        # Transform fact tables
        if not self._transform_population():
            success = False
        
        if not self._transform_sanitation():
            success = False
        
        if not self._transform_literacy():
            success = False

        if not self._transform_inflation():
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

    def _build_annual_inflation_reference(
        self,
        inflation_data: Optional[List[Dict]],
        base_year: int = 2022,
    ) -> pd.DataFrame:
        """
        Build annual IPCA reference data using BCB SGS monthly series 433.

        The monthly series is chain-linked into an index and then averaged by year.
        Annual averages are used as the deflation basis for census-year income comparisons.
        """
        if not inflation_data:
            return pd.DataFrame()

        df = pd.DataFrame(inflation_data)
        if df.empty or 'data' not in df.columns or 'valor' not in df.columns:
            return pd.DataFrame()

        df['reference_date'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        df['monthly_ipca_rate_pct'] = pd.to_numeric(
            df['valor'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )
        df = df.dropna(subset=['reference_date', 'monthly_ipca_rate_pct']).copy()
        if df.empty:
            return pd.DataFrame()

        df = df.sort_values('reference_date').reset_index(drop=True)
        df['year'] = df['reference_date'].dt.year.astype(int)
        df['month'] = df['reference_date'].dt.month.astype(int)
        df['monthly_factor'] = 1 + (df['monthly_ipca_rate_pct'] / 100.0)
        df['ipca_index'] = 100.0 * df['monthly_factor'].cumprod()

        annual = (
            df.groupby('year', as_index=False)
            .agg(
                annual_ipca_rate_pct=('monthly_factor', lambda x: (np.prod(x) - 1) * 100),
                ipca_index_avg=('ipca_index', 'mean'),
                ipca_index_dec=('ipca_index', 'last'),
                last_reference_date=('reference_date', 'max'),
            )
            .sort_values('year')
            .reset_index(drop=True)
        )

        base_match = annual.loc[annual['year'] == base_year, 'ipca_index_avg']
        if base_match.empty or pd.isna(base_match.iloc[0]) or base_match.iloc[0] == 0:
            logger.error("❌ Missing IPCA base year %s in inflation reference", base_year)
            return pd.DataFrame()

        base_index = float(base_match.iloc[0])
        annual['annual_ipca_rate_pct'] = annual['annual_ipca_rate_pct'].round(4)
        annual['ipca_index_avg'] = annual['ipca_index_avg'].round(6)
        annual['ipca_index_dec'] = annual['ipca_index_dec'].round(6)
        annual['deflator_to_2022'] = (base_index / annual['ipca_index_avg']).round(6)
        annual['reference_base_year'] = base_year

        return annual[
            [
                'year',
                'annual_ipca_rate_pct',
                'ipca_index_avg',
                'ipca_index_dec',
                'deflator_to_2022',
                'reference_base_year',
                'last_reference_date',
            ]
        ]

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
                    muni_name_raw = parsed['municipality_name']
                    state_code = self._extract_state_code(muni_code)
                    region_code = self._get_region_code(state_code)
                    state_abbrev = self._get_state_abbrev(state_code)
                    
                    # Remove state suffix from municipality name (e.g., " - RO")
                    muni_name = re.sub(r'\s*-\s*[A-Z]{2}$', '', muni_name_raw) if muni_name_raw else f"Municipality {muni_code}"
                    
                    municipalities[muni_code] = {
                        'municipality_code': muni_code,
                        'municipality_name': muni_name,
                        'state_code': state_code,
                        'state_abbrev': state_abbrev,
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
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('dim_municipalities', 'SUCCESS' if success else 'FAILED',
                          0, len(df), list(self.BRONZE_FILES.values()), output_key)
        
        logger.info(f"✅ Municipalities dimension: {len(df)} municipalities")
        return success

    def _normalize_municipality_name(self, name: str) -> str:
        """
        Normalize municipality name for cross-referencing.
        
        Transformations:
        - Remove state suffix (e.g., " - RO")
        - Remove accents (é → e, ã → a, etc.)
        - Convert to uppercase
        - Remove apostrophes but keep the letter after
        
        Example: "Nova Brasilândia D'Oeste - RO" → "NOVA BRASILANDIA DOESTE"
        
        :param name: Original municipality name from IBGE.
        :return: Normalized name for matching.
        """
        if not name:
            return ""
        
        # Remove state suffix (e.g., " - RO", " - SP")
        name = re.sub(r'\s*-\s*[A-Z]{2}$', '', name)
        
        # Remove accents using unicode normalization
        # NFD decomposes characters (é → e + combining accent)
        # Then we filter out combining characters
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        
        # Remove apostrophes but keep adjacent letters (D'Oeste → DOeste)
        name = name.replace("'", "")
        
        # Convert to uppercase
        name = name.upper()
        
        # Normalize whitespace
        name = ' '.join(name.split())
        
        return name

    def _transform_municipality_lookup(self) -> bool:
        """
        Build municipality lookup table with normalized names for cross-referencing.
        
        This table enables matching municipality names from external sources
        (like Transparency Portal) to IBGE codes.
        
        Output: silver/dim_municipality_lookup/data.parquet
        
        Schema:
        - municipality_code: 7-digit IBGE code
        - municipality_name_normalized: Uppercase, no accents, no state suffix
        - state_code: 2-digit state code
        - state_abbrev: 2-letter state abbreviation (RO, SP, etc.)
        """
        logger.info("📊 Building municipality lookup table...")
        
        output_key = 'silver/dim_municipality_lookup/data.parquet'
        metadata_key = 'silver/dim_municipality_lookup/_metadata.json'
        source_keys = [self.BRONZE_FILES['pop_2010'], self.BRONZE_FILES['pop_2022']]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping municipality lookup: {reason}")
            return True
        
        # State code to abbreviation mapping
        state_abbrev = {
            '11': 'RO', '12': 'AC', '13': 'AM', '14': 'RR', '15': 'PA',
            '16': 'AP', '17': 'TO', '21': 'MA', '22': 'PI', '23': 'CE',
            '24': 'RN', '25': 'PB', '26': 'PE', '27': 'AL', '28': 'SE',
            '29': 'BA', '31': 'MG', '32': 'ES', '33': 'RJ', '35': 'SP',
            '41': 'PR', '42': 'SC', '43': 'RS', '50': 'MS', '51': 'MT',
            '52': 'GO', '53': 'DF'
        }
        
        municipalities = {}
        
        # Use population data as source for municipality list
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
                    
                    municipalities[muni_code] = {
                        'municipality_code': muni_code,
                        'municipality_name_normalized': self._normalize_municipality_name(muni_name),
                        'state_code': state_code,
                        'state_abbrev': state_abbrev.get(state_code, 'XX')
                    }
        
        if not municipalities:
            logger.error("❌ No municipalities extracted for lookup table")
            self.log_processing('dim_municipality_lookup', 'FAILED', 0, 0,
                              source_keys, output_key, 'No municipalities found')
            return False
        
        df = pd.DataFrame(list(municipalities.values()))
        
        # Sort by code for consistency
        df = df.sort_values('municipality_code').reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('dim_municipality_lookup', 'SUCCESS' if success else 'FAILED',
                          0, len(df), source_keys, output_key)
        
        logger.info(f"✅ Municipality lookup table: {len(df)} municipalities with normalized names")
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
                    'total_population': value
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
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
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
                    'total_households': value
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
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
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
        
        Both years use literacy rate directly from IBGE SIDRA API.
        - 2010: Table 1383 - Taxa de alfabetização (10+ years)
        - 2022: Table 9543 - Taxa de alfabetização (15+ years)
        
        Output: silver/fact_literacy/data.parquet
        """
        logger.info("📊 Transforming literacy data...")
        
        # Check if we can skip processing
        output_key = 'silver/fact_literacy/data.parquet'
        metadata_key = 'silver/fact_literacy/_metadata.json'
        source_keys = [
            self.BRONZE_FILES['literacy_2010'],
            self.BRONZE_FILES['literacy_2022']
        ]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping literacy: {reason}")
            return True
        
        all_records = []
        total_input = 0
        
        # Process 2010 data - literacy rate comes directly from Table 1383
        data_2010 = self._read_bronze_json(self.BRONZE_FILES['literacy_2010'])
        if data_2010:
            total_input += len(data_2010) - 1
            
            for row in data_2010[1:]:  # Skip header
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_float(parsed['value'])
                if value is None:
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': 2010,
                    'literacy_rate': value
                })
            
            logger.info(f"📊 2010 literacy: {len(data_2010) - 1} municipalities processed")
        else:
            logger.warning("⚠️ Missing 2010 literacy data file")
        
        # Process 2022 data - rate comes directly from API
        data_2022 = self._read_bronze_json(self.BRONZE_FILES['literacy_2022'])
        if data_2022:
            total_input += len(data_2022) - 1
            
            for row in data_2022[1:]:
                parsed = self._extract_sidra_data(row)
                if not parsed:
                    continue
                
                value = self._safe_float(parsed['value'])
                if value is None:
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': 2022,
                    'literacy_rate': value
                })
        else:
            logger.warning("⚠️ No literacy data for 2022")
        
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
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
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
        source_keys = [
            self.BRONZE_FILES['income_2010'],
            self.BRONZE_FILES['income_2022'],
            self.BRONZE_FILES['inflation'],
        ]
        
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping income: {reason}")
            return True
        
        inflation_reference = getattr(self, '_cached_inflation_reference', None)
        if inflation_reference is None or inflation_reference.empty:
            inflation_reference = self._build_annual_inflation_reference(
                self._read_bronze_json(self.BRONZE_FILES['inflation'])
            )
        if inflation_reference.empty:
            logger.error("❌ Inflation reference data is required to deflate census income")
            self.log_processing(
                'census_income',
                'FAILED',
                0,
                0,
                source_keys,
                output_key,
                'Missing or invalid inflation reference',
            )
            return False

        inflation_lookup = inflation_reference.set_index('year').to_dict('index')
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

                inflation_info = inflation_lookup.get(year)
                if not inflation_info:
                    logger.warning("⚠️ Missing inflation reference for income year %s", year)
                    continue
                
                all_records.append({
                    'municipality_code': parsed['municipality_code'],
                    'year': year,
                    'avg_income': value,
                    'annual_ipca_rate_pct': inflation_info['annual_ipca_rate_pct'],
                    'ipca_index_avg': inflation_info['ipca_index_avg'],
                    'deflator_to_2022': inflation_info['deflator_to_2022'],
                    'reference_base_year': inflation_info['reference_base_year'],
                    'avg_income_real_2022_brl': round(value * inflation_info['deflator_to_2022'], 2),
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
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat()
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('census_income', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        logger.info(f"✅ Income: {len(df)} records")
        return success

    def _transform_inflation(self) -> bool:
        """
        Transform monthly IPCA data into annual reference indices.

        Output: silver/dim_inflation_index/data.parquet
        """
        logger.info("📊 Transforming annual inflation reference...")

        output_key = 'silver/dim_inflation_index/data.parquet'
        metadata_key = 'silver/dim_inflation_index/_metadata.json'
        source_keys = [self.BRONZE_FILES['inflation']]

        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping inflation reference: {reason}")
            return True

        inflation_data = self._read_bronze_json(self.BRONZE_FILES['inflation'])
        annual = self._build_annual_inflation_reference(inflation_data)
        self._cached_inflation_reference = annual

        if annual.empty:
            logger.error("❌ No inflation reference records extracted")
            self.log_processing(
                'inflation_index',
                'FAILED',
                0,
                0,
                source_keys,
                output_key,
                'No inflation records extracted',
            )
            return False

        df = self.validate_schema(annual, 'inflation_index')
        df = df.sort_values('year').reset_index(drop=True)

        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))

        if success:
            metadata = {
                'output_file': output_key,
                'source_files': {key: self._get_object_digest(key) for key in source_keys},
                'record_count': len(df),
                'processed_at': pd.Timestamp.now().isoformat(),
                'reference_base_year': 2022,
            }
            self._save_silver_metadata(metadata_key, metadata)

        self.log_processing(
            'inflation_index',
            'SUCCESS' if success else 'FAILED',
            len(inflation_data or []),
            len(df),
            source_keys,
            output_key,
        )

        logger.info(f"✅ Inflation reference: {len(df)} annual records")
        return success


if __name__ == "__main__":
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "silver_schemas.json"
    
    transformer = IBGETransformer(BUCKET_NAME, str(CONFIG_FILE))
    transformer.transform()
