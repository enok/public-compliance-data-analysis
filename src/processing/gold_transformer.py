"""
Gold Layer Transformer for Analysis-Ready Data

Transforms Silver layer data into Gold layer:
- Municipality-level socioeconomic aggregations with change metrics
- State-level summaries
- Sanctions aggregations
- Analysis-ready compliance dataset

Features:
- Idempotent: Safe to run multiple times
- Smart caching: Skips unchanged sources
- Graceful: Handles missing data
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

from src.processing.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class GoldTransformer(BaseTransformer):
    """Transformer for Gold layer aggregations (Silver → Gold)."""

    SILVER_FILES = {
        'municipalities': 'silver/dim_municipalities/data.parquet',
        'population': 'silver/fact_population/data.parquet',
        'sanitation': 'silver/fact_sanitation/data.parquet',
        'literacy': 'silver/fact_literacy/data.parquet',
        'income': 'silver/fact_income/data.parquet',
        'sanctions': 'silver/fact_sanctions/data.parquet',
        'federal_transfers': 'silver/fact_federal_transfers/data.parquet',
    }

    def get_source_datasets(self) -> List[str]:
        """Return list of source dataset names."""
        return list(self.SILVER_FILES.keys())

    def transform(self) -> bool:
        """Execute all Gold layer transformations."""
        logger.info("🚀 Starting Gold layer transformation...")
        
        success = True
        
        # Build municipality socioeconomic aggregation
        if not self._transform_municipality_socioeconomic():
            success = False
        
        # Build state-level summaries
        if not self._transform_state_summary():
            success = False
        
        # Build sanctions aggregations
        if not self._transform_sanctions_summary():
            success = False
        
        # Build analysis-ready compliance dataset
        if not self._transform_analysis_compliance():
            success = False

        # Build municipality-level analysis-ready compliance dataset
        if not self._transform_analysis_compliance_municipality():
            success = False
        
        # Build consolidated clustering dataset
        if not self._transform_clustering_dataset():
            success = False
        
        if success:
            logger.info("✅ Gold layer transformation complete!")
        else:
            logger.warning("⚠️ Gold layer transformation completed with errors")
        
        return success

    def _read_silver_parquet(self, s3_key: str) -> Optional[pd.DataFrame]:
        """
        Read a Parquet file from the Silver layer in S3.
        
        :param s3_key: S3 key for the silver file
        :return: DataFrame or None if not found
        """
        try:
            import tempfile
            import os
            
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp.write(response['Body'].read())
                tmp_path = tmp.name
            
            df = pd.read_parquet(tmp_path)
            os.unlink(tmp_path)
            
            logger.info(f"📥 Read {len(df)} records from {s3_key}")
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"⚠️ Silver file not found: {s3_key}")
                return None
            raise
        except Exception as e:
            logger.error(f"❌ Failed to read Parquet from {s3_key}: {e}")
            return None

    def _write_gold_parquet(self, df: pd.DataFrame, s3_key: str) -> bool:
        """
        Write a DataFrame to the Gold layer as Parquet.
        
        :param df: DataFrame to write
        :param s3_key: S3 key for the gold file
        :return: True if successful
        """
        return self._write_silver_parquet(df, s3_key)

    def _write_gold_json(self, df: pd.DataFrame, s3_key: str) -> bool:
        """
        Write a DataFrame to the Gold layer as JSON.
        
        :param df: DataFrame to write
        :param s3_key: S3 key for the gold file
        :return: True if successful
        """
        return self._write_silver_json(df, s3_key)

    def _calculate_change_pct(self, current: float, previous: float) -> Optional[float]:
        """
        Calculate percentage change between two values.
        
        :param current: Current value
        :param previous: Previous value
        :return: Percentage change or None if invalid
        """
        # Check NA first to avoid boolean comparison errors with pd.NA
        if previous is None or pd.isna(previous):
            return None
        if current is None or pd.isna(current):
            return None
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 2)

    def _transform_municipality_socioeconomic(self) -> bool:
        """
        Build municipality-level socioeconomic aggregation with change metrics.
        
        Combines population, literacy, income, sanitation data for 2010 and 2022,
        calculating change indicators between census years.
        
        Output: gold/agg_municipality_socioeconomic/data.parquet
        """
        logger.info("📊 Building municipality socioeconomic aggregation...")
        
        output_key = 'gold/agg_municipality_socioeconomic/data.parquet'
        metadata_key = 'gold/agg_municipality_socioeconomic/_metadata.json'
        
        # Collect source keys
        source_keys = [
            self.SILVER_FILES['municipalities'],
            self.SILVER_FILES['population'],
            self.SILVER_FILES['literacy'],
            self.SILVER_FILES['income'],
            self.SILVER_FILES['sanitation'],
        ]
        
        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping municipality socioeconomic: {reason}")
            return True
        
        # Read silver tables
        df_muni = self._read_silver_parquet(self.SILVER_FILES['municipalities'])
        df_pop = self._read_silver_parquet(self.SILVER_FILES['population'])
        df_lit = self._read_silver_parquet(self.SILVER_FILES['literacy'])
        df_inc = self._read_silver_parquet(self.SILVER_FILES['income'])
        df_san = self._read_silver_parquet(self.SILVER_FILES['sanitation'])
        
        if df_muni is None:
            logger.error("❌ Missing municipalities dimension table")
            return False
        
        # Start with municipalities as base
        result = df_muni.copy()
        
        # Process population data
        if df_pop is not None:
            pop_2010 = df_pop[df_pop['year'] == 2010][['municipality_code', 'total_population']].copy()
            pop_2010.columns = ['municipality_code', 'population_2010']
            
            pop_2022 = df_pop[df_pop['year'] == 2022][['municipality_code', 'total_population']].copy()
            pop_2022.columns = ['municipality_code', 'population_2022']
            
            result = result.merge(pop_2010, on='municipality_code', how='left')
            result = result.merge(pop_2022, on='municipality_code', how='left')
            
            # Calculate population change
            result['population_change_pct'] = result.apply(
                lambda row: self._calculate_change_pct(row['population_2022'], row['population_2010']),
                axis=1
            )
        
        # Process literacy data
        if df_lit is not None:
            lit_2010 = df_lit[df_lit['year'] == 2010][['municipality_code', 'literacy_rate']].copy()
            lit_2010.columns = ['municipality_code', 'literacy_rate_2010']
            
            lit_2022 = df_lit[df_lit['year'] == 2022][['municipality_code', 'literacy_rate']].copy()
            lit_2022.columns = ['municipality_code', 'literacy_rate_2022']
            
            result = result.merge(lit_2010, on='municipality_code', how='left')
            result = result.merge(lit_2022, on='municipality_code', how='left')
            
            # Calculate literacy change (in percentage points)
            result['literacy_change_pp'] = result.apply(
                lambda row: round(row['literacy_rate_2022'] - row['literacy_rate_2010'], 2)
                if pd.notna(row.get('literacy_rate_2022')) and pd.notna(row.get('literacy_rate_2010'))
                else None,
                axis=1
            )
        
        # Process income data
        if df_inc is not None:
            inc_2010 = df_inc[
                df_inc['year'] == 2010
            ][['municipality_code', 'avg_income', 'avg_income_real_2022_brl']].copy()
            inc_2010.columns = [
                'municipality_code',
                'avg_income_2010',
                'avg_income_real_2010_2022_brl',
            ]
            
            inc_2022 = df_inc[
                df_inc['year'] == 2022
            ][['municipality_code', 'avg_income', 'avg_income_real_2022_brl']].copy()
            inc_2022.columns = [
                'municipality_code',
                'avg_income_2022',
                'avg_income_real_2022_2022_brl',
            ]
            
            result = result.merge(inc_2010, on='municipality_code', how='left')
            result = result.merge(inc_2022, on='municipality_code', how='left')
            
            # Calculate income changes in nominal and real terms
            result['income_change_pct'] = result.apply(
                lambda row: self._calculate_change_pct(row['avg_income_2022'], row['avg_income_2010']),
                axis=1
            )
            result['income_change_real_pct'] = result.apply(
                lambda row: self._calculate_change_pct(
                    row['avg_income_real_2022_2022_brl'],
                    row['avg_income_real_2010_2022_brl'],
                ),
                axis=1
            )
        
        # Process sanitation data
        if df_san is not None:
            san_2010 = df_san[df_san['year'] == 2010][['municipality_code', 'total_households']].copy()
            san_2010.columns = ['municipality_code', 'households_2010']
            
            san_2022 = df_san[df_san['year'] == 2022][['municipality_code', 'total_households']].copy()
            san_2022.columns = ['municipality_code', 'households_2022']
            
            result = result.merge(san_2010, on='municipality_code', how='left')
            result = result.merge(san_2022, on='municipality_code', how='left')
            
            # Calculate households change
            result['households_change_pct'] = result.apply(
                lambda row: self._calculate_change_pct(row['households_2022'], row['households_2010']),
                axis=1
            )
        
        # Sort by municipality code
        result = result.sort_values('municipality_code').reset_index(drop=True)
        
        # Validate schema
        result = self.validate_schema(result, 'gold_municipality_socioeconomic')
        
        # Write output
        success = self._write_gold_parquet(result, output_key)
        self._write_gold_json(result, output_key.replace('.parquet', '.json'))
        
        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(result)
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('gold_municipality_socioeconomic', 'SUCCESS' if success else 'FAILED',
                          len(df_muni) if df_muni is not None else 0, len(result), source_keys, output_key)
        
        logger.info(f"✅ Municipality Socioeconomic: {len(result)} municipalities")
        return success

    def _transform_state_summary(self) -> bool:
        """
        Build state-level summary aggregations.
        
        Aggregates municipality data at state level for regional analysis.
        
        Output: gold/agg_state_summary/data.parquet
        """
        logger.info("📊 Building state summary aggregation...")
        
        output_key = 'gold/agg_state_summary/data.parquet'
        metadata_key = 'gold/agg_state_summary/_metadata.json'
        
        source_keys = [
            self.SILVER_FILES['municipalities'],
            self.SILVER_FILES['population'],
            self.SILVER_FILES['income'],
            self.SILVER_FILES['sanctions'],
        ]
        
        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping state summary: {reason}")
            return True
        
        # Read silver tables
        df_muni = self._read_silver_parquet(self.SILVER_FILES['municipalities'])
        df_pop = self._read_silver_parquet(self.SILVER_FILES['population'])
        df_inc = self._read_silver_parquet(self.SILVER_FILES['income'])
        df_sanctions = self._read_silver_parquet(self.SILVER_FILES['sanctions'])
        
        if df_muni is None:
            logger.error("❌ Missing municipalities dimension table")
            return False
        
        # Get unique states from municipalities
        states = df_muni[['state_code', 'state_name', 'region_code', 'region_name']].drop_duplicates()
        
        # Count municipalities per state
        muni_counts = df_muni.groupby('state_code').size().reset_index(name='municipality_count')
        states = states.merge(muni_counts, on='state_code', how='left')
        
        # Aggregate population by state
        if df_pop is not None:
            # Add state_code to population data
            pop_with_state = df_pop.merge(
                df_muni[['municipality_code', 'state_code']],
                on='municipality_code',
                how='left'
            )
            
            # Population 2022
            pop_2022 = pop_with_state[pop_with_state['year'] == 2022].groupby('state_code').agg(
                total_population_2022=('total_population', 'sum')
            ).reset_index()
            states = states.merge(pop_2022, on='state_code', how='left')
            
            # Population 2010
            pop_2010 = pop_with_state[pop_with_state['year'] == 2010].groupby('state_code').agg(
                total_population_2010=('total_population', 'sum')
            ).reset_index()
            states = states.merge(pop_2010, on='state_code', how='left')
            
            # Calculate population change
            states['population_change_pct'] = states.apply(
                lambda row: self._calculate_change_pct(
                    row.get('total_population_2022'), 
                    row.get('total_population_2010')
                ),
                axis=1
            )
        
        # Aggregate income by state (average of municipality averages)
        if df_inc is not None:
            inc_with_state = df_inc.merge(
                df_muni[['municipality_code', 'state_code']],
                on='municipality_code',
                how='left'
            )
            
            inc_2010 = inc_with_state[inc_with_state['year'] == 2010].groupby('state_code').agg(
                avg_income_2010=('avg_income', 'mean'),
                avg_income_real_2010_2022_brl=('avg_income_real_2022_brl', 'mean'),
            ).reset_index()
            inc_2010['avg_income_2010'] = inc_2010['avg_income_2010'].round(2)
            inc_2010['avg_income_real_2010_2022_brl'] = inc_2010['avg_income_real_2010_2022_brl'].round(2)
            states = states.merge(inc_2010, on='state_code', how='left')

            inc_2022 = inc_with_state[inc_with_state['year'] == 2022].groupby('state_code').agg(
                avg_income_2022=('avg_income', 'mean'),
                avg_income_real_2022_2022_brl=('avg_income_real_2022_brl', 'mean'),
            ).reset_index()
            inc_2022['avg_income_2022'] = inc_2022['avg_income_2022'].round(2)
            inc_2022['avg_income_real_2022_2022_brl'] = inc_2022['avg_income_real_2022_2022_brl'].round(2)
            states = states.merge(inc_2022, on='state_code', how='left')

            states['income_change_pct'] = states.apply(
                lambda row: self._calculate_change_pct(
                    row.get('avg_income_2022'),
                    row.get('avg_income_2010'),
                ),
                axis=1
            )
            states['income_change_real_pct'] = states.apply(
                lambda row: self._calculate_change_pct(
                    row.get('avg_income_real_2022_2022_brl'),
                    row.get('avg_income_real_2010_2022_brl'),
                ),
                axis=1
            )
        
        # Count sanctions by state
        if df_sanctions is not None:
            sanctions_by_state = df_sanctions[df_sanctions['state_code'].notna()].groupby('state_code').agg(
                total_sanctions=('sanction_id', 'nunique'),
                sanctions_pf=('entity_type', lambda x: (x == 'PF').sum()),
                sanctions_pj=('entity_type', lambda x: (x == 'PJ').sum())
            ).reset_index()
            states = states.merge(sanctions_by_state, on='state_code', how='left')
            
            # Calculate sanctions per 100k population
            states['sanctions_per_100k'] = states.apply(
                lambda row: round((row.get('total_sanctions', 0) / row.get('total_population_2022', 1)) * 100000, 2)
                if pd.notna(row.get('total_population_2022')) and row.get('total_population_2022', 0) > 0
                else None,
                axis=1
            )
        
        # Fill NaN values for counts
        for col in ['total_sanctions', 'sanctions_pf', 'sanctions_pj']:
            if col in states.columns:
                states[col] = states[col].fillna(0).astype(int)
        
        # Sort by state code
        states = states.sort_values('state_code').reset_index(drop=True)
        
        # Validate schema
        states = self.validate_schema(states, 'gold_state_summary')
        
        # Write output
        success = self._write_gold_parquet(states, output_key)
        self._write_gold_json(states, output_key.replace('.parquet', '.json'))
        
        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(states)
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('gold_state_summary', 'SUCCESS' if success else 'FAILED',
                          len(df_muni) if df_muni is not None else 0, len(states), source_keys, output_key)
        
        logger.info(f"✅ State Summary: {len(states)} states")
        return success

    def _transform_sanctions_summary(self) -> bool:
        """
        Build sanctions aggregations by registry type and state.
        
        Output: gold/agg_sanctions_summary/data.parquet
        """
        logger.info("📊 Building sanctions summary aggregation...")
        
        output_key = 'gold/agg_sanctions_summary/data.parquet'
        metadata_key = 'gold/agg_sanctions_summary/_metadata.json'
        
        source_keys = [self.SILVER_FILES['sanctions']]
        
        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping sanctions summary: {reason}")
            return True
        
        df_sanctions = self._read_silver_parquet(self.SILVER_FILES['sanctions'])
        
        if df_sanctions is None or len(df_sanctions) == 0:
            logger.warning("⚠️ No sanctions data available")
            return False
        
        # Aggregate by registry type
        registry_summary = df_sanctions.groupby('registry_type').agg(
            total_sanctions=('sanction_id', 'nunique'),
            sanctions_pf=('entity_type', lambda x: (x == 'PF').sum()),
            sanctions_pj=('entity_type', lambda x: (x == 'PJ').sum()),
            unique_agencies=('sanctioning_agency', 'nunique'),
            earliest_sanction=('sanction_start_date', 'min'),
            latest_sanction=('sanction_start_date', 'max')
        ).reset_index()
        
        # Calculate PF/PJ ratio
        registry_summary['pj_ratio_pct'] = registry_summary.apply(
            lambda row: round((row['sanctions_pj'] / row['total_sanctions']) * 100, 2)
            if row['total_sanctions'] > 0 else 0,
            axis=1
        )
        
        # Add state breakdown as nested structure (for JSON output)
        state_breakdown = df_sanctions[df_sanctions['state_code'].notna()].groupby(
            ['registry_type', 'state_code']
        ).agg(
            count=('sanction_id', 'nunique')
        ).reset_index()
        
        # Pivot to wide format for each registry
        state_pivot = state_breakdown.pivot(
            index='registry_type', 
            columns='state_code', 
            values='count'
        ).fillna(0).astype(int).reset_index()
        
        # Merge with summary
        result = registry_summary.merge(state_pivot, on='registry_type', how='left')
        
        # Sort by registry type
        result = result.sort_values('registry_type').reset_index(drop=True)
        
        # Validate schema
        result = self.validate_schema(result, 'gold_sanctions_summary')
        
        # Write output
        success = self._write_gold_parquet(result, output_key)
        self._write_gold_json(result, output_key.replace('.parquet', '.json'))
        
        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(result)
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('gold_sanctions_summary', 'SUCCESS' if success else 'FAILED',
                          len(df_sanctions), len(result), source_keys, output_key)
        
        logger.info(f"✅ Sanctions Summary: {len(result)} registry types")
        return success

    def _transform_analysis_compliance(self) -> bool:
        """
        Build analysis-ready compliance dataset.
        
        Joins socioeconomic indicators with sanctions data at state level
        for correlation and regression analysis.
        
        Output: gold/analysis_compliance/data.parquet
        """
        logger.info("📊 Building analysis compliance dataset...")
        
        output_key = 'gold/analysis_compliance/data.parquet'
        metadata_key = 'gold/analysis_compliance/_metadata.json'
        
        source_keys = [
            self.SILVER_FILES['municipalities'],
            self.SILVER_FILES['population'],
            self.SILVER_FILES['literacy'],
            self.SILVER_FILES['income'],
            self.SILVER_FILES['sanctions'],
        ]
        
        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping analysis compliance: {reason}")
            return True
        
        # Read silver tables
        df_muni = self._read_silver_parquet(self.SILVER_FILES['municipalities'])
        df_pop = self._read_silver_parquet(self.SILVER_FILES['population'])
        df_lit = self._read_silver_parquet(self.SILVER_FILES['literacy'])
        df_inc = self._read_silver_parquet(self.SILVER_FILES['income'])
        df_sanctions = self._read_silver_parquet(self.SILVER_FILES['sanctions'])
        
        if df_muni is None:
            logger.error("❌ Missing municipalities dimension table")
            return False
        
        # Build state-level aggregation for analysis
        states = df_muni[['state_code', 'state_name', 'region_code', 'region_name']].drop_duplicates()
        
        # Count municipalities
        muni_counts = df_muni.groupby('state_code').size().reset_index(name='n_municipalities')
        states = states.merge(muni_counts, on='state_code', how='left')
        
        # Population 2022
        if df_pop is not None:
            pop_with_state = df_pop.merge(
                df_muni[['municipality_code', 'state_code']], 
                on='municipality_code', 
                how='left'
            )
            pop_2022 = pop_with_state[pop_with_state['year'] == 2022].groupby('state_code').agg(
                population=('total_population', 'sum')
            ).reset_index()
            states = states.merge(pop_2022, on='state_code', how='left')
        
        # Literacy 2022 (average across municipalities)
        if df_lit is not None:
            lit_with_state = df_lit.merge(
                df_muni[['municipality_code', 'state_code']], 
                on='municipality_code', 
                how='left'
            )
            lit_2022 = lit_with_state[lit_with_state['year'] == 2022].groupby('state_code').agg(
                avg_literacy_rate=('literacy_rate', 'mean')
            ).reset_index()
            lit_2022['avg_literacy_rate'] = lit_2022['avg_literacy_rate'].round(2)
            states = states.merge(lit_2022, on='state_code', how='left')
        
        # Income 2022 (average across municipalities)
        if df_inc is not None:
            inc_with_state = df_inc.merge(
                df_muni[['municipality_code', 'state_code']], 
                on='municipality_code', 
                how='left'
            )
            inc_2022 = inc_with_state[inc_with_state['year'] == 2022].groupby('state_code').agg(
                avg_income=('avg_income', 'mean')
            ).reset_index()
            inc_2022['avg_income'] = inc_2022['avg_income'].round(2)
            states = states.merge(inc_2022, on='state_code', how='left')
        
        # Sanctions counts
        if df_sanctions is not None and len(df_sanctions) > 0:
            sanctions_by_state = df_sanctions[df_sanctions['state_code'].notna()].groupby('state_code').agg(
                n_sanctions=('sanction_id', 'nunique'),
                n_sanctions_ceis=('registry_type', lambda x: (x == 'CEIS').sum()),
                n_sanctions_cnep=('registry_type', lambda x: (x == 'CNEP').sum()),
                n_sanctions_cepim=('registry_type', lambda x: (x == 'CEPIM').sum())
            ).reset_index()
            states = states.merge(sanctions_by_state, on='state_code', how='left')
        
        # Fill NaN values for counts
        count_cols = ['n_sanctions', 'n_sanctions_ceis', 'n_sanctions_cnep', 
                      'n_sanctions_cepim']
        for col in count_cols:
            if col in states.columns:
                states[col] = states[col].fillna(0).astype(int)
        
        # Calculate derived metrics for analysis
        if 'population' in states.columns and 'n_sanctions' in states.columns:
            # Sanctions per 100,000 population
            states['sanctions_per_100k'] = states.apply(
                lambda row: round((row['n_sanctions'] / row['population']) * 100000, 4)
                if pd.notna(row.get('population')) and row.get('population', 0) > 0
                else None,
                axis=1
            )
            
            # Log-transformed population (for regression)
            states['log_population'] = states['population'].apply(
                lambda x: round(np.log(x), 4) if pd.notna(x) and x > 0 else None
            )
        
        if 'avg_income' in states.columns:
            # Log-transformed income (for regression)
            states['log_income'] = states['avg_income'].apply(
                lambda x: round(np.log(x), 4) if pd.notna(x) and x > 0 else None
            )
        
        # Add dummy variables for regions (for regression)
        for region_code, region_info in self.region_mapping.items():
            region_name = region_info['name'].lower().replace(' ', '_').replace('-', '_')
            col_name = f'is_{region_name}'
            states[col_name] = (states['region_code'] == region_code).astype(int)
        
        # Sort by state code
        states = states.sort_values('state_code').reset_index(drop=True)
        
        # Validate schema
        states = self.validate_schema(states, 'gold_analysis_compliance')
        
        # Write output
        success = self._write_gold_parquet(states, output_key)
        self._write_gold_json(states, output_key.replace('.parquet', '.json'))
        
        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(states)
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('gold_analysis_compliance', 'SUCCESS' if success else 'FAILED',
                          0, len(states), source_keys, output_key)
        
        logger.info(f"✅ Analysis Compliance: {len(states)} states ready for analysis")
        return success

    def _transform_analysis_compliance_municipality(self) -> bool:
        """
        Build municipality-level analysis-ready compliance dataset.

        Joins socioeconomic indicators, sanctions, and transfer aggregates at
        municipality level to support statistical and ML analysis beyond the
        state-only perspective.

        Output: gold/analysis_compliance_municipality/data.parquet
        """
        logger.info("📊 Building municipality analysis compliance dataset...")

        output_key = 'gold/analysis_compliance_municipality/data.parquet'
        metadata_key = 'gold/analysis_compliance_municipality/_metadata.json'

        source_keys = [
            self.SILVER_FILES['municipalities'],
            self.SILVER_FILES['population'],
            self.SILVER_FILES['literacy'],
            self.SILVER_FILES['income'],
            self.SILVER_FILES['sanctions'],
            self.SILVER_FILES['federal_transfers'],
        ]

        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping municipality analysis compliance: {reason}")
            return True

        # Read silver tables
        df_muni = self._read_silver_parquet(self.SILVER_FILES['municipalities'])
        df_pop = self._read_silver_parquet(self.SILVER_FILES['population'])
        df_lit = self._read_silver_parquet(self.SILVER_FILES['literacy'])
        df_inc = self._read_silver_parquet(self.SILVER_FILES['income'])
        df_sanctions = self._read_silver_parquet(self.SILVER_FILES['sanctions'])
        df_transfers = self._read_silver_parquet(self.SILVER_FILES['federal_transfers'])

        if df_muni is None:
            logger.error("❌ Missing municipalities dimension table")
            return False

        # Base result (one row per municipality)
        cities = df_muni.copy()

        # Population 2022
        if df_pop is not None:
            pop_2022 = df_pop[df_pop['year'] == 2022][['municipality_code', 'total_population']].copy()
            pop_2022.columns = ['municipality_code', 'population_2022']
            cities = cities.merge(pop_2022, on='municipality_code', how='left')

        # Literacy 2022
        if df_lit is not None:
            lit_2022 = df_lit[df_lit['year'] == 2022][['municipality_code', 'literacy_rate']].copy()
            lit_2022.columns = ['municipality_code', 'literacy_rate_2022']
            cities = cities.merge(lit_2022, on='municipality_code', how='left')

        # Income 2022 (nominal and real)
        if df_inc is not None:
            inc_2022 = df_inc[df_inc['year'] == 2022][
                ['municipality_code', 'avg_income', 'avg_income_real_2022_brl']
            ].copy()
            inc_2022.columns = [
                'municipality_code',
                'avg_income_2022',
                'avg_income_real_2022_2022_brl',
            ]
            cities = cities.merge(inc_2022, on='municipality_code', how='left')

        # Sanctions counts by municipality (only rows with municipality_code)
        if df_sanctions is not None and len(df_sanctions) > 0:
            sanctions_geo = df_sanctions[df_sanctions['municipality_code'].notna()].copy()
            if len(sanctions_geo) > 0:
                sanctions_by_city = sanctions_geo.groupby('municipality_code').agg(
                    n_sanctions=('sanction_id', 'nunique'),
                    n_sanctions_ceis=('registry_type', lambda x: (x == 'CEIS').sum()),
                    n_sanctions_cnep=('registry_type', lambda x: (x == 'CNEP').sum()),
                    n_sanctions_cepim=('registry_type', lambda x: (x == 'CEPIM').sum()),
                ).reset_index()
                cities = cities.merge(sanctions_by_city, on='municipality_code', how='left')

        # Ensure sanctions columns exist even when geolocation coverage is absent.
        for col in ['n_sanctions', 'n_sanctions_ceis', 'n_sanctions_cnep', 'n_sanctions_cepim']:
            if col not in cities.columns:
                cities[col] = 0

        # Federal transfers by municipality
        if df_transfers is not None and len(df_transfers) > 0:
            transfers_geo = df_transfers[df_transfers['municipality_code'].notna()].copy()
            if len(transfers_geo) > 0:
                transfers_by_city = transfers_geo.groupby('municipality_code').agg(
                    total_transfers=('transfer_amount', 'sum'),
                    n_transfer_records=('transfer_amount', 'size'),
                ).reset_index()
                transfers_by_city['total_transfers'] = transfers_by_city['total_transfers'].round(2)
                cities = cities.merge(transfers_by_city, on='municipality_code', how='left')

        # Fill count columns
        count_cols = [
            'n_sanctions',
            'n_sanctions_ceis',
            'n_sanctions_cnep',
            'n_sanctions_cepim',
            'n_transfer_records',
        ]
        for col in count_cols:
            if col in cities.columns:
                cities[col] = cities[col].fillna(0).astype(int)

        # Fill transfer amount where missing
        if 'total_transfers' in cities.columns:
            cities['total_transfers'] = cities['total_transfers'].fillna(0.0).clip(lower=0.0).round(2)
        else:
            cities['total_transfers'] = 0.0
            cities['n_transfer_records'] = 0

        # Derived indicators
        if 'population_2022' in cities.columns and 'n_sanctions' in cities.columns:
            cities['sanctions_per_100k'] = cities.apply(
                lambda row: round((row['n_sanctions'] / row['population_2022']) * 100000, 4)
                if pd.notna(row.get('population_2022')) and row.get('population_2022', 0) > 0
                else None,
                axis=1
            )

        if 'population_2022' in cities.columns and 'total_transfers' in cities.columns:
            cities['avg_transfer_per_capita'] = cities.apply(
                lambda row: round(row['total_transfers'] / row['population_2022'], 4)
                if pd.notna(row.get('population_2022')) and row.get('population_2022', 0) > 0
                else None,
                axis=1
            )

        cities['sanctions_per_million_brl_transfers'] = cities.apply(
            lambda row: round((row['n_sanctions'] / row['total_transfers']) * 1_000_000, 6)
            if pd.notna(row.get('total_transfers')) and row.get('total_transfers', 0) > 0
            else None,
            axis=1
        )

        if 'population_2022' in cities.columns:
            cities['log_population'] = cities['population_2022'].apply(
                lambda x: round(np.log(x), 6) if pd.notna(x) and x > 0 else None
            )

        if 'avg_income_real_2022_2022_brl' in cities.columns:
            cities['log_income'] = cities['avg_income_real_2022_2022_brl'].apply(
                lambda x: round(np.log(x), 6) if pd.notna(x) and x > 0 else None
            )

        cities['log_total_transfers'] = cities['total_transfers'].apply(
            lambda x: round(np.log1p(x), 6) if pd.notna(x) and x >= 0 else None
        )

        # Sort for deterministic outputs
        cities = cities.sort_values('municipality_code').reset_index(drop=True)

        # Validate schema
        cities = self.validate_schema(cities, 'gold_analysis_compliance_municipality')

        # Write output
        success = self._write_gold_parquet(cities, output_key)
        self._write_gold_json(cities, output_key.replace('.parquet', '.json'))

        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash

            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(cities)
            }
            self._save_silver_metadata(metadata_key, metadata)

        self.log_processing(
            'gold_analysis_compliance_municipality',
            'SUCCESS' if success else 'FAILED',
            len(df_muni),
            len(cities),
            source_keys,
            output_key,
        )

        logger.info(f"✅ Municipality Analysis Compliance: {len(cities)} municipalities ready for analysis")
        return success


    def _transform_clustering_dataset(self) -> bool:
        """
        Build consolidated municipality-level dataset for K-means clustering and PCA.
        
        Requirements:
        - One record per city (municipality)
        - No duplicate cities
        - No missing values (rows with missing values are dropped)
        - All relevant features from both census years as columns
        - Normalized features for clustering (z-score standardization)
        
        Features included:
        - Population (2010, 2022, change)
        - Literacy rate (2010, 2022, change)
        - Income (2010, 2022, change)
        - Households (2010, 2022, change)
        
        Output: gold/consolidated_clustering/data.parquet
        """
        logger.info("📊 Building consolidated clustering dataset...")
        
        output_key = 'gold/consolidated_clustering/data.parquet'
        metadata_key = 'gold/consolidated_clustering/_metadata.json'
        
        source_keys = [
            self.SILVER_FILES['municipalities'],
            self.SILVER_FILES['population'],
            self.SILVER_FILES['literacy'],
            self.SILVER_FILES['income'],
            self.SILVER_FILES['sanitation'],
        ]
        
        # Smart caching check
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, source_keys)
        if should_skip:
            logger.info(f"⏭️  Skipping consolidated clustering: {reason}")
            return True
        
        # Read silver tables
        df_muni = self._read_silver_parquet(self.SILVER_FILES['municipalities'])
        df_pop = self._read_silver_parquet(self.SILVER_FILES['population'])
        df_lit = self._read_silver_parquet(self.SILVER_FILES['literacy'])
        df_inc = self._read_silver_parquet(self.SILVER_FILES['income'])
        df_san = self._read_silver_parquet(self.SILVER_FILES['sanitation'])
        
        if df_muni is None:
            logger.error("❌ Missing municipalities dimension table")
            return False
        
        # Start with municipalities as base
        result = df_muni.copy()
        
        # Process population data (2010 and 2022)
        if df_pop is not None:
            pop_2010 = df_pop[df_pop['year'] == 2010][['municipality_code', 'total_population']].copy()
            pop_2010.columns = ['municipality_code', 'population_2010']
            
            pop_2022 = df_pop[df_pop['year'] == 2022][['municipality_code', 'total_population']].copy()
            pop_2022.columns = ['municipality_code', 'population_2022']
            
            result = result.merge(pop_2010, on='municipality_code', how='left')
            result = result.merge(pop_2022, on='municipality_code', how='left')
        
        # Process literacy data (2010 and 2022)
        if df_lit is not None:
            lit_2010 = df_lit[df_lit['year'] == 2010][['municipality_code', 'literacy_rate']].copy()
            lit_2010.columns = ['municipality_code', 'literacy_rate_2010']
            
            lit_2022 = df_lit[df_lit['year'] == 2022][['municipality_code', 'literacy_rate']].copy()
            lit_2022.columns = ['municipality_code', 'literacy_rate_2022']
            
            result = result.merge(lit_2010, on='municipality_code', how='left')
            result = result.merge(lit_2022, on='municipality_code', how='left')
        
        # Process income data (2010 and 2022)
        if df_inc is not None:
            inc_2010 = df_inc[
                df_inc['year'] == 2010
            ][['municipality_code', 'avg_income', 'avg_income_real_2022_brl']].copy()
            inc_2010.columns = [
                'municipality_code',
                'avg_income_2010',
                'avg_income_real_2010_2022_brl',
            ]
            
            inc_2022 = df_inc[
                df_inc['year'] == 2022
            ][['municipality_code', 'avg_income', 'avg_income_real_2022_brl']].copy()
            inc_2022.columns = [
                'municipality_code',
                'avg_income_2022',
                'avg_income_real_2022_2022_brl',
            ]
            
            result = result.merge(inc_2010, on='municipality_code', how='left')
            result = result.merge(inc_2022, on='municipality_code', how='left')
        
        # Process sanitation/household data (2010 and 2022)
        if df_san is not None:
            san_2010 = df_san[df_san['year'] == 2010][['municipality_code', 'total_households']].copy()
            san_2010.columns = ['municipality_code', 'households_2010']
            
            san_2022 = df_san[df_san['year'] == 2022][['municipality_code', 'total_households']].copy()
            san_2022.columns = ['municipality_code', 'households_2022']
            
            result = result.merge(san_2010, on='municipality_code', how='left')
            result = result.merge(san_2022, on='municipality_code', how='left')
        
        # Calculate change metrics
        numeric_cols = ['population_2010', 'population_2022', 'literacy_rate_2010', 'literacy_rate_2022',
                       'avg_income_2010', 'avg_income_2022', 'households_2010', 'households_2022']
        
        # Population change percentage
        result['population_change_pct'] = result.apply(
            lambda row: self._calculate_change_pct(row.get('population_2022'), row.get('population_2010')),
            axis=1
        )
        
        # Literacy change (percentage points)
        result['literacy_change_pp'] = result.apply(
            lambda row: round(row['literacy_rate_2022'] - row['literacy_rate_2010'], 2)
            if pd.notna(row.get('literacy_rate_2022')) and pd.notna(row.get('literacy_rate_2010'))
            else None,
            axis=1
        )
        
        # Income change percentage
        result['income_change_pct'] = result.apply(
            lambda row: self._calculate_change_pct(row.get('avg_income_2022'), row.get('avg_income_2010')),
            axis=1
        )
        result['income_change_real_pct'] = result.apply(
            lambda row: self._calculate_change_pct(
                row.get('avg_income_real_2022_2022_brl'),
                row.get('avg_income_real_2010_2022_brl'),
            ),
            axis=1
        )
        
        # Households change percentage
        result['households_change_pct'] = result.apply(
            lambda row: self._calculate_change_pct(row.get('households_2022'), row.get('households_2010')),
            axis=1
        )
        
        # Define clustering feature columns
        clustering_features = [
            'population_2010', 'population_2022', 'population_change_pct',
            'literacy_rate_2010', 'literacy_rate_2022', 'literacy_change_pp',
            'avg_income_real_2010_2022_brl', 'avg_income_real_2022_2022_brl', 'income_change_real_pct',
            'households_2010', 'households_2022', 'households_change_pct'
        ]
        
        # Count records before dropping missing values
        records_before = len(result)
        
        # Drop rows with any missing values in clustering features
        result_complete = result.dropna(subset=clustering_features).copy()
        records_after = len(result_complete)
        
        logger.info(f"📉 Dropped {records_before - records_after} rows with missing values ")
        logger.info(f"   ({records_after}/{records_before} municipalities retained = {100*records_after/records_before:.1f}%)")
        
        # Remove duplicates (should not happen, but ensure)
        result_complete = result_complete.drop_duplicates(subset='municipality_code').reset_index(drop=True)
        
        # Calculate normalized (z-score) features for clustering
        for col in clustering_features:
            col_mean = result_complete[col].mean()
            col_std = result_complete[col].std()
            if col_std > 0:
                result_complete[f'{col}_norm'] = (result_complete[col] - col_mean) / col_std
            else:
                result_complete[f'{col}_norm'] = 0.0
        
        # Add log-transformed features for skewed distributions
        for col in ['population_2022', 'avg_income_2022', 'households_2022']:
            result_complete[f'log_{col}'] = result_complete[col].apply(
                lambda x: round(np.log(x), 4) if pd.notna(x) and x > 0 else None
            )
        
        # Sort by municipality code
        result_complete = result_complete.sort_values('municipality_code').reset_index(drop=True)
        
        # Write output
        success = self._write_gold_parquet(result_complete, output_key)
        self._write_gold_json(result_complete, output_key.replace('.parquet', '.json'))
        
        # Save metadata
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(result_complete),
                'records_dropped': records_before - records_after,
                'clustering_features': clustering_features,
                'normalized_features': [f'{col}_norm' for col in clustering_features]
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('gold_consolidated_clustering', 'SUCCESS' if success else 'FAILED',
                          records_before, len(result_complete), source_keys, output_key)
        
        logger.info(f"✅ Consolidated Clustering: {len(result_complete)} municipalities ready for analysis")
        return success


if __name__ == "__main__":
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "silver_schemas.json"
    
    transformer = GoldTransformer(BUCKET_NAME, str(CONFIG_FILE))
    transformer.transform()
