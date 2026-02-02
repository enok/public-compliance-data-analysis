"""
Base Transformer for Silver Layer Processing

This module provides the abstract base class for all Bronze → Silver transformations.
Handles S3 I/O, schema validation, and common transformation utilities.
"""

import os
import json
import logging
import hashlib
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for Bronze → Silver data transformations."""

    def __init__(self, bucket_name: str, schema_config_path: str):
        """
        Initialize the transformer.
        
        :param bucket_name: S3 bucket name for the data lake.
        :param schema_config_path: Path to silver_schemas.json config file.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.schema_config = self._load_schema_config(schema_config_path)
        
        # State and region mappings from config
        self.state_mapping = self.schema_config.get('state_mapping', {})
        self.region_mapping = self.schema_config.get('region_mapping', {})
        
        # Build reverse lookup: state_code -> region_code
        self.state_to_region = {}
        for region_code, region_info in self.region_mapping.items():
            for state_code in region_info.get('states', []):
                self.state_to_region[state_code] = region_code
        
        # Processing log for thesis documentation
        self.processing_log = Path(__file__).parent.parent.parent / "docs" / "processing.log"
        os.makedirs(self.processing_log.parent, exist_ok=True)

    def _load_schema_config(self, path: str) -> Dict:
        """Load the silver schemas configuration."""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _read_bronze_json(self, s3_key: str) -> Optional[List[Dict]]:
        """
        Read a JSON file from the Bronze layer in S3.
        
        :param s3_key: S3 key for the bronze file (e.g., 'bronze/ibge/census_2010_pop.json')
        :return: Parsed JSON data as list of dicts, or None if not found.
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            logger.info(f"📥 Read {len(data)} records from {s3_key}")
            return data
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"⚠️ Bronze file not found: {s3_key}")
                return None
            raise

    def _write_silver_parquet(self, df: pd.DataFrame, s3_key: str, partition_cols: Optional[List[str]] = None) -> bool:
        """
        Write a DataFrame to the Silver layer as Parquet.
        
        :param df: DataFrame to write.
        :param s3_key: S3 key for the silver file (e.g., 'silver/fact_population/data.parquet')
        :param partition_cols: Optional columns to partition by.
        :return: True if successful, False otherwise.
        """
        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp_path = tmp.name
            
            df.to_parquet(tmp_path, index=False, engine='pyarrow')
            
            with open(tmp_path, 'rb') as f:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType='application/octet-stream'
                )
            
            os.unlink(tmp_path)
            logger.info(f"📤 Wrote {len(df)} records to {s3_key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to write Parquet to {s3_key}: {e}")
            return False

    def _write_silver_json(self, df: pd.DataFrame, s3_key: str) -> bool:
        """
        Write a DataFrame to the Silver layer as JSON (for compatibility/debugging).
        
        :param df: DataFrame to write.
        :param s3_key: S3 key for the silver file.
        :return: True if successful, False otherwise.
        """
        try:
            content = df.to_json(orient='records', force_ascii=False)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content.encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )
            logger.info(f"📤 Wrote {len(df)} records to {s3_key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to write JSON to {s3_key}: {e}")
            return False

    def _calculate_md5(self, content: str) -> str:
        """Calculate MD5 hash of content."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _get_silver_metadata(self, metadata_key: str) -> Optional[Dict]:
        """
        Retrieve silver layer metadata from S3.
        
        Metadata tracks:
        - source_files: dict of {bronze_key: md5_hash}
        - last_updated: timestamp
        - record_count: number of records in silver output
        
        :param metadata_key: S3 key for metadata file
        :return: Metadata dict or None if not found
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return None
    
    def _save_silver_metadata(self, metadata_key: str, metadata: Dict):
        """
        Save silver layer metadata to S3.
        
        :param metadata_key: S3 key for metadata file
        :param metadata: Metadata dict to save
        """
        self.s3.put_object(
            Bucket=self.bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json; charset=utf-8'
        )
        logger.info(f"💾 Saved silver metadata: {metadata_key}")
    
    def _get_bronze_file_hash(self, s3_key: str) -> Optional[str]:
        """
        Get MD5 hash of a bronze file from S3 ETag.
        
        :param s3_key: S3 key for bronze file
        :return: MD5 hash or None if file doesn't exist
        """
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            # ETag is MD5 hash for single-part uploads
            etag = response['ETag'].strip('"')
            return etag
        except ClientError:
            return None
    
    def _check_sources_changed(self, metadata_key: str, source_keys: List[str]) -> tuple[bool, List[str]]:
        """
        Check if any source files have changed since last processing.
        
        :param metadata_key: S3 key for silver metadata
        :param source_keys: List of bronze S3 keys to check
        :return: (has_changes, list of changed/new files)
        """
        existing_metadata = self._get_silver_metadata(metadata_key)
        
        if not existing_metadata:
            # No metadata = first run, all files are "new"
            return True, source_keys
        
        tracked_sources = existing_metadata.get('source_files', {})
        changed_files = []
        
        for s3_key in source_keys:
            current_hash = self._get_bronze_file_hash(s3_key)
            
            if not current_hash:
                # File doesn't exist, skip it
                continue
            
            tracked_hash = tracked_sources.get(s3_key)
            
            if tracked_hash != current_hash:
                # File is new or changed
                changed_files.append(s3_key)
        
        # Also check for new files not in tracked sources
        for s3_key in source_keys:
            if s3_key not in tracked_sources and self._get_bronze_file_hash(s3_key):
                if s3_key not in changed_files:
                    changed_files.append(s3_key)
        
        has_changes = len(changed_files) > 0
        return has_changes, changed_files
    
    def _should_skip_processing(self, output_key: str, metadata_key: str, source_keys: List[str]) -> tuple[bool, str]:
        """
        Smart check: skip processing only if output exists AND no source files changed.
        
        :param output_key: S3 key for silver output
        :param metadata_key: S3 key for silver metadata
        :param source_keys: List of bronze source file keys
        :return: (should_skip, reason)
        """
        # Check if output exists
        try:
            self.s3.head_object(Bucket=self.bucket, Key=output_key)
        except ClientError:
            # Output doesn't exist, must process
            return False, "output does not exist"
        
        # Output exists, check if sources changed
        has_changes, changed_files = self._check_sources_changed(metadata_key, source_keys)
        
        if not has_changes:
            return True, "output exists and no source files changed"
        
        # Log what changed
        logger.info(f"🔄 Reprocessing - {len(changed_files)} source file(s) changed or new:")
        for f in changed_files[:5]:  # Show first 5
            logger.info(f"   - {f}")
        if len(changed_files) > 5:
            logger.info(f"   ... and {len(changed_files) - 5} more")
        
        return False, f"{len(changed_files)} source file(s) changed"

    def _extract_municipality_code(self, raw_code: Any) -> Optional[str]:
        """
        Extract and validate 7-digit IBGE municipality code.
        
        IBGE codes are 7 digits: UUFFMMMM where:
        - UU: State code (11-53)
        - FF: Mesoregion code
        - MMMM: Municipality code within mesoregion
        
        :param raw_code: Raw code value from source data.
        :return: 7-digit string code or None if invalid.
        """
        if raw_code is None:
            return None
        
        code_str = str(raw_code).strip()
        
        # Remove any decimal points (e.g., "1100015.0" -> "1100015")
        if '.' in code_str:
            code_str = code_str.split('.')[0]
        
        # Validate length
        if len(code_str) != 7:
            return None
        
        # Validate it's numeric
        if not code_str.isdigit():
            return None
        
        # Validate state code is valid (first 2 digits)
        state_code = code_str[:2]
        if state_code not in self.state_mapping:
            return None
        
        return code_str

    def _extract_state_code(self, municipality_code: str) -> str:
        """Extract 2-digit state code from municipality code."""
        return municipality_code[:2] if municipality_code else None

    def _get_state_name(self, state_code: str) -> str:
        """Get state name from code."""
        return self.state_mapping.get(state_code, "Unknown")

    def _get_region_code(self, state_code: str) -> str:
        """Get region code from state code."""
        return self.state_to_region.get(state_code, "0")

    def _get_region_name(self, region_code: str) -> str:
        """Get region name from code."""
        region_info = self.region_mapping.get(region_code, {})
        return region_info.get('name', "Unknown")

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert a value to int, returning None for invalid values."""
        if value is None or value == '' or value == '-':
            return None
        try:
            # Handle string numbers with commas or spaces
            if isinstance(value, str):
                value = value.replace(',', '').replace(' ', '').strip()
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert a value to float, returning None for invalid values."""
        if value is None or value == '' or value == '-':
            return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '.').replace(' ', '').strip()
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str: Any, formats: List[str] = None) -> Optional[datetime]:
        """
        Parse a date string into datetime object.
        
        :param date_str: Date string to parse.
        :param formats: List of date formats to try.
        :return: datetime object or None if parsing fails.
        """
        if date_str is None or date_str == '':
            return None
        
        if formats is None:
            formats = [
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%d/%m/%Y %H:%M:%S'
            ]
        
        date_str = str(date_str).strip()
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None

    def log_processing(self, dataset_name: str, status: str, records_in: int, records_out: int, 
                       source_keys: List[str], output_key: str, error_msg: str = None):
        """
        Log processing results for thesis documentation.
        
        :param dataset_name: Name of the dataset being processed.
        :param status: Processing status (SUCCESS, FAILED, SKIPPED).
        :param records_in: Number of input records.
        :param records_out: Number of output records.
        :param source_keys: List of source S3 keys.
        :param output_key: Output S3 key.
        :param error_msg: Optional error message.
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        with open(self.processing_log, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] Silver Transformation: {dataset_name}\n")
            f.write(f"Status: {status}\n")
            f.write(f"Source(s): {', '.join(source_keys)}\n")
            f.write(f"Output: {output_key}\n")
            f.write(f"Records In: {records_in}\n")
            f.write(f"Records Out: {records_out}\n")
            if error_msg:
                f.write(f"Error: {error_msg}\n")
            f.write("-" * 50 + "\n")

    def validate_schema(self, df: pd.DataFrame, schema_name: str) -> pd.DataFrame:
        """
        Validate and enforce schema on DataFrame.
        
        :param df: DataFrame to validate.
        :param schema_name: Schema name from silver_schemas.json.
        :return: DataFrame with validated schema.
        """
        schema = self.schema_config.get('schemas', {}).get(schema_name, {})
        columns = schema.get('columns', {})
        
        if not columns:
            logger.warning(f"⚠️ No schema defined for {schema_name}")
            return df
        
        # Ensure required columns exist
        for col_name, col_spec in columns.items():
            if col_name not in df.columns:
                nullable = col_spec.get('nullable', False)
                if nullable:
                    df[col_name] = None
                else:
                    logger.warning(f"⚠️ Required column {col_name} missing from {schema_name}")
        
        # Select only defined columns (in order)
        existing_cols = [c for c in columns.keys() if c in df.columns]
        df = df[existing_cols]
        
        # Apply type conversions
        for col_name in existing_cols:
            col_type = columns[col_name].get('type', 'string')
            try:
                if col_type == 'integer':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                elif col_type == 'float':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                elif col_type == 'date':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                elif col_type == 'string':
                    df[col_name] = df[col_name].astype(str).replace('nan', None).replace('None', None)
            except Exception as e:
                logger.warning(f"⚠️ Type conversion failed for {col_name}: {e}")
        
        return df

    @abstractmethod
    def transform(self) -> bool:
        """
        Execute the transformation from Bronze to Silver.
        
        Must be implemented by subclasses.
        
        :return: True if transformation succeeded, False otherwise.
        """
        pass

    @abstractmethod
    def get_source_datasets(self) -> List[str]:
        """
        Return list of source dataset names this transformer processes.
        
        Must be implemented by subclasses.
        """
        pass
