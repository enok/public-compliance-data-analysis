"""
Data Loader for Gold Layer Analysis

Provides convenient access to Gold layer datasets from S3 for statistical analysis and ML.
"""

import os
import logging
import tempfile
from typing import Optional, Dict, List
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GoldDataLoader:
    """Load Gold layer datasets from S3 for analysis."""
    
    GOLD_DATASETS = {
        'municipality_socioeconomic': 'gold/agg_municipality_socioeconomic/data.parquet',
        'state_summary': 'gold/agg_state_summary/data.parquet',
        'sanctions_summary': 'gold/agg_sanctions_summary/data.parquet',
        'analysis_compliance': 'gold/analysis_compliance/data.parquet',
        'consolidated_clustering': 'gold/consolidated_clustering/data.parquet',
    }
    
    def __init__(self, bucket_name: str, aws_profile: Optional[str] = None, cache_dir: Optional[Path] = None):
        """
        Initialize the data loader.
        
        :param bucket_name: S3 bucket name for the data lake
        :param aws_profile: AWS CLI profile name (optional)
        :param cache_dir: Local cache directory for downloaded files (optional)
        """
        session_kwargs = {}
        if aws_profile:
            session_kwargs['profile_name'] = aws_profile
        
        session = boto3.Session(**session_kwargs)
        self.s3 = session.client('s3')
        self.bucket = bucket_name
        
        if cache_dir is None:
            cache_dir = Path(__file__).resolve().parents[2] / ".cache" / "gold_data"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self._cache = {}
    
    def load_dataset(self, dataset_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """
        Load a Gold layer dataset.
        
        :param dataset_name: Name of the dataset (see GOLD_DATASETS keys)
        :param use_cache: Use local cache if available
        :return: DataFrame or None if not found
        """
        if dataset_name not in self.GOLD_DATASETS:
            logger.error(f"❌ Unknown dataset: {dataset_name}")
            logger.info(f"Available datasets: {list(self.GOLD_DATASETS.keys())}")
            return None
        
        if use_cache and dataset_name in self._cache:
            logger.info(f"📦 Using cached data for {dataset_name}")
            return self._cache[dataset_name].copy()
        
        s3_key = self.GOLD_DATASETS[dataset_name]
        
        try:
            logger.info(f"📥 Loading {dataset_name} from S3: {s3_key}")
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp.write(response['Body'].read())
                tmp_path = tmp.name
            
            df = pd.read_parquet(tmp_path)
            os.unlink(tmp_path)
            
            logger.info(f"✅ Loaded {len(df)} records, {len(df.columns)} columns")
            
            if use_cache:
                self._cache[dataset_name] = df.copy()
            
            return df
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"❌ Dataset not found in S3: {s3_key}")
                logger.info("💡 Run Gold layer transformation first: python -m src.processing.gold_transformer")
            else:
                logger.error(f"❌ S3 error loading {dataset_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Failed to load {dataset_name}: {e}")
            return None
    
    def load_all(self, use_cache: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Load all Gold layer datasets.
        
        :param use_cache: Use local cache if available
        :return: Dictionary of dataset_name -> DataFrame
        """
        logger.info("📚 Loading all Gold layer datasets...")
        datasets = {}
        
        for name in self.GOLD_DATASETS.keys():
            df = self.load_dataset(name, use_cache=use_cache)
            if df is not None:
                datasets[name] = df
        
        logger.info(f"✅ Loaded {len(datasets)}/{len(self.GOLD_DATASETS)} datasets")
        return datasets
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict]:
        """
        Get information about a dataset without loading it.
        
        :param dataset_name: Name of the dataset
        :return: Dictionary with dataset info
        """
        df = self.load_dataset(dataset_name)
        if df is None:
            return None
        
        info = {
            'name': dataset_name,
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'missing_values': df.isnull().sum().to_dict(),
        }
        
        return info
    
    def list_available_datasets(self) -> List[str]:
        """List all available Gold layer datasets."""
        return list(self.GOLD_DATASETS.keys())
    
    def clear_cache(self):
        """Clear the in-memory cache."""
        self._cache.clear()
        logger.info("🗑️ Cache cleared")


def load_analysis_data(bucket_name: str = "enok-mba-thesis-datalake", 
                       aws_profile: str = "mba-thesis") -> pd.DataFrame:
    """
    Convenience function to load the main analysis dataset.
    
    :param bucket_name: S3 bucket name
    :param aws_profile: AWS CLI profile name
    :return: Analysis compliance DataFrame
    """
    loader = GoldDataLoader(bucket_name, aws_profile)
    return loader.load_dataset('analysis_compliance')


if __name__ == "__main__":
    loader = GoldDataLoader("enok-mba-thesis-datalake", aws_profile="mba-thesis")
    
    print("\n📊 Available Gold Layer Datasets:")
    print("-" * 50)
    for dataset in loader.list_available_datasets():
        print(f"  • {dataset}")
    
    print("\n📥 Loading analysis_compliance dataset...")
    df = loader.load_dataset('analysis_compliance')
    
    if df is not None:
        print("\n✅ Dataset loaded successfully!")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")
        print(f"\n📋 Column names:")
        for col in df.columns:
            print(f"   • {col}")
        print(f"\n📊 First few rows:")
        print(df.head())
