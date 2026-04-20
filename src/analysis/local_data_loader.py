"""Local Data Loader for Gold Layer Analysis (No AWS Required)

Provides convenient access to Gold layer datasets from local filesystem.
"""

import logging
from typing import Optional, Dict
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LocalGoldDataLoader:
    """Load Gold layer datasets from local filesystem for analysis."""
    
    GOLD_DATASETS = {
        'municipality_socioeconomic': 'data/gold/agg_municipality_socioeconomic/data.parquet',
        'state_summary': 'data/gold/agg_state_summary/data.parquet',
        'sanctions_summary': 'data/gold/agg_sanctions_summary/data.parquet',
        'analysis_compliance': 'data/gold/analysis_compliance/data.parquet',
        'analysis_compliance_municipality': 'data/gold/analysis_compliance_municipality/data.parquet',
        'consolidated_clustering': 'data/gold/consolidated_clustering/data.parquet',
    }
    
    def __init__(self, project_root: Optional[Path] = None):
        """
        Initialize the local data loader.
        
        :param project_root: Project root directory (defaults to grandparent of this file)
        """
        if project_root is None:
            project_root = Path(__file__).resolve().parents[2]
        self.project_root = Path(project_root)
        self._cache = {}
    
    def load_dataset(self, dataset_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """
        Load a Gold layer dataset from local filesystem.
        
        :param dataset_name: Name of the dataset (see GOLD_DATASETS keys)
        :param use_cache: Use memory cache if available
        :return: DataFrame or None if not found
        """
        if dataset_name not in self.GOLD_DATASETS:
            logger.error(f"Unknown dataset: {dataset_name}")
            logger.info(f"Available datasets: {list(self.GOLD_DATASETS.keys())}")
            return None
        
        if use_cache and dataset_name in self._cache:
            logger.info(f"Using cached data for {dataset_name}")
            return self._cache[dataset_name].copy()
        
        rel_path = self.GOLD_DATASETS[dataset_name]
        file_path = self.project_root / rel_path
        
        if not file_path.exists():
            rel_display = Path(rel_path).as_posix()
            logger.error(f"Dataset file not found: {rel_display}")
            return None
        
        try:
            # Show relative path for cleaner output
            rel_display = Path(rel_path).as_posix()
            logger.info(f"Loading {dataset_name} from {rel_display}")
            # Use pandas read_parquet with numpy_nullable backend to avoid Arrow-backed
            # extension types that cause "pandas.period already defined" errors when
            # loading multiple parquet files in the same session
            df = pd.read_parquet(file_path, engine='pyarrow', dtype_backend='numpy_nullable')
            logger.info(f"Loaded {len(df)} rows for {dataset_name}")

            if use_cache:
                self._cache[dataset_name] = df.copy()

            return df
        except Exception as e:
            logger.error(f"Error loading {dataset_name}: {e}")
            return None
    
    def load_all(self, use_cache: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Load all available Gold layer datasets.
        
        :param use_cache: Use memory cache if available
        :return: Dictionary of dataset_name -> DataFrame
        """
        results = {}
        for dataset_name in self.GOLD_DATASETS.keys():
            df = self.load_dataset(dataset_name, use_cache=use_cache)
            if df is not None:
                results[dataset_name] = df
        return results
    
    def list_available_datasets(self) -> list:
        """Return list of available dataset names."""
        available = []
        for name, rel_path in self.GOLD_DATASETS.items():
            file_path = self.project_root / rel_path
            if file_path.exists():
                available.append(name)
        return available


# Backwards compatibility alias
GoldDataLoader = LocalGoldDataLoader
