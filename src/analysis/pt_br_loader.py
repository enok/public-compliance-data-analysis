"""
Portuguese Data Loader - Wrapper for GoldDataLoader with Portuguese column names.

This module provides a convenience wrapper around GoldDataLoader that automatically
translates column names to Portuguese for use in Portuguese-language notebooks.

Usage:
    from src.analysis.pt_br_loader import GoldDataLoaderPtBr
    
    loader = GoldDataLoaderPtBr(
        bucket_name="enok-mba-thesis-datalake",
        aws_profile="mba-thesis"
    )
    
    # Load all datasets with Portuguese column names
    datasets = loader.load_all()
    
    # Access datasets
    df_municipio = datasets.get('municipio_socioeconomico')
    df_estado = datasets.get('resumo_estado')
    df_sancoes = datasets.get('resumo_sancoes')
    df_analise = datasets.get('analise_compliance')
    df_analise_municipio = datasets.get('analise_compliance_municipio')
"""

import logging
from typing import Dict, Optional
import pandas as pd

from src.analysis.local_data_loader import LocalGoldDataLoader
from src.config.pt_br_translations import (
    DATASET_TRANSLATIONS,
    COLUMN_TRANSLATIONS,
    DISPLAY_NAME_TRANSLATIONS,
    translate_dataframe_columns
)

logger = logging.getLogger(__name__)


class GoldDataLoaderPtBr(LocalGoldDataLoader):
    """
    Portuguese wrapper for LocalGoldDataLoader.
    
    Automatically translates dataset and column names to Portuguese.
    """
    
    def __init__(self, bucket_name: Optional[str] = None, aws_profile: Optional[str] = None,
                 use_display_names: bool = False, project_root = None):
        """
        Initialize Portuguese data loader.
        
        Args:
            bucket_name: S3 bucket name (optional, for backwards compatibility)
            aws_profile: AWS profile name (optional, for backwards compatibility)
            use_display_names: If True, use display names (e.g., "Nome do Estado")
                             If False, use code-friendly names (e.g., "nome_estado")
            project_root: Project root directory for local loading
        """
        super().__init__(project_root=project_root)
        self.use_display_names = use_display_names
        logger.info(f"📚 Portuguese loader initialized (display_names={use_display_names})")
    
    def list_available_datasets(self) -> list:
        """
        List available datasets with Portuguese names.
        
        Returns:
            List of Portuguese dataset names
        """
        english_datasets = super().list_available_datasets()
        return [DATASET_TRANSLATIONS.get(ds, ds) for ds in english_datasets]
    
    def load_dataset(self, dataset_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """
        Load a single dataset and translate column names to Portuguese.
        
        Args:
            dataset_name: Dataset name (English or Portuguese)
            use_cache: Whether to use cached data (passed to parent class)
            
        Returns:
            DataFrame with Portuguese column names, or None if not found
        """
        # Check if it's already a Portuguese name, convert to English
        from src.config.pt_br_translations import DATASET_TRANSLATIONS_REVERSE
        english_name = DATASET_TRANSLATIONS_REVERSE.get(dataset_name, dataset_name)
        
        # Load using parent class
        df = super().load_dataset(english_name, use_cache=use_cache)
        
        if df is not None:
            # Translate column names
            df = translate_dataframe_columns(df, display=self.use_display_names)
            logger.info(f"✅ Loaded '{dataset_name}' with {len(df.columns)} Portuguese columns")
        
        return df
    
    def load_all(self) -> Dict[str, pd.DataFrame]:
        """
        Load all available datasets with Portuguese names and columns.
        
        Returns:
            Dictionary mapping Portuguese dataset names to DataFrames
        """
        logger.info("📚 Loading all Gold layer datasets (Portuguese)...")
        
        # Load using parent class
        english_datasets = super().load_all()
        
        # Translate dataset names and column names
        portuguese_datasets = {}
        for eng_name, df in english_datasets.items():
            pt_name = DATASET_TRANSLATIONS.get(eng_name, eng_name)
            df_translated = translate_dataframe_columns(df, display=self.use_display_names)
            portuguese_datasets[pt_name] = df_translated
            logger.info(f"✅ '{pt_name}': {len(df_translated)} registros, {len(df_translated.columns)} colunas")
        
        logger.info(f"✅ Carregados {len(portuguese_datasets)}/{len(self.GOLD_DATASETS)} datasets")
        return portuguese_datasets
    
    def get_column_mapping(self) -> Dict[str, str]:
        """
        Get the current column name mapping (English → Portuguese).
        
        Returns:
            Dictionary of column translations
        """
        return DISPLAY_NAME_TRANSLATIONS if self.use_display_names else COLUMN_TRANSLATIONS
    
    def get_dataset_mapping(self) -> Dict[str, str]:
        """
        Get the dataset name mapping (English → Portuguese).
        
        Returns:
            Dictionary of dataset translations
        """
        return DATASET_TRANSLATIONS


def load_gold_data_pt(bucket_name: str = "enok-mba-thesis-datalake",
                      aws_profile: Optional[str] = "mba-thesis",
                      use_display_names: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to load all Gold layer data with Portuguese names.
    
    Args:
        bucket_name: S3 bucket name
        aws_profile: AWS profile name
        use_display_names: Use display names instead of code-friendly names
        
    Returns:
        Dictionary of DataFrames with Portuguese names
        
    Example:
        datasets = load_gold_data_pt()
        df_analise = datasets['analise_compliance']
    """
    loader = GoldDataLoaderPtBr(bucket_name, aws_profile, use_display_names)
    return loader.load_all()


if __name__ == "__main__":
    # Example usage
    print("Portuguese Data Loader Example")
    print("=" * 60)
    
    loader = GoldDataLoaderPtBr(
        bucket_name="enok-mba-thesis-datalake",
        aws_profile="mba-thesis"
    )
    
    print("\nDatasets disponíveis:")
    for dataset in loader.list_available_datasets():
        print(f"  • {dataset}")
    
    print("\nMapeamento de colunas:")
    for eng, pt in list(loader.get_column_mapping().items())[:10]:
        print(f"  {eng:30} → {pt}")
