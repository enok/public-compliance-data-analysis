"""
Processing module for Silver and Gold layer transformations.

This module contains transformers that convert:
- Bronze layer data into normalized, cleaned Silver layer tables
- Silver layer data into analysis-ready Gold layer aggregations
"""

from src.processing.base_transformer import BaseTransformer
from src.processing.ibge_transformer import IBGETransformer
from src.processing.transparency_transformer import TransparencyTransformer
from src.processing.gold_transformer import GoldTransformer

__all__ = [
    'BaseTransformer',
    'IBGETransformer',
    'TransparencyTransformer',
    'GoldTransformer',
]
