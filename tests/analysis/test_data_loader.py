"""
Unit tests for Gold layer data loader.
"""

import pytest
from unittest.mock import patch, MagicMock


class TestGoldDataLoader:
    """Tests for GoldDataLoader (mocked S3)."""

    @pytest.fixture
    def loader(self):
        with patch('boto3.Session') as mock_session_cls:
            mock_session = MagicMock()
            mock_session.client.return_value = MagicMock()
            mock_session_cls.return_value = mock_session

            from src.analysis.data_loader import GoldDataLoader
            return GoldDataLoader(bucket_name="test-bucket")

    def test_gold_datasets_defined(self, loader):
        """All expected Gold datasets are registered."""
        expected = [
            'municipality_socioeconomic',
            'state_summary',
            'sanctions_summary',
            'analysis_compliance',
            'consolidated_clustering',
        ]
        for name in expected:
            assert name in loader.GOLD_DATASETS, f"Missing dataset: {name}"

    def test_list_available_datasets(self, loader):
        """list_available_datasets returns the right keys."""
        datasets = loader.list_available_datasets()
        assert len(datasets) >= 5
        assert 'analysis_compliance' in datasets

    def test_load_unknown_dataset_returns_none(self, loader):
        """Loading an unknown dataset returns None."""
        result = loader.load_dataset('nonexistent_dataset')
        assert result is None

    def test_clear_cache(self, loader):
        """clear_cache empties the in-memory cache."""
        loader._cache['fake'] = 'data'
        loader.clear_cache()
        assert len(loader._cache) == 0
