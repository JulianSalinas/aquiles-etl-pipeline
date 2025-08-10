"""
Test suite for the ETL pipeline functionality.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from core.etl_orchestrator import (
    check_process_file_status,
    normalize_to_staging_tables_from_dataframe,
    extract_invoice_data_with_openai,
    process_invoice_image
)


class TestETLPipeline:
    """Test cases for ETL pipeline functionality."""
    
    @patch('core.etl_orchestrator.Session')
    def test_check_process_file_status_not_exists(self, mock_session):
        """Test checking ProcessFile status when file doesn't exist."""
        mock_engine = Mock()
        mock_session_instance = Mock()
        mock_session.return_value.__enter__ = Mock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = Mock(return_value=None)
        mock_session_instance.query.return_value.filter.return_value.filter.return_value.first.return_value = None
        
        result = check_process_file_status(mock_engine, "test-container", "test-file.csv")
        
        assert result == 1  # Returns 1 when file doesn't exist
    
    @patch('core.etl_orchestrator.Session')
    def test_check_process_file_status_exists(self, mock_session):
        """Test checking ProcessFile status when file exists."""
        mock_engine = Mock()
        mock_session_instance = Mock()
        mock_session.return_value.__enter__ = Mock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = Mock(return_value=None)
        
        # Mock ProcessFile object
        mock_process_file = Mock()
        mock_process_file.StatusId = 3
        mock_session_instance.query.return_value.filter.return_value.filter.return_value.first.return_value = mock_process_file
        
        result = check_process_file_status(mock_engine, "test-container", "test-file.csv")
        
        assert result == 3  # Returns the StatusId when file exists
    
    def test_normalize_to_staging_tables_from_dataframe_empty(self):
        """Test normalization with empty DataFrame."""
        mock_engine = Mock()
        empty_df = pd.DataFrame()
        
        # Should not raise an exception with empty DataFrame
        normalize_to_staging_tables_from_dataframe(mock_engine, empty_df, "test-batch-guid")
    
    @patch('core.etl_orchestrator.get_units_of_measure_df')
    @patch('core.etl_orchestrator.get_provider_synonyms_df')
    def test_normalize_to_staging_tables_from_dataframe_with_data(self, mock_get_providers, mock_get_units):
        """Test normalization with actual data."""
        # Mock the data dependencies
        mock_get_providers.return_value = pd.DataFrame()
        mock_get_units.return_value = pd.DataFrame()
        
        mock_engine = Mock()
        test_df = pd.DataFrame({
            'CleanProviderName': ['Provider A'],
            'CleanDescription': ['Product A'],
            'RawDescription': ['Raw Product A'],
            'CleanPrice': [100.0],
            'Measure': ['500'],
            'UnitOfMeasure': ['g'],
            'CleanLastReviewDt': ['2024-01-01'],
            'PackageUnits': ['1'],
            'PercentageIVA': [19]
        })
        
        # Mock the to_sql method
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            normalize_to_staging_tables_from_dataframe(mock_engine, test_df, "test-batch-guid")
            
            # Verify that to_sql was called (indicating data was written)
            assert mock_to_sql.call_count >= 2  # Should be called for providers and products at minimum


class TestOpenAIIntegration:
    """Test cases for OpenAI integration."""
    
    def test_extract_invoice_data_with_openai_missing_config(self):
        """Test error handling when OpenAI configuration is missing."""
        # Clear environment variables
        import os
        if 'AZURE_OPENAI_ENDPOINT' in os.environ:
            del os.environ['AZURE_OPENAI_ENDPOINT']
        if 'AZURE_OPENAI_KEY' in os.environ:
            del os.environ['AZURE_OPENAI_KEY']
        
        image_content = b"mock_image_data"
        image_name = "test.jpg"
        
        with pytest.raises(ValueError, match="Azure OpenAI configuration not found"):
            extract_invoice_data_with_openai(image_content, image_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])