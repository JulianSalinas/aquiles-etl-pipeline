"""
Test suite for the new ETL pipeline functionality.
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
        assert result["id"] == 123
    
    @patch('core.etl_orchestrator.Session')
    def test_get_or_create_unit_of_measure_exists(self, mock_session):
        """Test getting existing unit of measure."""
        mock_engine = Mock()
        mock_session_instance = Mock()
        mock_session.return_value.__enter__ = Mock(return_value=mock_session_instance)
        mock_session.return_value.__exit__ = Mock(return_value=None)
        
        # Mock existing unit of measure
        mock_unit = Mock()
        mock_unit.Id = 5
        mock_session_instance.query.return_value.filter.return_value.first.return_value = mock_unit
        
        result = get_or_create_unit_of_measure(mock_engine, "kg")
        
        assert result == 5
    
    def test_get_or_create_unit_of_measure_none(self):
        """Test with None unit acronym."""
        mock_engine = Mock()
        
        result = get_or_create_unit_of_measure(mock_engine, None)
        
        assert result is None
    
    @patch('core.etl_orchestrator.read_from_products_step1')
    def test_normalize_to_staging_tables_empty_data(self, mock_read_products):
        """Test normalization with empty ProductsStep1 data."""
        mock_read_products.return_value = pd.DataFrame()  # Empty DataFrame
        mock_engine = Mock()
        
        result = normalize_to_staging_tables(mock_engine, "test-batch-guid")
        
        assert result["providers"] == 0
        assert result["products"] == 0
        assert result["provider_products"] == 0
    
    @patch('core.etl_orchestrator.os.environ.get')
    @patch('core.etl_orchestrator.extract_invoice_data_from_image')
    def test_extract_invoice_data_with_openai_no_config(self, mock_fallback, mock_env):
        """Test OpenAI extraction falls back to mock when no config."""
        mock_env.side_effect = lambda key: None  # No environment variables set
        mock_fallback.return_value = [{"Producto": "Test Product"}]
        
        result = extract_invoice_data_with_openai(b"fake_image_data", "test.jpg")
        
        assert result == [{"Producto": "Test Product"}]
        mock_fallback.assert_called_once()
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.normalize_to_staging_tables')
    @patch('core.etl_orchestrator.merge_staging_to_fact_tables')
    def test_process_from_products_step1_success(self, mock_merge, mock_normalize, 
                                                mock_ensure_conn, 
                                                mock_create_engine):
        """Test successful processing from ProductsStep1."""
        mock_ensure_conn.return_value = True
        mock_normalize.return_value = {"providers": 2, "products": 5, "provider_products": 10}
        
        result = process_from_products_step1("test-server", "test-db")
        
        assert result["status"] is True
        assert "batch_guid" in result
        assert result["staging_summary"]["providers"] == 2
        assert result["staging_summary"]["products"] == 5
        assert result["staging_summary"]["provider_products"] == 10
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    def test_process_from_products_step1_connection_failure(self, mock_ensure_conn, mock_create_engine):
        """Test processing fails when database connection cannot be established."""
        mock_ensure_conn.return_value = None  # Connection failed
        
        result = process_from_products_step1("test-server", "test-db")
        
        assert result["status"] is False
        assert "Failed to establish connection" in result["message"]


class TestDataValidation:
    """Test data validation and edge cases."""
    
    def test_batch_guid_format(self):
        """Test that batch GUIDs are properly formatted."""
        import uuid
        from core.etl_orchestrator import process_from_products_step1
        
        # Mock the functions to avoid actual database calls
        with patch('core.etl_orchestrator.create_azure_sql_engine'), \
             patch('core.etl_orchestrator.ensure_connection_established', return_value=True), \
             patch('core.etl_orchestrator.normalize_to_staging_tables', 
                   return_value={"providers": 1, "products": 1, "provider_products": 1}), \
             patch('core.etl_orchestrator.merge_staging_to_fact_tables'):
            
            result = process_from_products_step1("test-server", "test-db")
            
            # Verify batch_guid is a valid UUID
            batch_guid = result["batch_guid"]
            uuid.UUID(batch_guid)  # This will raise ValueError if not valid UUID
            assert len(batch_guid) == 36  # Standard UUID string length
    
    @patch('core.etl_orchestrator.read_from_products_step1')
    def test_error_handling_in_normalization(self, mock_read_products):
        """Test error handling in normalization process."""
        mock_engine = Mock()
        mock_read_products.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            normalize_to_staging_tables(mock_engine, "test-batch-guid")

    def test_normalize_to_staging_tables_from_dataframe_empty(self):
        """Test normalization with empty DataFrame."""
        mock_engine = Mock()
        empty_df = pd.DataFrame()
        
        result = normalize_to_staging_tables_from_dataframe(mock_engine, empty_df, "test-guid")
        
        assert result["providers"] == 0
        assert result["products"] == 0
        assert result["provider_products"] == 0

    @patch('core.etl_orchestrator.get_or_create_unit_of_measure')
    def test_normalize_to_staging_tables_from_dataframe_with_data(self, mock_get_unit):
        """Test normalization with actual DataFrame data."""
        mock_engine = Mock()
        mock_get_unit.return_value = 1
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'CleanProviderName': ['Provider A', 'Provider B', 'Provider A'],
            'CleanDescription': ['Product 1', 'Product 2', 'Product 3'],
            'RawDescription': ['Raw Prod 1', 'Raw Prod 2', 'Raw Prod 3'],
            'CleanPrice': [100.0, 200.0, 150.0],
            'Measure': [1.0, 2.0, 1.5],
            'UnitOfMeasure': ['kg', 'g', 'ml'],
            'CleanLastReviewDt': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'PackageUnits': [1, 2, 1],
            'PercentageIVA': [19, 19, 21]
        })
        
        # Mock pandas to_sql to avoid actual database operations
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            result = normalize_to_staging_tables_from_dataframe(mock_engine, test_df, "test-guid")
            
            # Should have 2 unique providers
            assert result["providers"] == 2
            assert result["products"] == 3
            assert result["provider_products"] == 3
            
            # Verify to_sql was called for each staging table
            assert mock_to_sql.call_count == 3  # Provider, Product, Provider_Product

    @patch('core.etl_orchestrator.extract_invoice_data_with_openai')
    @patch('core.etl_orchestrator.apply_transformations')
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.normalize_to_staging_tables_from_dataframe')
    @patch('core.etl_orchestrator.merge_staging_to_fact_tables')
    def test_process_invoice_image_direct(self, mock_merge, mock_normalize,
                                        mock_ensure_conn, mock_create_engine, mock_apply_transforms,
                                        mock_extract):
        """Test direct invoice processing without CSV intermediate storage."""
        
        # Setup mocks
        mock_extract.return_value = [
            {"Producto": "Test Product", "Provedor": "Test Provider", "Precio": "100.00", "Porcentaje de IVA": "19"}
        ]
        mock_apply_transforms.return_value = pd.DataFrame({
            'CleanDescription': ['Test Product'],
            'CleanProviderName': ['Test Provider']
        })
        mock_ensure_conn.return_value = True
        mock_normalize.return_value = {"providers": 1, "products": 1, "provider_products": 1}
        
        # Execute direct processing
        result = process_invoice_image_direct(b"fake_image_data", "test.jpg", "test-server", "test-db")
        
        # Verify success
        assert result["status"] is True
        assert "Direct invoice processing completed successfully" in result["message"]
        assert result["products_extracted"] == 1
        assert result["rows_processed"] == 1
        
        # Verify all steps were called
        mock_extract.assert_called_once()
        mock_apply_transforms.assert_called_once()
        mock_normalize.assert_called_once()
        mock_merge.assert_called_once()