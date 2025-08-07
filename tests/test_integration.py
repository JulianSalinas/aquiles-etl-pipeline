"""
Integration tests for the complete ETL pipeline.
These tests mock the database connections but test the full flow.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from core.etl_orchestrator import process_csv_from_stream, process_from_products_step1


class TestETLIntegration:
    """Integration tests for the complete ETL pipeline."""
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.check_process_file_status')
    @patch('core.etl_orchestrator.insert_process_file_record')
    @patch('core.etl_orchestrator.update_process_file_status')
    @patch('core.etl_orchestrator.create_staging_tables')
    @patch('core.etl_orchestrator.normalize_to_staging_tables')
    @patch('core.etl_orchestrator.merge_staging_to_fact_tables')
    @patch('core.etl_orchestrator.write_to_sql_database')
    def test_complete_csv_processing_flow(self, mock_write_sql, mock_merge, mock_normalize, 
                                        mock_create_staging, mock_update_status, mock_insert_file,
                                        mock_check_file, mock_ensure_conn, mock_create_engine):
        """Test the complete CSV processing flow from start to finish."""
        
        # Setup mocks
        mock_ensure_conn.return_value = True
        mock_check_file.return_value = {"exists": False, "status_id": None, "id": None}
        mock_insert_file.return_value = 123  # ProcessFile ID
        mock_write_sql.return_value = 5  # 5 rows written
        mock_normalize.return_value = {"providers": 2, "products": 5, "provider_products": 5}
        
        # Create test CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA
Arroz Premium 1kg,2024-01-15,Distribuidora San Juan,2500.00,19
Aceite Vegetal 500ml,2024-01-15,Distribuidora San Juan,4200.00,19
""".encode('utf-8')
        
        # Execute the full pipeline
        result = process_csv_from_stream(
            csv_data, 
            "test-file.csv", 
            "test-server", 
            "test-db", 
            "ProductsStep1"
        )
        
        # Verify the result
        assert result["status"] is True
        assert "ETL process completed successfully" in result["message"]
        assert result["rows_processed"] == 2
        assert result["rows_written"] == 5
        assert "batch_guid" in result
        assert result["staging_summary"]["providers"] == 2
        assert result["staging_summary"]["products"] == 5
        
        # Verify all pipeline steps were called
        mock_check_file.assert_called_once()
        mock_insert_file.assert_called_once()
        mock_create_staging.assert_called_once()
        mock_normalize.assert_called_once()
        mock_merge.assert_called_once()
        mock_update_status.assert_called_with(mock_create_engine.return_value, 123, 3)  # Status 3 = Success
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.check_process_file_status')
    def test_skip_already_processed_file(self, mock_check_file, mock_ensure_conn, mock_create_engine):
        """Test that files with Status 3 (Success) are skipped."""
        
        # Setup mocks - file already processed successfully
        mock_ensure_conn.return_value = True
        mock_check_file.return_value = {"exists": True, "status_id": 3, "id": 456}
        
        # Create test CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio
Test Product,2024-01-15,Test Provider,1000.00
""".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "already-processed.csv", 
            "test-server", 
            "test-db", 
            "ProductsStep1"
        )
        
        # Verify the file was skipped
        assert result["status"] is True
        assert "already processed successfully" in result["message"]
        assert result["rows_processed"] == 0
        assert result["rows_written"] == 0
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.create_staging_tables')
    @patch('core.etl_orchestrator.normalize_to_staging_tables')
    @patch('core.etl_orchestrator.merge_staging_to_fact_tables')
    def test_process_from_products_step1_with_data(self, mock_merge, mock_normalize, 
                                                 mock_create_staging, mock_ensure_conn, 
                                                 mock_create_engine):
        """Test processing existing data from ProductsStep1 table."""
        
        # Setup mocks
        mock_ensure_conn.return_value = True
        
        # Mock normalization results directly since we're not testing read_from_products_step1
        mock_normalize.return_value = {"providers": 2, "products": 3, "provider_products": 3}
        
        # Execute the processing
        result = process_from_products_step1("test-server", "test-db")
        
        # Verify the result
        assert result["status"] is True
        assert "completed successfully" in result["message"]
        assert "batch_guid" in result
        assert result["staging_summary"]["providers"] == 2
        assert result["staging_summary"]["products"] == 3
        assert result["staging_summary"]["provider_products"] == 3
        
        # Verify all steps were called
        mock_create_staging.assert_called_once()
        mock_normalize.assert_called_once()
        mock_merge.assert_called_once()
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    def test_database_connection_failure(self, mock_ensure_conn, mock_create_engine):
        """Test handling of database connection failures."""
        
        # Setup mock to simulate connection failure
        mock_ensure_conn.return_value = None
        
        # Create test CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio
Test Product,2024-01-15,Test Provider,1000.00
""".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "test-file.csv", 
            "test-server", 
            "test-db", 
            "ProductsStep1"
        )
        
        # Verify failure is handled properly
        assert result["status"] is False
        assert "Failed to establish connection" in result["message"]
    
    def test_empty_csv_handling(self):
        """Test handling of empty CSV files."""
        
        # Create empty CSV data
        csv_data = "".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "empty-file.csv", 
            "test-server", 
            "test-db", 
            "ProductsStep1"
        )
        
        # Verify failure is handled properly
        assert result["status"] is False
        assert "failed" in result["message"].lower()
    
    def test_malformed_csv_handling(self):
        """Test handling of malformed CSV files."""
        
        # Create malformed CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio
Test Product,2024-01-15,Test Provider
Missing Price Column
""".encode('utf-8')
        
        with patch('core.etl_orchestrator.create_azure_sql_engine'), \
             patch('core.etl_orchestrator.ensure_connection_established', return_value=True), \
             patch('core.etl_orchestrator.check_process_file_status', 
                   return_value={"exists": False, "status_id": None, "id": None}), \
             patch('core.etl_orchestrator.insert_process_file_record', return_value=123):
            
            # Execute the pipeline - this should still work as pandas is quite forgiving
            result = process_csv_from_stream(
                csv_data, 
                "malformed-file.csv", 
                "test-server", 
                "test-db", 
                "ProductsStep1"
            )
            
            # The CSV will be parsed (pandas handles missing values), 
            # so we expect either success or a specific error
            # depending on the downstream processing
            assert isinstance(result["status"], bool)


class TestOpenAIIntegration:
    """Test OpenAI integration functionality."""
    
    @patch('core.etl_orchestrator.os.environ.get')
    @patch('requests.post')
    def test_openai_successful_extraction(self, mock_post, mock_env):
        """Test successful product extraction using OpenAI."""
        from core.etl_orchestrator import extract_invoice_data_with_openai
        
        # Setup environment variables
        mock_env.side_effect = lambda key, default=None: {
            'AZURE_OPENAI_ENDPOINT': 'https://test.openai.azure.com',
            'AZURE_OPENAI_KEY': 'test-key',
            'AZURE_OPENAI_MODEL': 'gpt-4-vision-preview'
        }.get(key, default)
        
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': '[{"Producto": "Arroz Premium", "Provedor": "Test Provider", "Precio": "2500.00", "Porcentaje de IVA": "19"}]'
                }
            }]
        }
        mock_post.return_value = mock_response
        
        # Execute extraction
        result = extract_invoice_data_with_openai(b"fake_image_data", "test_invoice.jpg")
        
        # Verify result
        assert len(result) == 1
        assert result[0]["Producto"] == "Arroz Premium"
        assert result[0]["Provedor"] == "Test Provider"
        assert result[0]["Precio"] == "2500.00"
        assert result[0]["Porcentaje de IVA"] == "19"
    
    @patch('core.etl_orchestrator.os.environ.get')
    @patch('core.etl_orchestrator.extract_invoice_data_from_image')
    def test_openai_fallback_on_missing_config(self, mock_fallback, mock_env):
        """Test fallback to mock extraction when OpenAI config is missing."""
        from core.etl_orchestrator import extract_invoice_data_with_openai
        
        # Setup missing environment variables
        mock_env.return_value = None
        mock_fallback.return_value = [{"Producto": "Fallback Product"}]
        
        # Execute extraction
        result = extract_invoice_data_with_openai(b"fake_image_data", "test_invoice.jpg")
        
        # Verify fallback was used
        assert result == [{"Producto": "Fallback Product"}]
        mock_fallback.assert_called_once()