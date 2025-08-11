"""
Integration tests for the complete ETL pipeline.
These tests mock the database connections but test the full flow.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from core.etl_orchestrator import process_csv_from_stream


class TestETLIntegration:
    """Integration tests for the complete ETL pipeline."""
    
    @patch('core.etl_orchestrator.Session')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.check_process_file_status')
    def test_skip_already_processed_file(self, mock_check_status, mock_create_engine, 
                                       mock_ensure_conn, mock_session):
        """Test that already processed files are skipped."""
        # Mock file as already processed (status 3)
        mock_check_status.return_value = 3
        
        # Create test CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio
Test Product,2024-01-15,Test Provider,1000.00
""".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "already-processed.csv", 
            "test-server", 
            "test-db"
        )
        
        # Verify the file was skipped
        assert result.status is True
        assert "already processed successfully" in result.message
    
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.ensure_connection_established')
    def test_database_connection_failure(self, mock_ensure_conn, mock_create_engine):
        """Test handling of database connection failures."""
        
        # Setup mock to simulate connection failure
        mock_ensure_conn.side_effect = Exception("Could not establish database connection")
        
        # Create test CSV data
        csv_data = """Producto,Fecha 1,Provedor,Precio
Test Product,2024-01-15,Test Provider,1000.00
""".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "test-file.csv", 
            "test-server", 
            "test-db"
        )
        
        # Verify failure is handled properly
        assert result.status is False
        assert "Could not establish database connection" in result.message
    
    def test_empty_csv_handling(self):
        """Test handling of empty CSV files."""
        
        # Create empty CSV data
        csv_data = "".encode('utf-8')
        
        # Execute the pipeline
        result = process_csv_from_stream(
            csv_data, 
            "empty-file.csv", 
            "test-server", 
            "test-db"
        )
        
        # Verify failure is handled properly
        assert result.status is False
        assert "failed" in result.message.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])