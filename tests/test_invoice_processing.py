import sys
import os
import pytest
import io
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch

# Add the parent directory to the path so we can import from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.etl_orchestrator import (
    extract_invoice_data_with_openai,
    process_invoice_image
)


# Pytest markers for categorizing tests
pytestmark = pytest.mark.invoice


@pytest.mark.extraction
class TestInvoiceDataExtraction:
    """Tests for invoice data extraction functions."""
    
    @patch('core.etl_orchestrator.AzureOpenAI')
    def test_extract_invoice_data_with_openai_success(self, mock_openai_client):
        """Test extracting data from image with successful OpenAI response."""
        # Mock the OpenAI client and response
        mock_client = Mock()
        mock_openai_client.return_value = mock_client
        
        # Properly structure the mock response
        mock_choice = Mock()
        mock_choice.message.content = "Producto,Provedor,Precio,Porcentaje de IVA\nTest Product,Test Provider,100.00,19\nAnother Product,Another Provider,200.00,8"
        
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create.return_value = mock_response
        
        # Set required environment variables
        os.environ['AZURE_OPENAI_ENDPOINT'] = 'https://test.openai.azure.com/'
        os.environ['AZURE_OPENAI_KEY'] = 'test-key'
        
        image_content = b"mock_image_data"
        image_name = "factura_001.jpg"
        
        result = extract_invoice_data_with_openai(image_content, image_name)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "Producto" in result.columns
        assert "Provedor" in result.columns
        assert "Precio" in result.columns
        assert "Porcentaje de IVA" in result.columns
        assert result.iloc[0]["Producto"] == "Test Product"
        assert result.iloc[1]["Producto"] == "Another Product"
    
    @patch('core.etl_orchestrator.AzureOpenAI')
    def test_extract_invoice_data_with_openai_markdown_response(self, mock_openai_client):
        """Test extracting data with markdown-formatted OpenAI response."""
        # Mock the OpenAI client and response with markdown
        mock_client = Mock()
        mock_openai_client.return_value = mock_client
        
        # Properly structure the mock response
        mock_choice = Mock()
        mock_choice.message.content = "```csv\nProducto,Provedor,Precio,Porcentaje de IVA\nMarkdown Product,Markdown Provider,150.00,19\n```"
        
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create.return_value = mock_response
        
        # Set required environment variables
        os.environ['AZURE_OPENAI_ENDPOINT'] = 'https://test.openai.azure.com/'
        os.environ['AZURE_OPENAI_KEY'] = 'test-key'
        
        image_content = b"mock_image_data"
        image_name = "invoice_002.png"
        
        result = extract_invoice_data_with_openai(image_content, image_name)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["Producto"] == "Markdown Product"
    
@pytest.mark.integration
class TestInvoiceProcessingIntegration:
    """Integration tests for complete invoice processing pipeline."""
    
    @patch('core.etl_orchestrator.apply_transformations')
    @patch('core.etl_orchestrator.map_columns_to_apply_transformations')
    @patch('core.etl_orchestrator.merge_staging_to_fact_tables')
    @patch('core.etl_orchestrator.normalize_to_staging_tables_from_dataframe')
    @patch('core.etl_orchestrator.ensure_connection_established')
    @patch('core.etl_orchestrator.create_azure_sql_engine')
    @patch('core.etl_orchestrator.extract_invoice_data_with_openai')
    def test_process_invoice_image_success(self, mock_extract, mock_engine, mock_ensure_connection, mock_normalize, mock_merge, mock_map_columns, mock_apply_transforms):
        """Test successful invoice image processing."""
        # Setup mocks
        mock_df = pd.DataFrame({
            'Producto': ['Test Product'],
            'Provedor': ['Test Provider'],
            'Precio': ['100.00'],
            'Porcentaje de IVA': ['19']
        })
        mock_extract.return_value = mock_df
        mock_map_columns.return_value = mock_df
        mock_apply_transforms.return_value = mock_df
        
        mock_engine_instance = Mock()
        mock_engine.return_value = mock_engine_instance
        mock_ensure_connection.return_value = None  # No exception means success
        
        # Test data
        image_content = b"mock_image_data"
        image_name = "factura_test.jpg"
        server_name = "test-server"
        database_name = "test-db"
        
        result = process_invoice_image(image_content, image_name, server_name, database_name)
        
        # Verify result
        assert result.status is True
        assert hasattr(result, 'products_extracted')
        assert result.products_extracted == 1
        assert "Invoice processing completed successfully" in result.message
        
        # Verify mocks were called
        mock_extract.assert_called_once_with(image_content, image_name)
        mock_engine.assert_called_once_with(server_name, database_name)
        mock_ensure_connection.assert_called_once_with(mock_engine_instance)
        mock_normalize.assert_called_once()
        mock_merge.assert_called_once()
    
    @patch('core.etl_orchestrator.extract_invoice_data_with_openai')
    def test_process_invoice_image_extraction_failure(self, mock_extract):
        """Test invoice processing when data extraction fails."""
        # Setup mock to raise exception
        mock_extract.side_effect = Exception("OpenAI extraction failed")
        
        # Test data
        image_content = b"mock_image_data"
        image_name = "test.jpg"
        server_name = "test-server"
        database_name = "test-db"
        
        result = process_invoice_image(image_content, image_name, server_name, database_name)
        
        # Verify result indicates failure
        assert result.status is False
        assert "OpenAI extraction failed" in result.message
    
    def test_process_invoice_image_invalid_input(self):
        """Test invoice processing with invalid input."""
        # Test with None image content
        result = process_invoice_image(None, "test.jpg", "server", "database")
        
        assert result.status is False
        assert "message" in result.__dict__


def run_invoice_tests():
    """Run all invoice processing tests."""
    import subprocess
    
    print("🧪 Running Invoice Processing Tests with pytest")
    print("=" * 60)
    
    result = subprocess.run([
        'pytest', __file__, 
        '-v',
        '--tb=short',
        '-x'
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    
    if result.returncode == 0:
        print("\n🎉 All invoice processing tests passed!")
    else:
        print("\n❌ Some invoice processing tests failed!")
    
    return result.returncode == 0


if __name__ == "__main__":
    run_invoice_tests()