import sys
import os
import pytest
import io
from datetime import datetime
from unittest.mock import Mock, patch

# Add the parent directory to the path so we can import from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.etl_orchestrator import (
    extract_invoice_data_from_image,
    generate_csv_from_invoice_data,
    process_invoice_image
)


# Pytest markers for categorizing tests
pytestmark = pytest.mark.invoice


@pytest.mark.extraction
class TestInvoiceDataExtraction:
    """Tests for invoice data extraction functions."""
    
    def test_extract_invoice_data_from_image_factura_filename(self):
        """Test extracting data from image with 'factura' in filename."""
        image_content = b"mock_image_data"
        image_name = "factura_001.jpg"
        
        result = extract_invoice_data_from_image(image_content, image_name)
        
        assert isinstance(result, list)
        assert len(result) == 2  # Mock returns 2 products for factura
        assert all("Producto" in product for product in result)
        assert all("Provedor" in product for product in result)
        assert all("Precio" in product for product in result)
        assert all("Porcentaje de IVA" in product for product in result)
    
    def test_extract_invoice_data_from_image_invoice_filename(self):
        """Test extracting data from image with 'invoice' in filename."""
        image_content = b"mock_image_data"
        image_name = "invoice_002.png"
        
        result = extract_invoice_data_from_image(image_content, image_name)
        
        assert isinstance(result, list)
        assert len(result) == 2  # Mock returns 2 products for invoice
    
    def test_extract_invoice_data_from_image_generic_filename(self):
        """Test extracting data from image with generic filename."""
        image_content = b"mock_image_data"
        image_name = "document_001.jpg"
        
        result = extract_invoice_data_from_image(image_content, image_name)
        
        assert isinstance(result, list)
        assert len(result) == 1  # Mock returns 1 product for generic
        assert result[0]["Producto"] == "Producto Generico"
        assert result[0]["Provedor"] == "Proveedor Generico"


@pytest.mark.csv
class TestCSVGeneration:
    """Tests for CSV generation from invoice data."""
    
    def test_generate_csv_from_invoice_data_single_product(self):
        """Test CSV generation with single product."""
        products_data = [
            {
                "Producto": "Test Product",
                "Provedor": "Test Provider",
                "Precio": "1500.00",
                "Porcentaje de IVA": "19"
            }
        ]
        trigger_date = "2024-01-15"
        
        result = generate_csv_from_invoice_data(products_data, trigger_date)
        
        assert isinstance(result, str)
        assert "Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA" in result
        assert "Test Product" in result
        assert "2024-01-15" in result
        assert "Test Provider" in result
        assert "1500.00" in result
        assert "19" in result
    
    def test_generate_csv_from_invoice_data_multiple_products(self):
        """Test CSV generation with multiple products."""
        products_data = [
            {
                "Producto": "Product 1",
                "Provedor": "Provider 1",
                "Precio": "1000.00",
                "Porcentaje de IVA": "19"
            },
            {
                "Producto": "Product 2",
                "Provedor": "Provider 2",
                "Precio": "2000.00",
                "Porcentaje de IVA": "8"
            }
        ]
        trigger_date = "2024-01-15"
        
        result = generate_csv_from_invoice_data(products_data, trigger_date)
        
        # Count lines (header + 2 products)
        lines = result.strip().split('\n')
        assert len(lines) == 3
        assert "Product 1" in result
        assert "Product 2" in result
    
    def test_generate_csv_from_invoice_data_empty_products(self):
        """Test CSV generation with empty products list."""
        products_data = []
        trigger_date = "2024-01-15"
        
        result = generate_csv_from_invoice_data(products_data, trigger_date)
        
        assert isinstance(result, str)
        assert "Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA" in result
        # Should only have header
        lines = result.strip().split('\n')
        assert len(lines) == 1


@pytest.mark.integration
class TestInvoiceProcessingIntegration:
    """Integration tests for complete invoice processing pipeline."""
    
    @patch('core.etl_orchestrator.get_blob_service_client')
    @patch('core.etl_orchestrator.upload_blob_content')
    def test_process_invoice_image_success(self, mock_upload, mock_get_client):
        """Test successful invoice image processing."""
        # Setup mocks
        mock_blob_client = Mock()
        mock_get_client.return_value = mock_blob_client
        mock_upload.return_value = True
        
        # Test data
        image_content = b"mock_image_data"
        image_name = "factura_test.jpg"
        storage_account_name = "teststorage"
        output_container = "products-dev"
        
        result = process_invoice_image(image_content, image_name, storage_account_name, output_container)
        
        # Verify result
        assert result["status"] is True
        assert "products_extracted" in result
        assert result["products_extracted"] == 2  # Mock extracts 2 products for factura
        assert "csv_filename" in result
        assert result["csv_filename"].startswith("factura_test_products_")
        assert result["csv_filename"].endswith(".csv")
        assert result["output_container"] == output_container
        
        # Verify mocks were called
        mock_get_client.assert_called_once_with(storage_account_name)
        mock_upload.assert_called_once()
        
        # Verify upload was called with correct parameters
        call_args = mock_upload.call_args
        assert call_args[0][0] == mock_blob_client  # blob_service_client
        assert call_args[0][1] == output_container   # container_name
        assert call_args[0][2].startswith("factura_test_products_")  # blob_name
        assert "Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA" in call_args[0][3]  # content
    
    @patch('core.etl_orchestrator.get_blob_service_client')
    @patch('core.etl_orchestrator.upload_blob_content')
    def test_process_invoice_image_upload_failure(self, mock_upload, mock_get_client):
        """Test invoice processing when upload fails."""
        # Setup mocks to raise exception
        mock_blob_client = Mock()
        mock_get_client.return_value = mock_blob_client
        mock_upload.side_effect = Exception("Upload failed")
        
        # Test data
        image_content = b"mock_image_data"
        image_name = "test.jpg"
        storage_account_name = "teststorage"
        output_container = "products-dev"
        
        result = process_invoice_image(image_content, image_name, storage_account_name, output_container)
        
        # Verify result indicates failure
        assert result["status"] is False
        assert "Upload failed" in result["message"]
    
    def test_process_invoice_image_invalid_input(self):
        """Test invoice processing with invalid input."""
        # Test with None image content
        result = process_invoice_image(None, "test.jpg", "storage", "container")
        
        assert result["status"] is False
        assert "message" in result


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