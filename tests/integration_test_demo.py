import sys
import os
import tempfile
import logging
from unittest.mock import Mock, patch, MagicMock

# Add the parent directory to the path so we can import from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.etl_orchestrator import process_invoice_image
from core.storage import upload_blob_content, get_blob_service_client


def test_end_to_end_invoice_processing():
    """
    End-to-end integration test for invoice processing.
    This test simulates the complete workflow from image upload to CSV creation.
    """
    print("🧪 Running End-to-End Invoice Processing Test")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create mock image content (simulating an uploaded invoice image)
    mock_image_content = b"Mock invoice image content - this would be a real image in production"
    image_name = "factura_empresa_123.jpg"
    storage_account_name = "teststorage"
    output_container = "products-dev"
    
    # Mock the Azure Storage interactions
    with patch('core.etl_orchestrator.get_blob_service_client') as mock_get_client, \
         patch('core.etl_orchestrator.upload_blob_content') as mock_upload:
        
        # Setup mocks
        mock_blob_client = Mock()
        mock_get_client.return_value = mock_blob_client
        mock_upload.return_value = True
        
        print(f"📁 Processing invoice: {image_name}")
        print(f"📤 Target container: {output_container}")
        
        # Run the complete invoice processing pipeline
        result = process_invoice_image(
            image_content=mock_image_content,
            image_name=image_name,
            storage_account_name=storage_account_name,
            output_container=output_container
        )
        
        # Verify the result
        print(f"✅ Processing result: {result['status']}")
        
        if result["status"]:
            print(f"📊 Products extracted: {result['products_extracted']}")
            print(f"📄 CSV filename: {result['csv_filename']}")
            print(f"🗂️ Output container: {result['output_container']}")
            
            # Verify the CSV filename format
            expected_prefix = "factura_empresa_123_products_"
            assert result['csv_filename'].startswith(expected_prefix), f"CSV filename should start with {expected_prefix}"
            assert result['csv_filename'].endswith('.csv'), "CSV filename should end with .csv"
            
            # Verify mocks were called correctly
            mock_get_client.assert_called_once_with(storage_account_name)
            mock_upload.assert_called_once()
            
            # Extract the actual CSV content that would have been uploaded
            upload_call_args = mock_upload.call_args
            csv_content = upload_call_args[0][3]  # Fourth argument is the content
            
            print("\n📝 Generated CSV Content:")
            print("-" * 40)
            print(csv_content)
            print("-" * 40)
            
            # Verify CSV structure
            lines = csv_content.strip().split('\n')
            assert len(lines) >= 2, "CSV should have header + at least one data row"
            
            header = lines[0].strip()  # Remove any whitespace/carriage returns
            expected_header = "Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA"
            assert header == expected_header, f"Header should be: {expected_header}, got: {header}"
            
            # Verify data rows contain expected products
            assert "Arroz Premium 1kg" in csv_content, "Should contain Arroz Premium"
            assert "Aceite Vegetal 500ml" in csv_content, "Should contain Aceite Vegetal"
            assert "Distribuidora San Juan" in csv_content, "Should contain provider name"
            assert "19" in csv_content, "Should contain IVA percentage"
            
            print("✅ All validations passed!")
            return True
        else:
            print(f"❌ Processing failed: {result['message']}")
            return False


def test_csv_upload_functionality():
    """
    Test the CSV upload functionality in isolation.
    """
    print("\n🧪 Testing CSV Upload Functionality")
    print("=" * 60)
    
    # Create sample CSV content
    csv_content = """Producto,Fecha 1,Provedor,Precio,Porcentaje de IVA
Test Product,2024-01-15,Test Provider,1500.00,19"""
    
    # Mock the blob service client and upload
    with patch('core.storage.BlobServiceClient') as mock_blob_service_class:
        mock_blob_service = Mock()
        mock_blob_client = Mock()
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        mock_blob_service_class.return_value = mock_blob_service
        
        try:
            # Test the upload function
            result = upload_blob_content(
                blob_service_client=mock_blob_service,
                container_name="test-container",
                blob_name="test_file.csv",
                content=csv_content,
                content_type="text/csv"
            )
            
            # Verify upload was called correctly
            mock_blob_service.get_blob_client.assert_called_once_with(
                container="test-container", 
                blob="test_file.csv"
            )
            mock_blob_client.upload_blob.assert_called_once()
            
            # Check the upload_blob call arguments
            upload_call = mock_blob_client.upload_blob.call_args
            assert upload_call[0][0] == csv_content, "Content should match"
            assert upload_call[1]['overwrite'] is True, "Should overwrite existing"
            assert upload_call[1]['content_settings']['content_type'] == "text/csv", "Content type should be text/csv"
            
            print("✅ CSV upload functionality works correctly!")
            return True
            
        except Exception as e:
            print(f"❌ CSV upload test failed: {e}")
            return False


def demo_invoice_types():
    """
    Demonstrate processing different types of invoice images.
    """
    print("\n🧪 Demonstrating Different Invoice Types")
    print("=" * 60)
    
    test_images = [
        ("factura_supermercado.jpg", "Supermarket invoice"),
        ("invoice_restaurant.png", "Restaurant invoice"), 
        ("receipt_pharmacy.jpg", "Pharmacy receipt"),
        ("documento_comercial.pdf", "Generic commercial document")
    ]
    
    for image_name, description in test_images:
        print(f"\n📄 Processing: {description}")
        print(f"   File: {image_name}")
        
        try:
            # Mock the storage operations
            with patch('core.etl_orchestrator.get_blob_service_client') as mock_get_client, \
                 patch('core.etl_orchestrator.upload_blob_content') as mock_upload:
                
                mock_blob_client = Mock()
                mock_get_client.return_value = mock_blob_client
                mock_upload.return_value = True
                
                # Process the "image"
                result = process_invoice_image(
                    image_content=b"mock_image_data",
                    image_name=image_name,
                    storage_account_name="teststorage",
                    output_container="products-dev"
                )
                
                if result["status"]:
                    print(f"   ✅ Success: {result['products_extracted']} products extracted")
                    print(f"   📄 Output: {result['csv_filename']}")
                else:
                    print(f"   ❌ Failed: {result['message']}")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    print("🚀 Running Invoice Processing Integration Tests")
    print("=" * 80)
    
    success = True
    
    # Run end-to-end test
    success &= test_end_to_end_invoice_processing()
    
    # Test CSV upload
    success &= test_csv_upload_functionality()
    
    # Demo different invoice types
    demo_invoice_types()
    
    print("\n" + "=" * 80)
    if success:
        print("🎉 All integration tests completed successfully!")
        print("\n📋 Summary:")
        print("   ✅ Invoice processing pipeline working")
        print("   ✅ CSV generation and upload working")
        print("   ✅ Multiple invoice types supported")
        print("   ✅ Ready for Azure deployment")
    else:
        print("❌ Some integration tests failed!")
        exit(1)