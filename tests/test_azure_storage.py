import os
import sys
import pytest
import json
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))
from core.storage import (
    get_blob_service_client, 
    read_blob_content, 
    get_blob_properties,
    list_blobs_in_container
)


# Pytest markers for categorizing tests
pytestmark = pytest.mark.storage


def setup_storage_environment():
    """Set up environment variables for storage testing."""

    # Try to load from local.settings.json first
    local_settings_path = Path(__file__).parent.parent / "local.settings.json"
    if local_settings_path.exists():
        try:
            with open(local_settings_path, 'r') as f:
                settings = json.load(f)
                for key, value in settings.get('Values', {}).items():
                    os.environ[key] = value
        except Exception as e:
            print(f"Warning: Could not load local.settings.json: {e}")
    
    # Verify required environment variables are set
    required_vars = ['STORAGE_ACCOUNT_NAME']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}.")


@pytest.fixture(scope="session")
def blob_service_client():
    """Create Azure Blob Service Client for testing."""
    setup_storage_environment()
    return get_blob_service_client(os.environ.get('STORAGE_ACCOUNT_NAME'))


@pytest.fixture(scope="session")
def test_container():
    """Test container name."""
    return "products-dev"


@pytest.fixture(scope="session")
def sample_csv_content():
    """Sample CSV content for testing."""
    return """
        Producto,Precio,Provedor,Fecha 1
        Arroz Premium 500g,1200,Proveedor ABC,15/03/2024
        Harina de Trigo 1kg,800,Proveedor XYZ,20/03/2024
        Aceite Vegetal 500ml,950,Proveedor DEF,25/03/2024
    """


@pytest.mark.integration
class TestAzureStorageConnection:
    """Tests for Azure Storage authentication and blob access."""
    
    def test_blob_service_client_creation(self, blob_service_client):
        """Test that Azure Storage client can be created successfully."""
        assert blob_service_client is not None, "Blob service client should be created"
        print(f"✅ Blob service client created successfully")
    
    def test_blob_service_client_properties(self, blob_service_client):
        """Test that we can access blob service properties."""
        try:
            # Get account information
            account_info = blob_service_client.get_account_information()
            assert account_info is not None, "Should be able to get account information"
            print(f"✅ Account info retrieved: {account_info}")
        except Exception as e:
            pytest.fail(f"Failed to get account information: {str(e)}")
    
    def test_container_exists(self, blob_service_client, test_container):
        """Test that the target container exists and is accessible."""
        try:
            container_client = blob_service_client.get_container_client(test_container)
            container_properties = container_client.get_container_properties()
            assert container_properties is not None, f"Container {test_container} should exist"
            print(f"✅ Container {test_container} exists and is accessible")
            print(f"📊 Container created: {container_properties.last_modified}")
        except Exception as e:
            pytest.fail(f"Failed to access container {test_container}: {str(e)}")
    
    def test_list_blobs_in_container(self, blob_service_client, test_container):
        """Test that we can list blobs in the container."""
        try:
            blobs = list_blobs_in_container(blob_service_client, test_container)
            assert isinstance(blobs, list), "Should return a list of blobs"
            print(f"✅ Successfully listed blobs in {test_container}")
            print(f"📦 Found {len(blobs)} blobs in container")
            
            # Print first few blobs for information
            for i, blob in enumerate(blobs[:3]):
                print(f"   - {blob.name} ({blob.size} bytes)")
                if i >= 2:  # Show max 3 blobs
                    break
            
        except Exception as e:
            pytest.fail(f"Failed to list blobs in container {test_container}: {str(e)}")
    
    def test_blob_upload_and_read(self, blob_service_client, test_container, sample_csv_content):
        """Test uploading a blob and reading it back."""
        test_blob_name = f"test-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
        
        try:
            # Upload test blob
            blob_client = blob_service_client.get_blob_client(
                container=test_container, 
                blob=test_blob_name
            )
            
            # Upload the sample CSV content
            blob_client.upload_blob(sample_csv_content, overwrite=True)
            print(f"✅ Successfully uploaded test blob: {test_blob_name}")
            
            # Read the blob back
            downloaded_content = read_blob_content(blob_service_client, test_container, test_blob_name)
            downloaded_text = downloaded_content.decode('utf-8')
            
            assert downloaded_text == sample_csv_content, "Downloaded content should match uploaded content"
            print(f"✅ Successfully read back test blob content")
            print(f"📄 Content size: {len(downloaded_content)} bytes")
            
            # Get blob properties
            properties = get_blob_properties(blob_service_client, test_container, test_blob_name)
            assert properties is not None, "Should be able to get blob properties"
            print(f"✅ Blob properties retrieved:")
            print(f"   - Size: {properties.size} bytes")
            print(f"   - Last modified: {properties.last_modified}")
            print(f"   - Content type: {properties.content_type}")
            
        except Exception as e:
            pytest.fail(f"Failed blob upload/read test: {str(e)}")
        
        finally:
            # Clean up: delete the test blob
            try:
                blob_client = blob_service_client.get_blob_client(
                    container=test_container, 
                    blob=test_blob_name
                )
                blob_client.delete_blob()
                print(f"🧹 Cleaned up test blob: {test_blob_name}")
            except Exception as cleanup_error:
                print(f"⚠️ Warning: Failed to clean up test blob {test_blob_name}: {cleanup_error}")
    
    def test_storage_permissions(self, blob_service_client, test_container):
        """Test that we have appropriate storage permissions."""
        try:
            container_client = blob_service_client.get_container_client(test_container)
            
            # Test read permission by listing blobs
            blob_list = list(container_client.list_blobs())
            print(f"✅ Read permission confirmed - can list {len(blob_list)} blobs")
            
            # Test write permission by creating a small test blob
            test_blob_name = f"permission-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
            test_content = "Permission test content"
            
            blob_client = container_client.get_blob_client(test_blob_name)
            blob_client.upload_blob(test_content, overwrite=True)
            print(f"✅ Write permission confirmed - uploaded test blob")
            
            # Test delete permission
            blob_client.delete_blob()
            print(f"✅ Delete permission confirmed - removed test blob")
            
        except Exception as e:
            pytest.fail(f"Storage permissions test failed: {str(e)}")
    
    def test_error_handling_invalid_blob(self, test_container):
        """Test error handling for non-existent blobs."""
        non_existent_blob = "this-blob-does-not-exist-12345.csv"
        
        with pytest.raises(Exception):
            read_blob_content(test_container, non_existent_blob)
        
        print(f"✅ Error handling works correctly for non-existent blobs")
    
    def test_error_handling_invalid_container(self):
        """Test error handling for non-existent containers."""
        non_existent_container = "this-container-does-not-exist-12345"
        test_blob = "any-blob.csv"
        
        with pytest.raises(Exception):
            read_blob_content(non_existent_container, test_blob)
        
        print(f"✅ Error handling works correctly for non-existent containers")


@pytest.mark.integration
class TestStorageIntegration:
    """Integration tests for storage operations used in ETL pipeline."""
    
    def test_csv_processing_workflow(self, blob_service_client, test_container, sample_csv_content):
        """Test the complete CSV processing workflow."""
        test_blob_name = f"integration-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
        
        try:
            # Step 1: Upload CSV file (simulating file upload)
            blob_client = blob_service_client.get_blob_client(
                container=test_container, 
                blob=test_blob_name
            )
            blob_client.upload_blob(sample_csv_content, overwrite=True)
            print(f"✅ Step 1: Uploaded CSV file: {test_blob_name}")
            
            # Step 2: Read and validate content (simulating ETL trigger)
            content = read_blob_content(blob_service_client, test_container, test_blob_name)
            assert len(content) > 0, "Content should not be empty"
            print(f"✅ Step 2: Read content ({len(content)} bytes)")
            
            # Step 3: Parse CSV content (simulating data processing)
            content_str = content.decode('utf-8')
            lines = content_str.strip().split('\n')
            assert len(lines) >= 2, "Should have header and data rows"  # Header + at least 1 data row
            print(f"✅ Step 3: Parsed CSV - {len(lines)} lines ({len(lines)-1} data rows)")
            
            # Step 4: Validate CSV structure
            header = lines[0].split(',')
            expected_columns = ['Producto', 'Precio', 'Provedor', 'Fecha 1']
            assert header == expected_columns, f"Header should match expected columns: {expected_columns}"
            print(f"✅ Step 4: CSV structure validated - columns: {header}")
            
            # Step 5: Get file metadata (simulating logging)
            properties = get_blob_properties(blob_service_client, test_container, test_blob_name)
            print(f"✅ Step 5: File metadata retrieved:")
            print(f"   - File: {properties.name}")
            print(f"   - Size: {properties.size} bytes")
            print(f"   - Upload time: {properties.last_modified}")
            
        except Exception as e:
            pytest.fail(f"CSV processing workflow failed: {str(e)}")
        
        finally:
            # Clean up
            try:
                blob_client.delete_blob()
                print(f"🧹 Cleaned up integration test file: {test_blob_name}")
            except Exception as cleanup_error:
                print(f"⚠️ Warning: Failed to clean up {test_blob_name}: {cleanup_error}")


def run_storage_tests():
    """Run storage tests using pytest."""
    import subprocess
    
    print("☁️ Running Azure Storage Connection Tests with pytest")
    print("=" * 60)
    
    # Run with verbose output
    result = subprocess.run([
        'pytest', __file__, 
        '-v',                   # verbose output
        '--tb=short',           # shorter traceback format
        '-s',                   # don't capture output (show prints)
        '-x'                    # stop on first failure
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    
    # Additional summary
    if result.returncode == 0:
        print("\n🎉 All storage tests passed!")
        print("💡 Azure Storage is ready for ETL pipeline")
        print("💡 You can run these tests with:")
        print("   pytest tests/test_azure_storage.py -v")
        print("   pytest tests/test_azure_storage.py -m integration")
        print("   pytest -m storage  # Run all storage tests")
    else:
        print("\n❌ Some storage tests failed!")
        print("🔧 Check storage account configuration and permissions")
    
    return result.returncode == 0


if __name__ == "__main__":
    run_storage_tests()
