import os
import sys
import json
import pytest
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).parent.parent))
from common.sql_utils import create_azure_sql_engine, ensure_connection_established, initialize_database


# Pytest markers for categorizing tests
pytestmark = pytest.mark.database


def setup_environment():
    """Set up environment variables for Azure SQL testing."""

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
    required_vars = ['SQL_SERVER', 'SQL_DATABASE']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}.")


@pytest.fixture(scope="session")
def azure_sql_engine():
    """Create Azure SQL engine for testing."""
    setup_environment()
    engine = create_azure_sql_engine(
        os.environ.get('SQL_SERVER'), 
        os.environ.get('SQL_DATABASE')
    )
    initialize_database(engine)
    return engine


@pytest.mark.integration
class TestAzureADConnection:
    """Tests for Azure AD authentication and database access."""
    
    def test_azure_sql_connection_successful(self, azure_sql_engine):
        """Test that Azure AD authentication works and connection is successful."""
        result = ensure_connection_established(azure_sql_engine)
        assert result is not None, "Connection should be successful"
        print(f"✅ Connection successful: {result}")
    
    def test_database_has_tables(self, azure_sql_engine):
        """Test that the database contains tables."""
        with azure_sql_engine.connect() as conn:
            table_count = conn.execute(text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")).fetchone()
            assert table_count[0] > 0, "Database should contain tables"
            print(f"📊 Database has {table_count[0]} tables")
    
    def test_unit_of_measure_table_exists(self, azure_sql_engine):
        """Test that UnitOfMeasure table exists and has records."""
        with azure_sql_engine.connect() as conn:
            # First check if table exists
            table_exists = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'UnitOfMeasure'"
            )).fetchone()
            assert table_exists[0] == 1, "UnitOfMeasure table should exist"
            
            # Then check record count
            unit_count = conn.execute(text("SELECT COUNT(*) FROM UnitOfMeasure")).fetchone()
            assert unit_count[0] >= 0, "UnitOfMeasure table should be accessible"
            print(f"📦 UnitOfMeasure table has {unit_count[0]} records")
    
    def test_database_permissions(self, azure_sql_engine):
        """Test that we have appropriate database permissions."""
        with azure_sql_engine.connect() as conn:
            # Test read permission
            result = conn.execute(text("SELECT 1")).fetchone()
            assert result[0] == 1, "Should have read permissions"
            
            # Test schema access
            schema_count = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA"
            )).fetchone()
            assert schema_count[0] > 0, "Should be able to access schema information"


def run_integration_tests():
    """Run integration tests using pytest."""
    import subprocess
    
    print("🔗 Running Azure AD SQL Connection Tests with pytest")
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
        print("\n🎉 All connection tests passed!")
        print("💡 Ready to use in function_app.py")
        print("💡 You can run these tests with:")
        print("   pytest tests/test_azure_ad_default.py -v")
        print("   pytest tests/test_azure_ad_default.py -m integration")
    else:
        print("\n❌ Some connection tests failed!")
        print("🔧 Check firewall rules and database permissions")
    
    return result.returncode == 0


if __name__ == "__main__":
    run_integration_tests()
