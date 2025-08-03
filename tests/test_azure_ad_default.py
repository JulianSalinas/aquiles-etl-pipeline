import os
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).parent.parent))
from common.sql_utils import create_azure_sql_engine, test_connection, initialize_database


def setup_environment():
    """Set up environment variables for testing."""
    os.environ['SQL_SERVER'] = 'provider24-dev.database.windows.net'
    os.environ['SQL_DATABASE'] = 'provider24'


def test_azure_sql_connection():
    """Test Azure AD authentication and database access."""
    setup_environment()
    
    try:
        engine = create_azure_sql_engine(os.environ.get('SQL_SERVER'), os.environ.get('SQL_DATABASE'))
        initialize_database(engine)     
        result = test_connection(engine)

        print(f"✅ Connection successful: {result}")
        
        with engine.connect() as conn:
            table_count = conn.execute(text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")).fetchone()
            print(f"📊 Database has {table_count[0]} tables")
        
        with engine.connect() as conn:
            unit_count = conn.execute(text("SELECT COUNT(*) FROM UnitOfMeasure")).fetchone()
            print(f"📦 UnitOfMeasure table has {unit_count[0]} records")
        
        return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    """Main test execution."""
    print("Azure AD SQL Connection Test")
    print("=" * 50)
    
    if test_azure_sql_connection():
        print("💡 Ready to use in function_app.py")
    else:
        print("🔧 Check firewall rules and database permissions")


if __name__ == "__main__":
    main()
