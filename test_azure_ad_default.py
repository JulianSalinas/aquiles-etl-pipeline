import os
import pyodbc
import urllib
import struct
from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.declarative import declarative_base
from urllib.parse import quote_plus
from azure.identity import DefaultAzureCredential

# Set environment variables
os.environ['SQL_SERVER'] = 'provider24-dev.database.windows.net'
os.environ['SQL_DATABASE'] = 'provider24'

def get_sql_engine():
    server_name = os.environ['SQL_SERVER']
    database_name = os.environ['SQL_DATABASE']
    driver_name = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'Driver={driver_name};Server=tcp:{server_name},1433;Database={database_name};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30'
    params = urllib.parse.quote(connection_string)
    url = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(url)

Base = declarative_base()
engine = get_sql_engine()

# from https://docs.sqlalchemy.org/en/20/core/engines.html#generating-dynamic-authentication-tokens
@event.listens_for(engine, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    """Called before the engine creates a new connection. Injects an EntraID token into the connection parameters."""
    credential = DefaultAzureCredential()
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

# set up the database
Base.metadata.create_all(engine)

def test_azure_ad_default():
    """Test Azure AD Default authentication (uses existing Azure CLI login)."""
    

    # Test SQLAlchemy with Azure AD Default
    print("1. Testing SQLAlchemy with Azure AD Default")
    try:
        print("   Attempting connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM UnitOfMeasure")).fetchone()
            print(f"SUCCESS: {result}")
            
            # Test a simple query to make sure permissions work
            print("   Testing table access...")
            try:
                result2 = conn.execute(text("SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES")).fetchone()
                print(f"Table access: Found {result2[0]} tables")
            except Exception as e:
                print(f" Limited access: {e}")
            
            return True
    except Exception as e:
        print(f"FAILED: {e}")
        return False

if __name__ == "__main__":
    print("Azure AD Default Connection Test")
    print("=" * 50)
    
    success = test_azure_ad_default()
    
    if success:
        print("\n✅ Connection successful! You can now update function_app.py")
        print("💡 This method will work both locally (with Azure CLI) and in Azure (with Managed Identity)")
    else:
        print("\n❌ Connection failed.")
        print("\nTroubleshooting steps:")
        print("1. Check if your Azure account has access to the database")
        print("2. Verify your IP is allowed in the SQL Server firewall")
        print("3. Make sure the database exists and is accessible")
        print("4. Try: az sql server firewall-rule create --resource-group <rg> --server provider24-dev --name AllowMyIP --start-ip-address <your-ip> --end-ip-address <your-ip>")
