import os
import urllib
import struct
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from azure.identity import DefaultAzureCredential


def setup_environment():
    """Set up environment variables for testing."""
    os.environ['SQL_SERVER'] = 'provider24-dev.database.windows.net'
    os.environ['SQL_DATABASE'] = 'provider24'

def get_connection_string(server_name, database_name):
    driver_name = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'Driver={driver_name};Server=tcp:{server_name},1433;Database={database_name};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30'
    params = urllib.parse.quote(connection_string)
    return f"mssql+pyodbc:///?odbc_connect={params}"

def get_azure_access_token():
    credential = DefaultAzureCredential()
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return { SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def create_azure_sql_engine(server_name, database_name):
    """Create SQLAlchemy engine with Azure AD authentication."""
    url = get_connection_string(server_name, database_name)
    token = get_azure_access_token()
    return create_engine(url, connect_args={'attrs_before': token})

def initialize_database(engine):
    """Initialize database schema."""
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return Base

def test_azure_sql_connection():
    """Test Azure AD authentication and database access."""
    setup_environment()
    server_name = os.environ['SQL_SERVER']
    database_name = os.environ['SQL_DATABASE']
    engine = create_azure_sql_engine(server_name, database_name)
    initialize_database(engine)
    
    try:
        with engine.connect() as conn:
            table_count = conn.execute(text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")).fetchone()
            print(f"Connection successful!")
            print(f"Database has {table_count[0]} tables")
            conn.close()

        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM UnitOfMeasure")).fetchone()
            print(f"Connection successful!")
            print(f"There are: {result[0]} units of measure")
            conn.close()
                   
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_azure_sql_connection()
