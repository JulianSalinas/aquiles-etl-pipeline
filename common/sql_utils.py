"""
SQL utilities for Azure SQL Database connectivity with Azure AD authentication.
"""
import urllib
import struct
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from azure.identity import DefaultAzureCredential


def get_connection_string(server_name, database_name):
    """Generate ODBC connection string for Azure SQL Database."""
    driver_name = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'Driver={driver_name};Server=tcp:{server_name},1433;Database={database_name};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30'
    params = urllib.parse.quote(connection_string)
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_azure_access_token():
    """Get Azure AD access token for SQL Database authentication."""
    credential = DefaultAzureCredential()
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def create_azure_sql_engine(server_name, database_name):
    """Create SQLAlchemy engine with Azure AD authentication."""
    url = get_connection_string(server_name, database_name)
    token = get_azure_access_token()
    return create_engine(url, connect_args={'attrs_before': token})


def test_connection(engine):
    """Test SQL connection with a simple query."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 as test_value")).fetchone()
        return result


def initialize_database(engine):
    """Initialize database schema."""
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return Base
