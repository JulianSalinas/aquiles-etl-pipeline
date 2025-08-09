"""
Database operations module for SQL Server integration with Azure AD authentication.
Merged from sql_utils.py for consolidated database functionality.
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
    engine = create_engine(url, connect_args={'attrs_before': token})
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return engine

def ensure_connection_established(engine, max_retries=3):
    """
    Test SQL connection with a simple query and awaken database if needed.
    
    The database has a feature to turn off itself. When the database is turned-off 
    it needs to be awaken to work. Therefore, make a dummy request to the database 
    and expect for it to fail the first time, then try again until the database is awaken.
    
    Args:
        engine: SQLAlchemy engine
        max_retries: Maximum number of retry attempts
        
    Returns:
        Query result if successful, None if all retries failed
    """
    import time
    import logging
    
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value")).fetchone()
                if attempt > 0:
                    logging.info(f"Database connection established after {attempt + 1} attempts")
                return result
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                logging.warning(f"Database connection attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Database connection failed after {max_retries} attempts: {str(e)}")
                raise
    
    return None
    
