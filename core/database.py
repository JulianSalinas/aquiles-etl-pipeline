import logging
import struct
import time
import urllib
import urllib.parse

from azure.identity import DefaultAzureCredential
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import declarative_base


def get_connection_string(server_name: str, database_name: str) -> str:
    """Generate ODBC connection string for Azure SQL Database."""
    driver_name = '{ODBC Driver 17 for SQL Server}'
    connection_string: str = f'Driver={driver_name};Server=tcp:{server_name},1433;Database={database_name};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30'
    params: str = urllib.parse.quote(connection_string)
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_azure_access_token() -> dict[int, bytes]:
    """Get Azure AD access token for SQL Database authentication."""
    credential = DefaultAzureCredential()
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def create_azure_sql_engine(server_name: str, database_name: str) -> Engine:
    """Create SQLAlchemy engine with Azure AD authentication."""
    url: str = get_connection_string(server_name, database_name)
    token: dict[int, bytes] = get_azure_access_token()
    engine: Engine = create_engine(url, connect_args={'attrs_before': token})
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return engine

def ensure_connection_established(engine: Engine, retries_left: int = 3) -> None:
    """
    Test SQL connection with a simple query and awaken database if needed using recursion.
    """
    if retries_left <= 0:
        raise ValueError("Could not establish database connection.")
    try:
        with engine.connect() as conn: conn.execute(text("SELECT 1 AS V")).fetchone()
    except Exception as e:
        wait_time = 2 ** (2 - retries_left)  # Exponential backoff: 1, 2 seconds
        logging.warning(f"Database connection failed: {str(e)}. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)
        ensure_connection_established(engine, retries_left - 1)
