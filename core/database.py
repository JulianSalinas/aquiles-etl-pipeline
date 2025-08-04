"""
Database operations module for SQL Server integration with Azure AD authentication.
Merged from sql_utils.py for consolidated database functionality.
"""
import logging
import pandas as pd
import urllib
import struct
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from azure.identity import DefaultAzureCredential
import os


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

def ensure_connection_established(engine):
    """Test SQL connection with a simple query."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 as test_value")).fetchone()
        return result

def write_to_sql_database(df, server_name, database_name, table_name="ProductsTemp"):
    """Write DataFrame to SQL Database using SQLAlchemy."""
    try:
        engine = create_azure_sql_engine(server_name, database_name)
        
        # Test connection before proceeding
        result = ensure_connection_established(engine)
        logging.info(f"✅ SQL connection verified: {result}")
        
        # Prepare data for insertion - select only the columns we need
        columns_to_insert = [
            'RawPrice', 
            'CleanPrice', 
            'IsValidPrice', 
            'RawLastReviewDt', 
            'CleanLastReviewDt',
            'RawDescription', 
            'CleanDescription', 
            'TransformedDescription', 
            'Measure', 
            'UnitOfMeasure', 
            'PackageUnits',
            'RawProviderName', 
            'CleanProviderName'
        ]
        
        # Filter DataFrame to only include columns that exist
        available_columns = [col for col in columns_to_insert if col in df.columns]
        df_to_insert = df[available_columns].copy()
        
        logging.info(f"Preparing to insert {len(df_to_insert)} rows with columns: {available_columns}")
        
        # Convert boolean to int for SQL Server compatibility
        if 'IsValidPrice' in df_to_insert.columns:
            df_to_insert['IsValidPrice'] = df_to_insert['IsValidPrice'].astype(int)
        
        # Use pandas to_sql method for efficient bulk insert
        rows_inserted = df_to_insert.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logging.info(f"✅ Successfully wrote {len(df_to_insert)} rows to {table_name} table")
        return len(df_to_insert)
        
    except Exception as e:
        logging.error(f"❌ Error writing to SQL database: {str(e)}")
        raise