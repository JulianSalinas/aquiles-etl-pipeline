"""
Database operations module for SQL Server integration.
"""
import logging
import pandas as pd
from common.sql_utils import create_azure_sql_engine, ensure_connection_established, initialize_database
import os


def get_sql_connection():
    """Get SQL connection engine using environment variables."""
    server_name = os.environ.get('SQL_SERVER', 'provider24-dev.database.windows.net')
    database_name = os.environ.get('SQL_DATABASE', 'provider24')
    return create_azure_sql_engine(server_name, database_name)


def write_to_sql_database(df, table_name="ProductsTemp"):
    """Write DataFrame to SQL Database using SQLAlchemy."""
    try:
        # Get SQL connection using common utilities
        engine = get_sql_connection()
        
        # Initialize database schema if needed
        initialize_database(engine)
        
        # Test connection before proceeding
        result = ensure_connection_established(engine)
        logging.info(f"✅ SQL connection verified: {result}")
        
        # Prepare data for insertion - select only the columns we need
        columns_to_insert = [
            'RawPrice', 'CleanPrice', 'IsValidPrice', 'RawLastReviewDt', 'CleanLastReviewDt',
            'RawDescription', 'CleanDescription', 'TransformedDescription', 'Measure', 'UnitOfMeasure', 'PackageUnits',
            'RawProviderName', 'CleanProviderName'
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


def test_database_connection():
    """Test database connection and return status."""
    try:
        engine = get_sql_connection()
        result = ensure_connection_established(engine)
        logging.info(f"Database connection test successful: {result}")
        return True
    except Exception as e:
        logging.error(f"Database connection test failed: {str(e)}")
        return False


def get_database_info():
    """Get database information and statistics."""
    try:
        engine = get_sql_connection()
        
        with engine.connect() as conn:
            # Get table count
            table_count_query = "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            table_count = conn.execute(table_count_query).fetchone()[0]
            
            # Get database name
            db_name_query = "SELECT DB_NAME() as db_name"
            db_name = conn.execute(db_name_query).fetchone()[0]
            
            return {
                "database_name": db_name,
                "table_count": table_count,
                "connection_status": "healthy"
            }
    except Exception as e:
        logging.error(f"Error getting database info: {str(e)}")
        return {
            "database_name": "unknown",
            "table_count": 0,
            "connection_status": "error",
            "error": str(e)
        }
