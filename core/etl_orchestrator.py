"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import logging
import pandas as pd
import io
from .data_processor import apply_transformations
from .database import write_to_sql_database
from .storage import read_blob_content, get_blob_service_client


def process_csv_from_stream(csv_data, blob_name, server_name, database_name, table_name):
    """
    Internal function to process CSV data (common logic for both blob and stream processing).
    
    Args:
        csv_data: CSV data as bytes or stream
        blob_name (str): Name of the source blob (for logging)
        server_name (str): SQL Server name
        database_name (str): SQL Database name
        table_name (str): Target database table name

    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info("Parsing CSV data...")
        df = pd.read_csv(io.BytesIO(csv_data))
        
        if df.empty:
            raise ValueError("CSV file is empty")
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Original columns: {list(df.columns)}")
        
        logging.info("Applying data transformations...")
        transformed_df = apply_transformations(df)
        
        logging.info("Writing to SQL database...")
        rows_written = write_to_sql_database(transformed_df, server_name, database_name, table_name)
        
        logging.info(f"ETL process completed successfully for blob: {blob_name}")

        return {
            "status": True,
            "message": f"ETL process completed successfully for blob: {blob_name}",
            "rows_processed": len(transformed_df),
            "rows_written": rows_written
        }
        
    except Exception as e:
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }


def process_csv_from_blob(storage_account_name, container_name, blob_name, server_name, database_name, table_name):
    """
    Complete ETL process: read CSV from blob, transform, and write to database.
    
    Args:
        container_name (str): Azure Storage container name
        blob_name (str): Blob name (CSV file)
        server_name (str): SQL Server name (optional, uses environment if not provided)
        database_name (str): SQL Database name (optional, uses environment if not provided)
        table_name (str): Target database table name

    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info(f"Starting ETL process for blob: {container_name}/{blob_name}")
        
        logging.info("Reading blob content...")

        blob_service_client = get_blob_service_client(storage_account_name)

        csv_content = read_blob_content(blob_service_client, container_name, blob_name)

        return process_csv_from_stream(csv_content, blob_name, server_name, database_name, table_name)

    except Exception as e:
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }