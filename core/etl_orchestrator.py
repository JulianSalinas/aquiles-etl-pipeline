"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import logging
import pandas as pd
import io
from .data_processor import apply_transformations, validate_dataframe, get_processing_summary
from .database import write_to_sql_database, test_database_connection
from .storage import read_blob_content, get_blob_properties


def process_csv_from_blob(container_name, blob_name, server_name, database_name):
    """
    Complete ETL process: read CSV from blob, transform, and write to database.
    
    Args:
        container_name (str): Azure Storage container name
        blob_name (str): Blob name (CSV file)
        server_name (str): SQL Server name
        database_name (str): SQL Database name

    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info(f"Starting ETL process for blob: {container_name}/{blob_name}")
        
        # Step 1: Read blob content
        logging.info("Step 1: Reading blob content...")
        csv_content = read_blob_content(container_name, blob_name)
        blob_properties = get_blob_properties(container_name, blob_name)
        
        # Step 2: Parse CSV
        logging.info("Step 2: Parsing CSV data...")
        df = pd.read_csv(io.BytesIO(csv_content))
        validate_dataframe(df)
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Original columns: {list(df.columns)}")
        
        # Step 3: Apply transformations
        logging.info("Step 3: Applying data transformations...")
        transformed_df = apply_transformations(df)
        
        # Step 4: Write to database
        logging.info("Step 4: Writing to SQL database...")
        rows_written = write_to_sql_database(transformed_df, table_name)
        
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


def process_csv_from_stream(csv_stream, blob_name, server_name, database_name):
    """
    ETL process for CSV data from an input stream (for blob triggers).
    
    Args:
        csv_stream: Input stream containing CSV data
        blob_name (str): Name of the source blob (for logging)
        server_name (str): SQL Server name
        database_name (str): SQL Database name

    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info(f"Starting ETL process for blob stream: {blob_name}")
        
        # Step 1: Parse CSV from stream
        logging.info("Step 1: Parsing CSV data from stream...")
        df = pd.read_csv(io.BytesIO(csv_stream))
        validate_dataframe(df)
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Original columns: {list(df.columns)}")
        
        logging.info("Step 2: Applying data transformations...")
        transformed_df = apply_transformations(df)
        
        logging.info("Step 3: Writing to SQL database...")
        rows_written = write_to_sql_database(transformed_df, server_name, database_name)

        logging.info(f"ETL process completed successfully for blob: {blob_name}")
        
        return {
            "success": True,
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