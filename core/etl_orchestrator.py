"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import logging
import pandas as pd
import io
from .data_processor import apply_transformations, validate_dataframe, get_processing_summary
from .database import write_to_sql_database, test_database_connection
from .storage import read_blob_content, get_blob_properties


def process_csv_from_blob(container_name, blob_name, table_name="ProductsTemp"):
    """
    Complete ETL process: read CSV from blob, transform, and write to database.
    
    Args:
        container_name (str): Azure Storage container name
        blob_name (str): Blob name (CSV file)
        table_name (str): Target SQL table name
    
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
        
        # Step 5: Generate summary
        summary = get_processing_summary(transformed_df)
        
        result = {
            "status": "success",
            "message": f"ETL process completed successfully for blob: {blob_name}",
            "blob_info": blob_properties,
            "rows_processed": len(transformed_df),
            "rows_written": rows_written,
            "table_name": table_name,
            "processing_summary": summary
        }
        
        logging.info(f"ETL process completed successfully for blob: {blob_name}")
        return result
        
    except Exception as e:
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": "error",
            "message": error_message,
            "blob_name": blob_name,
            "container_name": container_name,
            "error": str(e)
        }


def process_csv_from_stream(csv_stream, blob_name, table_name="ProductsTemp"):
    """
    ETL process for CSV data from an input stream (for blob triggers).
    
    Args:
        csv_stream: Input stream containing CSV data
        blob_name (str): Name of the source blob (for logging)
        table_name (str): Target SQL table name
    
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
        
        # Step 2: Apply transformations
        logging.info("Step 2: Applying data transformations...")
        transformed_df = apply_transformations(df)
        
        # Step 3: Write to database
        logging.info("Step 3: Writing to SQL database...")
        rows_written = write_to_sql_database(transformed_df, table_name)
        
        # Step 4: Generate summary
        summary = get_processing_summary(transformed_df)
        
        result = {
            "status": "success",
            "message": f"ETL process completed successfully for blob: {blob_name}",
            "rows_processed": len(transformed_df),
            "rows_written": rows_written,
            "table_name": table_name,
            "processing_summary": summary
        }
        
        logging.info(f"ETL process completed successfully for blob: {blob_name}")
        return result
        
    except Exception as e:
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": "error",
            "message": error_message,
            "blob_name": blob_name,
            "error": str(e)
        }


def get_pipeline_health():
    """Get overall pipeline health status."""
    try:
        # Test database connection
        db_healthy = test_database_connection()
        
        # TODO: Add blob storage health check
        # TODO: Add other health checks as needed
        
        return {
            "status": "healthy" if db_healthy else "unhealthy",
            "database": "healthy" if db_healthy else "error",
            "storage": "unknown",  # Will implement later
            "timestamp": pd.Timestamp.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Error checking pipeline health: {str(e)}")
        return {
            "status": "error",
            "database": "error",
            "storage": "error", 
            "error": str(e),
            "timestamp": pd.Timestamp.now().isoformat()
        }
