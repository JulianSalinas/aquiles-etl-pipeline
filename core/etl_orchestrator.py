"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import logging
import pandas as pd
import io
import csv
from datetime import datetime
from .data_processor import apply_transformations
from .database import create_azure_sql_engine, ensure_connection_established
from .storage import read_blob_content, get_blob_service_client, upload_blob_content



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

def write_to_sql_database(df, server_name, database_name, table_name):
    """Write DataFrame to SQL Database using SQLAlchemy."""
    try:
        engine = create_azure_sql_engine(server_name, database_name)
        
        result = ensure_connection_established(engine)

        if result is None:
            raise ValueError("Failed to establish connection to SQL Database")
        
        columns_to_insert = [
            'RawPrice', 
            'CleanPrice', 
            'IsValidPrice', 
            'RawLastReviewDt', 
            'CleanLastReviewDt',
            'RawDescription', 
            'CleanDescription', 
            'Measure', 
            'UnitOfMeasure', 
            'PackageUnits',
            'RawProviderName', 
            'CleanProviderName',
            'PercentageIVA'
        ]
        
        # Filter DataFrame to only include columns that exist
        available_columns = [col for col in columns_to_insert if col in df.columns]
        df_to_insert = df[available_columns].copy()
        
        # Convert boolean to int for SQL Server compatibility
        if 'IsValidPrice' in df_to_insert.columns:
            df_to_insert['IsValidPrice'] = df_to_insert['IsValidPrice'].astype(int)
        
        df_to_insert.to_sql(table_name, engine, if_exists='append', index=False)
        
        logging.info(f"✅ Successfully wrote {len(df_to_insert)} rows to {table_name} table")

        return len(df_to_insert)
        
    except Exception as e:
        logging.error(f"❌ Error writing to SQL database: {str(e)}")
        raise


def extract_invoice_data_from_image(image_content, image_name):
    """
    Extract invoice data from image content.
    This is a mock implementation that simulates OCR processing.
    In a real implementation, this would use Azure Computer Vision or Form Recognizer.
    
    Args:
        image_content: Binary content of the image
        image_name: Name of the image file
    
    Returns:
        list: List of product dictionaries extracted from the invoice
    """
    try:
        logging.info(f"Processing invoice image: {image_name}")
        
        # Mock OCR extraction - in reality this would use Azure Computer Vision/Form Recognizer
        # For demonstration, we'll return mock invoice data based on the image name
        
        # Extract basic info from filename or use defaults
        if "factura" in image_name.lower() or "invoice" in image_name.lower():
            # Mock extracted products from invoice
            mock_products = [
                {
                    "Producto": "Arroz Premium 1kg",
                    "Provedor": "Distribuidora San Juan",
                    "Precio": "2500.00",
                    "Porcentaje de IVA": "19"
                },
                {
                    "Producto": "Aceite Vegetal 500ml", 
                    "Provedor": "Distribuidora San Juan",
                    "Precio": "4200.00",
                    "Porcentaje de IVA": "19"
                }
            ]
        else:
            # Default single product
            mock_products = [
                {
                    "Producto": "Producto Generico",
                    "Provedor": "Proveedor Generico", 
                    "Precio": "1000.00",
                    "Porcentaje de IVA": "19"
                }
            ]
        
        logging.info(f"Extracted {len(mock_products)} products from invoice {image_name}")
        return mock_products
        
    except Exception as e:
        logging.error(f"Error extracting data from invoice image {image_name}: {str(e)}")
        raise


def generate_csv_from_invoice_data(products_data, trigger_date):
    """
    Generate CSV content from extracted invoice data.
    
    Args:
        products_data: List of product dictionaries
        trigger_date: Date when the trigger was invoked
    
    Returns:
        str: CSV content as string
    """
    try:
        # Create CSV content with required columns
        output = io.StringIO()
        fieldnames = ["Producto", "Fecha 1", "Provedor", "Precio", "Porcentaje de IVA"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write product rows
        for product in products_data:
            row = {
                "Producto": product.get("Producto", ""),
                "Fecha 1": trigger_date,
                "Provedor": product.get("Provedor", ""), 
                "Precio": product.get("Precio", ""),
                "Porcentaje de IVA": product.get("Porcentaje de IVA", "")
            }
            writer.writerow(row)
        
        csv_content = output.getvalue()
        output.close()
        
        logging.info(f"Generated CSV with {len(products_data)} product rows")
        return csv_content
        
    except Exception as e:
        logging.error(f"Error generating CSV from invoice data: {str(e)}")
        raise


def process_invoice_image(image_content, image_name, storage_account_name, output_container):
    """
    Complete invoice processing pipeline: extract data from image, generate CSV, and upload to storage.
    
    Args:
        image_content: Binary content of the invoice image
        image_name: Name of the image file
        storage_account_name: Azure Storage account name
        output_container: Container name where CSV should be uploaded
    
    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info(f"Starting invoice processing for image: {image_name}")
        
        # Extract data from invoice image
        logging.info("Extracting data from invoice image...")
        products_data = extract_invoice_data_from_image(image_content, image_name)
        
        # Generate timestamp for the CSV
        trigger_date = datetime.now().strftime("%Y-%m-%d")
        
        # Generate CSV content
        logging.info("Generating CSV from extracted data...")
        csv_content = generate_csv_from_invoice_data(products_data, trigger_date)
        
        # Create output filename
        base_name = image_name.rsplit('.', 1)[0]  # Remove file extension
        csv_filename = f"{base_name}_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Upload CSV to storage
        logging.info(f"Uploading CSV to {output_container} container...")
        blob_service_client = get_blob_service_client(storage_account_name)
        upload_blob_content(blob_service_client, output_container, csv_filename, csv_content)
        
        logging.info(f"Invoice processing completed successfully for: {image_name}")

        return {
            "status": True,
            "message": f"Invoice processing completed successfully for: {image_name}",
            "products_extracted": len(products_data),
            "csv_filename": csv_filename,
            "output_container": output_container
        }
        
    except Exception as e:
        error_message = f"Invoice processing failed for {image_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }