import logging
import os
from typing import Any

import azure.functions as func

from core.etl_orchestrator import (ProcessingResult, process_csv_from_blob,
                                   process_csv_from_stream,
                                   process_invoice_image)

app = func.FunctionApp()

@app.route(route="process-csv", methods=["POST"])
def provider24_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP triggered function to process CSV files from Azure Blob Storage.
    Expects JSON body with: {"container": "container-name", "blob": "blob-name.csv"}
    """
    req_body: Any = req.get_json()

    container_name: str = req_body.get('container') or 'unknown-container'

    blob_name: str = req_body.get('blob') or 'unknown-blob'

    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')

        if not storage_account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")

        server_name = os.environ.get('SQL_SERVER')

        if not server_name:
            raise ValueError("SQL_SERVER environment variable is not set")

        database_name = os.environ.get('SQL_DATABASE')

        if not database_name:
            raise ValueError("SQL_DATABASE environment variable is not set")
    
        logging.info(f"HTTP TRIGGER - Processing blob: {container_name}/{blob_name}")

        result: ProcessingResult = process_csv_from_blob(storage_account_name, container_name, blob_name, server_name, database_name)

        if result.status == True:
            success_msg = f"ETL process completed successfully for blob: {blob_name}"
            logging.info(success_msg)
            return func.HttpResponse(success_msg,status_code=200)
        else:
            error_msg = f"ETL process failed for blob: {blob_name} - {result.message}"
            logging.error(error_msg)
            return func.HttpResponse(error_msg, status_code=400)

    except Exception as e:
        error_msg = f"Error processing blob {blob_name}: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)
    
@app.blob_trigger(arg_name="myblob", path="products-dev", connection="provider24_STORAGE")  # type: ignore
def provider24_elt_blob_trigger(myblob: func.InputStream):
    """
    Azure Function triggered by blob upload to process CSV files.
    Reads CSV, applies transformations, and writes to SQL Database.
    """
    try:
        logging.info(f"BLOB TRIGGER - Processing blob: {myblob.name}")

        server_name = os.environ.get('SQL_SERVER')

        if not server_name:
            raise ValueError("SQL_SERVER environment variable is not set")

        database_name = os.environ.get('SQL_DATABASE')

        if not database_name:
            raise ValueError("SQL_DATABASE environment variable is not set")

        csv_content = myblob.read()
        
        assert isinstance(myblob.name, str)

        result: ProcessingResult = process_csv_from_stream(csv_content, myblob.name, server_name, database_name)

        if result.status == True:
            success_msg = f"ETL process completed successfully for blob: {myblob.name}"
            logging.info(success_msg)
        else:
            error_msg = f"ETL process failed for blob: {myblob.name} - {result.message}"
            logging.error(error_msg)
        
    except Exception as e:
        error_msg = f"Error processing blob {myblob.name}: {str(e)}"
        logging.error(error_msg)


@app.blob_trigger(arg_name="invoice_blob", path="invoices-dev", connection="provider24_STORAGE") # type: ignore
def invoice_processor_blob_trigger(invoice_blob: func.InputStream):
    """
    Azure Function triggered by blob upload to process invoice images.
    Uses optimized direct processing that bypasses CSV generation and storage.
    """
    try:
        logging.info(f"INVOICE TRIGGER - Processing invoice image: {invoice_blob.name}")

        server_name = os.environ.get('SQL_SERVER')

        if not server_name:
            raise ValueError("SQL_SERVER environment variable is not set")

        database_name = os.environ.get('SQL_DATABASE')

        if not database_name:
            raise ValueError("SQL_DATABASE environment variable is not set")

        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')

        if not storage_account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")

        image_content = invoice_blob.read()

        assert isinstance(invoice_blob.name, str)

        # Extract container name from the blob trigger path or use default
        container = "invoices-dev"

        result: ProcessingResult = process_invoice_image(storage_account_name, container, image_content, invoice_blob.name, server_name, database_name)

        if result.status == True:
            success_msg = f"Invoice processing completed successfully for: {invoice_blob.name}"
            logging.info(success_msg)
            return func.HttpResponse(status_code=200, body=success_msg)
        
        else:
            error_msg = f"Invoice processing failed for: {invoice_blob.name} - {result.message}"
            logging.error(error_msg)
            return func.HttpResponse(status_code=500, body=error_msg)

    except Exception as e:
        error_msg = f"Error processing invoice {invoice_blob.name}: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(status_code=500, body=error_msg)