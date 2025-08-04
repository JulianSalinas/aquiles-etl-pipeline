import os
import logging
import azure.functions as func
from core.etl_orchestrator import process_csv_from_blob, process_csv_from_stream, process_invoice_image

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="products-dev", connection="provider24_STORAGE") 
def provider24_elt_blob_trigger(myblob: func.InputStream):
    """
    Azure Function triggered by blob upload to process CSV files.
    Reads CSV, applies transformations, and writes to SQL Database.
    """
    try:
        logging.info(f"BLOB TRIGGER - Processing blob: {myblob.name}")

        server_name = os.environ.get('SQL_SERVER')
        database_name = os.environ.get('SQL_DATABASE')
        
        csv_content = myblob.read()
        
        result = process_csv_from_stream(csv_content, myblob.name, server_name, database_name, "ProductsStep1")

        if result["status"] == True:
            success_msg = f"ETL process completed successfully for blob: {myblob.name}"
            logging.info(success_msg)
        else:
            error_msg = f"ETL process failed for blob: {myblob.name} - {result['message']}"
            logging.error(error_msg)
        
    except Exception as e:
        error_msg = f"Error processing blob {myblob.name}: {str(e)}"
        logging.error(error_msg)


@app.route(route="process-csv", methods=["POST"])
def provider24_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP triggered function to process CSV files from Azure Blob Storage.
    Expects JSON body with: {"container": "container-name", "blob": "blob-name.csv"}
    """
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        server_name = os.environ.get('SQL_SERVER')
        database_name = os.environ.get('SQL_DATABASE')

        req_body = req.get_json()
        container_name = req_body.get('container')
        blob_name = req_body.get('blob')
    
        logging.info(f"HTTP TRIGGER - Processing blob: {container_name}/{blob_name}")

        result = process_csv_from_blob(storage_account_name, container_name, blob_name, server_name, database_name, "ProductsStep1")

        if result["status"] == True:
            success_msg = f"ETL process completed successfully for blob: {blob_name}. Rows processed: {result.get('rows_processed', 'N/A')}"
            logging.info(success_msg)
            return func.HttpResponse(success_msg,status_code=200)
        else:
            error_msg = f"ETL process failed for blob: {blob_name} - {result['message']}"
            logging.error(error_msg)
            return func.HttpResponse(error_msg, status_code=400)

    except Exception as e:
        error_msg = f"Error processing blob {blob_name if 'blob_name' in locals() else 'unknown'}: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)


@app.blob_trigger(arg_name="invoice_blob", path="invoices", connection="provider24_STORAGE")
def invoice_processor_blob_trigger(invoice_blob: func.InputStream):
    """
    Azure Function triggered by blob upload to process invoice images.
    Extracts product data from invoice images and generates CSV files.
    """
    try:
        logging.info(f"INVOICE TRIGGER - Processing invoice image: {invoice_blob.name}")

        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        
        image_content = invoice_blob.read()
        
        result = process_invoice_image(image_content, invoice_blob.name, storage_account_name, "products-dev")

        if result["status"] == True:
            success_msg = f"Invoice processing completed successfully for: {invoice_blob.name}"
            logging.info(success_msg)
        else:
            error_msg = f"Invoice processing failed for: {invoice_blob.name} - {result['message']}"
            logging.error(error_msg)
        
    except Exception as e:
        error_msg = f"Error processing invoice {invoice_blob.name}: {str(e)}"
        logging.error(error_msg)