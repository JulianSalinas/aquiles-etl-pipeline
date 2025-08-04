import os
import logging
import azure.functions as func
from core.etl_orchestrator import process_csv_from_blob, process_csv_from_stream

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
            logging.info(f"ETL process completed successfully for blob: {myblob.name}")
        else:
            logging.error(f"ETL process failed for blob: {myblob.name} - {result['message']}")
            raise Exception(result["message"])
        
    except Exception as e:
        logging.error(f"Error processing blob {myblob.name}: {str(e)}")
        raise


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
            logging.info(f"ETL process completed successfully for blob: {blob_name}")
        else:
            logging.error(f"ETL process failed for blob: {blob_name} - {result['message']}")
            raise Exception(result["message"])
        
        
    except Exception as e:
        logging.error(f"Error processing blob {blob_name}: {str(e)}")
        raise