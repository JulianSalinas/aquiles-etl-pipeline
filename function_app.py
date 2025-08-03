import azure.functions as func
import logging
import json
from datetime import datetime
from core.etl_orchestrator import process_csv_from_blob, process_csv_from_stream, get_pipeline_health
app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="products-dev", connection="provider24_STORAGE") 
def provider24_elt_blob_trigger(myblob: func.InputStream):
    """
    Azure Function triggered by blob upload to process CSV files.
    Reads CSV, applies transformations, and writes to SQL Database.
    """
    try:
        logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")
        
        # Read CSV from blob stream
        csv_content = myblob.read()
        
        # Use core ETL orchestrator to process the data
        result = process_csv_from_stream(csv_content, myblob.name)
        
        if result["status"] == "success":
            logging.info(f"ETL process completed successfully for blob: {myblob.name}")
            logging.info(f"Processed {result['rows_processed']} rows, wrote {result['rows_written']} rows")
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
        logging.info("HTTP trigger function received a request")
        
        # Parse request body
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                "Invalid JSON in request body",
                status_code=400
            )
        
        if not req_body:
            return func.HttpResponse(
                "Request body is required with 'container' and 'blob' parameters",
                status_code=400
            )
        
        container_name = req_body.get('container')
        blob_name = req_body.get('blob')
        table_name = req_body.get('table_name', 'ProductsTemp')  # Optional table name
        
        if not container_name or not blob_name:
            return func.HttpResponse(
                "Both 'container' and 'blob' parameters are required",
                status_code=400
            )
        
        logging.info(f"Processing request for container: {container_name}, blob: {blob_name}")
        
        # Use core ETL orchestrator to process the data
        result = process_csv_from_blob(container_name, blob_name, table_name)
        
        if result["status"] == "success":
            logging.info(f"ETL process completed successfully for blob: {blob_name}")
            
            return func.HttpResponse(
                body=json.dumps(result),
                status_code=200,
                headers={"Content-Type": "application/json"}
            )
        else:
            logging.error(f"ETL process failed: {result['message']}")
            
            return func.HttpResponse(
                body=json.dumps(result),
                status_code=500,
                headers={"Content-Type": "application/json"}
            )
        
    except Exception as e:
        error_result = {
            "status": "error",
            "message": f"Error processing request: {str(e)}"
        }
        logging.error(error_result["message"])
        
        return func.HttpResponse(
            body=json.dumps(error_result),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint for monitoring pipeline status.
    """
    try:
        logging.info("Health check requested")
        
        # Get pipeline health status
        health_status = get_pipeline_health()
        
        status_code = 200 if health_status["status"] == "healthy" else 503
        
        return func.HttpResponse(
            body=json.dumps(health_status),
            status_code=status_code,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        
        message = f"Health check failed: {str(e)}"

        error_result = {
            "status": 500,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }

        logging.error(error_result["message"])
        
        return func.HttpResponse(
            body=json.dumps(error_result),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )
