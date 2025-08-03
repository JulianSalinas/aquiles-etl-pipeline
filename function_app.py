import azure.functions as func
import logging
import pandas as pd
import pyodbc
import io
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from common.transforms import (
    infer_and_transform_date, 
    transform_price, 
    remove_special_characters, 
    transform_provider_name, 
    extract_measure_and_unit
)

app = func.FunctionApp()

def get_sql_connection():
    """Get SQLAlchemy engine using Azure Default Credential."""
    # Debug: Log all environment variables related to SQL
    logging.info("Checking SQL environment variables...")
    server = os.environ.get('SQL_SERVER')
    database = os.environ.get('SQL_DATABASE')
    
    logging.info(f"SQL_SERVER: {server}")
    logging.info(f"SQL_DATABASE: {database}")
    
    # Also check if they exist with different casing or prefixes
    all_env_vars = {k: v for k, v in os.environ.items() if 'SQL' in k.upper()}
    logging.info(f"All SQL-related environment variables: {all_env_vars}")
    
    if not all([server, database]):
        raise ValueError(f"Missing environment variables - SQL_SERVER: {server}, SQL_DATABASE: {database}")
    
    try:
        # Get access token using DefaultAzureCredential
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/")
        access_token = token.token
        
        # Try alternative connection string format
        try:
            # Method 1: Direct pyodbc connection string format
            odbc_connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
            )
            
            # Create SQLAlchemy URL from the ODBC connection string
            connection_string = f"mssql+pyodbc:///?odbc_connect={odbc_connection_string}"
            
            logging.info(f"Trying connection method 1 with connection string format")
            
            # Create engine with access token
            engine = create_engine(
                connection_string,
                connect_args={
                    "attrs_before": {
                        1256: access_token.encode('utf-16le')  # SQL_COPT_SS_ACCESS_TOKEN
                    }
                }
            )
            
        except Exception as e1:
            logging.warning(f"Method 1 failed: {e1}, trying method 2")
            
            # Method 2: URL-encoded connection string
            from urllib.parse import quote_plus
            
            odbc_connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
            )
            
            connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_string)}"
            
            logging.info(f"Trying connection method 2 with URL encoding")
            
            # Create engine with access token
            engine = create_engine(
                connection_string,
                connect_args={
                    "attrs_before": {
                        1256: access_token.encode('utf-16le')  # SQL_COPT_SS_ACCESS_TOKEN
                    }
                }
            )
        
        # Test the connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            logging.info(f"Connection test result: {result.fetchone()}")
        
        logging.info("Successfully connected to SQL Database using Azure Default Credential")
        return engine
        
    except Exception as e:
        logging.error(f"Error connecting to SQL Database with Azure Default Credential: {str(e)}")
        raise ValueError("Azure Default Credential failed")

def apply_transformations(df):
    """Apply data transformations to the DataFrame."""
    try:
        # Map column names to standard names if needed
        column_mapping = {
            'Producto': 'Description',
            'Fecha 1': 'LastReviewDt', 
            'Provedor': 'ProviderName',
            'Precio': 'Price'
        }
        
        # Rename columns if they exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # Apply price transformations
        if 'Price' in df.columns:
            df['RawPrice'] = df['Price'].astype(str)
            df['CleanPrice'] = df['Price'].apply(lambda x: transform_price(str(x)) if pd.notna(x) else None)
            df['IsValidPrice'] = df['Price'].notna() & (df['CleanPrice'].notna())
        
        # Apply date transformations
        if 'LastReviewDt' in df.columns:
            df['RawLastReviewDt'] = df['LastReviewDt'].astype(str)
            df['CleanLastReviewDt'] = df['LastReviewDt'].apply(lambda x: infer_and_transform_date(str(x)) if pd.notna(x) else None)
        
        # Apply description transformations
        if 'Description' in df.columns:
            df['RawDescription'] = df['Description'].astype(str)
            df['CleanDescription'] = df['Description'].apply(lambda x: remove_special_characters(str(x)) if pd.notna(x) else None)
            
            # Extract measure and unit information
            measure_unit_data = df['Description'].apply(lambda x: extract_measure_and_unit(str(x)) if pd.notna(x) else (None, None, None))
            df['Measure'] = measure_unit_data.apply(lambda x: x[0] if x else None)
            df['UnitOfMeasure'] = measure_unit_data.apply(lambda x: x[1].lower() if x and x[1] else None)
            df['PackageUnits'] = measure_unit_data.apply(lambda x: x[2] if x else None)
        
        # Apply provider name transformations
        if 'ProviderName' in df.columns:
            df['RawProviderName'] = df['ProviderName'].astype(str)
            df['CleanProviderName'] = df['ProviderName'].apply(lambda x: transform_provider_name(str(x)).title() if pd.notna(x) else None)
        
        # Remove rows with all null values
        df = df.dropna(how='all')
        
        logging.info(f"Applied transformations to {len(df)} rows")
        logging.info(f"Columns after transformation: {list(df.columns)}")
        return df
        
    except Exception as e:
        logging.error(f"Error applying transformations: {str(e)}")
        raise

def write_to_sql_database(df, table_name="ProductsTemp"):
    """Write DataFrame to SQL Database using SQLAlchemy."""
    try:
        engine = get_sql_connection()
        
        # Prepare data for insertion - select only the columns we need
        columns_to_insert = [
            'RawPrice', 'CleanPrice', 'IsValidPrice', 'RawLastReviewDt', 'CleanLastReviewDt',
            'RawDescription', 'CleanDescription', 'Measure', 'UnitOfMeasure', 'PackageUnits',
            'RawProviderName', 'CleanProviderName'
        ]
        
        # Filter DataFrame to only include columns that exist
        available_columns = [col for col in columns_to_insert if col in df.columns]
        df_to_insert = df[available_columns].copy()
        
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
        
        logging.info(f"Successfully wrote {len(df_to_insert)} rows to {table_name} table using SQLAlchemy")
        
    except Exception as e:
        logging.error(f"Error writing to SQL database: {str(e)}")
        raise

@app.blob_trigger(arg_name="myblob", path="products-dev", connection="provider24_STORAGE") 
def provider24_elt_blob_trigger(myblob: func.InputStream):
    """
    Azure Function triggered by blob upload to process CSV files.
    Reads CSV, applies transformations, and writes to SQL Database.
    """
    try:
        logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")
        
        # Read CSV from blob
        csv_content = myblob.read()
        df = pd.read_csv(io.BytesIO(csv_content))
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Columns: {list(df.columns)}")
        
        # Apply transformations
        transformed_df = apply_transformations(df)
        
        # Write to SQL Database
        write_to_sql_database(transformed_df)
        
        logging.info(f"ETL process completed successfully for blob: {myblob.name}")
        
    except Exception as e:
        logging.error(f"Error processing blob {myblob.name}: {str(e)}")
        raise

def get_blob_service_client():
    """Get Azure Blob Service Client using Azure Default Credential."""
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME', 'provider24')
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        
        # Try to use DefaultAzureCredential first
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        
        logging.info("Successfully created Blob Service Client using Azure Default Credential")
        return blob_service_client
        
    except Exception as e:
        logging.error(f"Error creating Blob Service Client with Azure Default Credential: {str(e)}")
        
        # Fallback to connection string if available
        connection_string = os.environ.get('provider24_STORAGE')
        if connection_string:
            logging.info("Falling back to connection string authentication for blob storage")
            return BlobServiceClient.from_connection_string(connection_string)
        else:
            raise ValueError("Both Azure Default Credential and connection string failed for blob storage")

def read_blob_content(container_name, blob_name):
    """Read blob content from Azure Storage."""
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Download blob content
        blob_data = blob_client.download_blob()
        content = blob_data.readall()
        
        logging.info(f"Successfully read blob: {blob_name} from container: {container_name}")
        return content
        
    except Exception as e:
        logging.error(f"Error reading blob {blob_name} from container {container_name}: {str(e)}")
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
        
        if not container_name or not blob_name:
            return func.HttpResponse(
                "Both 'container' and 'blob' parameters are required",
                status_code=400
            )
        
        logging.info(f"Processing request for container: {container_name}, blob: {blob_name}")
        
        # Read blob content
        csv_content = read_blob_content(container_name, blob_name)
        
        # Read CSV from blob content
        df = pd.read_csv(io.BytesIO(csv_content))
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Columns: {list(df.columns)}")
        
        # Apply transformations
        transformed_df = apply_transformations(df)
        
        # Write to SQL Database
        write_to_sql_database(transformed_df)
        
        response_data = {
            "status": "success",
            "message": f"ETL process completed successfully for blob: {blob_name}",
            "rows_processed": len(transformed_df),
            "columns": list(transformed_df.columns)
        }
        
        logging.info(f"ETL process completed successfully for blob: {blob_name}")
        
        return func.HttpResponse(
            body=str(response_data),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        error_message = f"Error processing request: {str(e)}"
        logging.error(error_message)
        
        return func.HttpResponse(
            body=f'{{"status": "error", "message": "{error_message}"}}',
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


