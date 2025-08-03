import azure.functions as func
import logging
import pandas as pd
import pyodbc
import io
import os
from azure.identity import DefaultAzureCredential
from common.transforms import (
    infer_and_transform_date, 
    transform_price, 
    remove_special_characters, 
    transform_provider_name, 
    extract_measure_and_unit
)

app = func.FunctionApp()

def get_sql_connection():
    """Get SQL Server connection using Azure Default Credential."""
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
        
        # Build connection string with access token
        connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        
        # Create connection attributes for access token authentication
        attrs_before = {
            1256: access_token.encode('utf-16le')  # SQL_COPT_SS_ACCESS_TOKEN
        }
        
        # Create connection with access token
        conn = pyodbc.connect(connection_string, attrs_before=attrs_before)
        
        logging.info("Successfully connected to SQL Database using Azure Default Credential")
        return conn
        
    except Exception as e:
        logging.error(f"Error connecting to SQL Database with Azure Default Credential: {str(e)}")
        # Fallback to username/password if available
        username = os.environ.get('SQL_USERNAME')
        password = os.environ.get('SQL_PASSWORD')
        
        if username and password:
            logging.info("Falling back to username/password authentication")
            connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            return pyodbc.connect(connection_string)
        else:
            raise ValueError("Azure Default Credential failed and no username/password provided as fallback")

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
    """Write DataFrame to SQL Database."""
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Create table if it doesn't exist (basic structure)
        create_table_sql = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            RawPrice NVARCHAR(255),
            CleanPrice DECIMAL(18,2),
            IsValidPrice BIT,
            RawLastReviewDt NVARCHAR(255),
            CleanLastReviewDt DATE,
            RawDescription NVARCHAR(MAX),
            CleanDescription NVARCHAR(MAX),
            Measure NVARCHAR(50),
            UnitOfMeasure NVARCHAR(50),
            PackageUnits NVARCHAR(50),
            RawProviderName NVARCHAR(255),
            CleanProviderName NVARCHAR(255),
            ProcessedDt DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_sql)
        
        # Insert data
        rows_inserted = 0
        for index, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {table_name} (
                RawPrice, CleanPrice, IsValidPrice, RawLastReviewDt, CleanLastReviewDt,
                RawDescription, CleanDescription, Measure, UnitOfMeasure, PackageUnits,
                RawProviderName, CleanProviderName
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(insert_sql, (
                row.get('RawPrice'),
                float(row.get('CleanPrice')) if row.get('CleanPrice') is not None else None,
                row.get('IsValidPrice'),
                row.get('RawLastReviewDt'),
                row.get('CleanLastReviewDt'),
                row.get('RawDescription'),
                row.get('CleanDescription'),
                row.get('Measure'),
                row.get('UnitOfMeasure'),
                row.get('PackageUnits'),
                row.get('RawProviderName'),
                row.get('CleanProviderName')
            ))
            rows_inserted += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully wrote {rows_inserted} rows to {table_name} table")
        
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

