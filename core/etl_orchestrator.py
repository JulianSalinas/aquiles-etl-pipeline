"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import logging
import pandas as pd
import io
import csv
import uuid
import os
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from models.process_file import ProcessFile
from .data_processor import apply_transformations
from .database import create_azure_sql_engine, ensure_connection_established
from .storage import read_blob_content, get_blob_service_client, upload_blob_content


def check_process_file_status(engine, container, filename):
    """
    Check if file already exists in ProcessFile table and return its status.
    
    Args:
        engine: SQLAlchemy engine
        container (str): Container name
        filename (str): File name
    
    Returns:
        dict: {"exists": bool, "status_id": int or None, "id": int or None}
    """
    try:
        with Session(engine) as session:
            pf = session.query(ProcessFile).filter_by(Container=container, FileName=filename).first()
            if pf:
                return {"exists": True, "status_id": pf.StatusId, "id": pf.Id}
            else:
                return {"exists": False, "status_id": None, "id": None}
    except Exception as e:
        logging.error(f"Error checking ProcessFile status: {str(e)}")
        raise


def insert_process_file_record(engine, container, filename, blob_size, content_type, created_dt, last_modified_dt, etag, metadata):
    """
    Insert new record into ProcessFile table with Status 2 (In Progress).
    
    Args:
        engine: SQLAlchemy engine
        container (str): Container name
        filename (str): File name
        blob_size (int): Size of blob in bytes
        content_type (str): MIME type of the file
        created_dt (datetime): Creation datetime
        last_modified_dt (datetime): Last modified datetime
        etag (str): ETag of the blob
        metadata (str): Metadata as JSON string
    
    Returns:
        int: ID of the inserted record
    """
    try:
        with Session(engine) as session:
            pf = ProcessFile(
                Container=container,
                FileName=filename,
                StatusId=2,  # In Progress
                ProcessDt=datetime.now(),
                BlobSize=blob_size,
                ContentType=content_type,
                CreatedDt=created_dt,
                LastModifiedDt=last_modified_dt,
                ETag=etag,
                Metadata=metadata
            )
            session.add(pf)
            session.commit()
            logging.info(f"Inserted ProcessFile record with ID {pf.Id} for {container}/{filename}")
            return pf.Id
    except Exception as e:
        logging.error(f"Error inserting ProcessFile record: {str(e)}")
        raise


def update_process_file_status(engine, process_file_id, status_id):
    """
    Update ProcessFile record status.
    
    Args:
        engine: SQLAlchemy engine
        process_file_id (int): ProcessFile record ID
        status_id (int): New status ID (3 = Success)
    """
    try:
        with Session(engine) as session:
            pf = session.query(ProcessFile).filter_by(Id=process_file_id).first()
            if pf:
                pf.StatusId = status_id
                pf.ProcessDt = datetime.now()
                session.commit()
                logging.info(f"Updated ProcessFile ID {process_file_id} to status {status_id}")
            else:
                logging.warning(f"ProcessFile ID {process_file_id} not found for status update")
    except Exception as e:
        logging.error(f"Error updating ProcessFile status: {str(e)}")
        raise


def create_staging_tables(engine):
    """
    Create staging tables if they don't exist.
    """
    try:
        # DDL is not supported by ORM, so we keep raw SQL but use session.begin()
        staging_tables_sql = """
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
        BEGIN
            EXEC('CREATE SCHEMA Staging')
        END
        
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Staging].[Provider]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [Staging].[Provider] (
                [Id] int IDENTITY(1,1) PRIMARY KEY,
                [Name] nvarchar(255) NOT NULL,
                [BatchGuid] uniqueidentifier NOT NULL,
                [CreateDt] datetime2 DEFAULT GETDATE()
            )
        END
        
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Staging].[Product]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [Staging].[Product] (
                [Id] int IDENTITY(1,1) PRIMARY KEY,
                [ProductName] nvarchar(255) NOT NULL,
                [Description] nvarchar(max),
                [Price] decimal(18,2),
                [Quantity] int,
                [Measure] decimal(18,2),
                [UnitOfMeasureId] int,
                [BatchGuid] uniqueidentifier NOT NULL,
                [CreatedDt] datetime2 DEFAULT GETDATE(),
                [UpdatedDt] datetime2 DEFAULT GETDATE()
            )
        END
        
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Staging].[Provider_Product]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [Staging].[Provider_Product] (
                [Id] int IDENTITY(1,1) PRIMARY KEY,
                [ProductId] int NOT NULL,
                [ProviderId] int NOT NULL,
                [LastReviewDt] datetime2,
                [PackageUnits] int,
                [IVA] decimal(18,2),
                [IsValidated] bit DEFAULT 0,
                [BatchGuid] uniqueidentifier NOT NULL
            )
        END
        """
        with engine.begin() as conn:
            conn.execute(text(staging_tables_sql))
            logging.info("Staging tables created successfully")
    except Exception as e:
        logging.error(f"Error creating staging tables: {str(e)}")
        raise


def read_from_products_step1(engine):
    """
    Read data from ProductsStep1 table for normalization.
    
    Returns:
        pandas.DataFrame: Data from ProductsStep1 table
    """
    try:
        # If you have a model for ProductsStep1, use ORM; otherwise, keep as is but use session
        with Session(engine) as session:
            df = pd.read_sql("SELECT * FROM ProductsStep1", session.bind)
            logging.info(f"Read {len(df)} rows from ProductsStep1 table")
            return df
    except Exception as e:
        logging.error(f"Error reading from ProductsStep1: {str(e)}")
        raise


def get_or_create_unit_of_measure(engine, unit_acronym):
    """
    Get UnitOfMeasureId for given acronym, create if doesn't exist.
    
    Args:
        engine: SQLAlchemy engine
        unit_acronym (str): Unit acronym (e.g., 'kg', 'g', 'ml')
    
    Returns:
        int: UnitOfMeasureId
    """
    from models.unit_of_measure import UnitOfMeasure
    try:
        if not unit_acronym:
            return None
        unit_acronym = unit_acronym.lower().strip()
        with Session(engine) as session:
            unit = session.query(UnitOfMeasure).filter(UnitOfMeasure.Acronym.ilike(unit_acronym)).first()
            if unit:
                return unit.Id
            # Create new unit of measure
            new_unit = UnitOfMeasure(Acronym=unit_acronym, Name=unit_acronym.upper())
            session.add(new_unit)
            session.commit()
            logging.info(f"Created new UnitOfMeasure: {unit_acronym} with ID {new_unit.Id}")
            return new_unit.Id
    except Exception as e:
        logging.error(f"Error getting/creating unit of measure: {str(e)}")
        raise


def normalize_to_staging_tables_from_dataframe(engine, df, batch_guid):
    """
    Normalize data from DataFrame (ProductsStep1 equivalent) into staging tables.
    Uses pandas bulk operations for better performance.
    
    Args:
        engine: SQLAlchemy engine
        df (pandas.DataFrame): Transformed data DataFrame
        batch_guid (str): Batch GUID for tracking this batch
    
    Returns:
        dict: Summary of records inserted into each staging table
    """
    try:
        if df.empty:
            logging.info("No data provided to normalize")
            return {"providers": 0, "products": 0, "provider_products": 0}
        
        # Prepare provider data
        unique_providers = df['CleanProviderName'].dropna().unique()
        if len(unique_providers) > 0:
            provider_df = pd.DataFrame({
                'Name': unique_providers,
                'BatchGuid': batch_guid
            })
            provider_df.to_sql('Provider', engine, schema='Staging', if_exists='append', index=False)
            providers_inserted = len(unique_providers)
        else:
            providers_inserted = 0
            
        # Prepare product data
        product_df = df[['CleanDescription', 'RawDescription', 'CleanPrice', 'Measure', 'UnitOfMeasure']].copy()
        product_df = product_df.dropna(subset=['CleanDescription'])
        
        if not product_df.empty:
            # Get or create unit of measure IDs
            product_df['UnitOfMeasureId'] = product_df['UnitOfMeasure'].apply(
                lambda x: get_or_create_unit_of_measure(engine, x) if pd.notna(x) else None
            )
            
            # Prepare for staging
            product_staging_df = pd.DataFrame({
                'ProductName': product_df['CleanDescription'],
                'Description': product_df['RawDescription'],
                'Price': product_df['CleanPrice'],
                'Measure': product_df['Measure'],
                'UnitOfMeasureId': product_df['UnitOfMeasureId'],
                'BatchGuid': batch_guid
            })
            
            product_staging_df.to_sql('Product', engine, schema='Staging', if_exists='append', index=False)
            products_inserted = len(product_staging_df)
        else:
            products_inserted = 0
            
        # Prepare provider_product data  
        provider_product_df = df[['CleanLastReviewDt', 'PackageUnits', 'PercentageIVA']].copy()
        
        if not provider_product_df.empty:
            provider_product_staging_df = pd.DataFrame({
                'ProductId': 0,  # Will be updated in merge process
                'ProviderId': 0,  # Will be updated in merge process  
                'LastReviewDt': provider_product_df['CleanLastReviewDt'],
                'PackageUnits': provider_product_df['PackageUnits'],
                'IVA': provider_product_df['PercentageIVA'],
                'IsValidated': 0,
                'BatchGuid': batch_guid
            })
            
            provider_product_staging_df.to_sql('Provider_Product', engine, schema='Staging', if_exists='append', index=False)
            provider_products_inserted = len(provider_product_staging_df)
        else:
            provider_products_inserted = 0
            
        logging.info(f"Normalized data to staging tables - Providers: {providers_inserted}, Products: {products_inserted}, Provider_Products: {provider_products_inserted}")
        return {
            "providers": providers_inserted,
            "products": products_inserted,
            "provider_products": provider_products_inserted
        }
    except Exception as e:
        logging.error(f"Error normalizing to staging tables: {str(e)}")
        raise


def normalize_to_staging_tables(engine, batch_guid):
    """
    Read data from ProductsStep1 and normalize into staging tables.
    
    DEPRECATED: This function reads from ProductsStep1 table. 
    Use normalize_to_staging_tables_from_dataframe() instead for better performance.
    
    Args:
        engine: SQLAlchemy engine
        batch_guid (str): Batch GUID for tracking this batch
    
    Returns:
        dict: Summary of records inserted into each staging table
    """
    try:
        # Read data from ProductsStep1
        df = read_from_products_step1(engine)
        return normalize_to_staging_tables_from_dataframe(engine, df, batch_guid)
    except Exception as e:
        logging.error(f"Error normalizing to staging tables: {str(e)}")
        raise


def merge_staging_to_fact_tables(engine, batch_guid):
    """
    Merge data from staging tables to fact tables using SQL MERGE statements.
    
    Args:
        engine: SQLAlchemy engine
        batch_guid (str): Batch GUID to identify records to merge
    """
    try:
        # DML/merge is not supported by ORM, so we keep raw SQL but use engine.begin()
        with engine.begin() as conn:
            merge_providers_sql = text("""
                MERGE Provider AS target
                USING (SELECT DISTINCT Name FROM Staging.Provider WHERE BatchGuid = :batch_guid) AS source
                ON target.Name = source.Name
                WHEN NOT MATCHED THEN
                    INSERT (Name, CreateDt)
                    VALUES (source.Name, GETDATE());
            """)
            conn.execute(merge_providers_sql, {"batch_guid": batch_guid})
            merge_products_sql = text("""
                MERGE Product AS target
                USING (
                    SELECT sp.ProductName, sp.Description, sp.Price, sp.Measure, sp.UnitOfMeasureId
                    FROM Staging.Product sp
                    WHERE sp.BatchGuid = :batch_guid
                ) AS source
                ON target.ProductName = source.ProductName
                WHEN MATCHED THEN
                    UPDATE SET 
                        Description = source.Description,
                        Price = source.Price,
                        Measure = source.Measure,
                        UnitOfMeasureId = source.UnitOfMeasureId,
                        UpdatedDt = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (ProductName, Description, Price, Measure, UnitOfMeasureId, CreatedDt, UpdatedDt)
                    VALUES (source.ProductName, source.Description, source.Price, source.Measure, source.UnitOfMeasureId, GETDATE(), GETDATE());
            """)
            conn.execute(merge_products_sql, {"batch_guid": batch_guid})
            merge_provider_product_sql = text("""
                MERGE Provider_Product AS target
                USING (
                    SELECT 
                        p.Id as ProductId,
                        pr.Id as ProviderId,
                        spp.LastReviewDt,
                        spp.PackageUnits,
                        spp.IVA,
                        spp.IsValidated
                    FROM Staging.Provider_Product spp
                    INNER JOIN Staging.Product sp ON spp.BatchGuid = sp.BatchGuid
                    INNER JOIN Product p ON p.ProductName = sp.ProductName
                    INNER JOIN Staging.Provider spr ON spp.BatchGuid = spr.BatchGuid  
                    INNER JOIN Provider pr ON pr.Name = spr.Name
                    WHERE spp.BatchGuid = :batch_guid
                ) AS source
                ON target.ProductId = source.ProductId AND target.ProviderId = source.ProviderId
                WHEN MATCHED THEN
                    UPDATE SET 
                        LastReviewDt = source.LastReviewDt,
                        PackageUnits = source.PackageUnits,
                        IVA = source.IVA,
                        IsValidated = source.IsValidated
                WHEN NOT MATCHED THEN
                    INSERT (ProductId, ProviderId, LastReviewDt, PackageUnits, IVA, IsValidated)
                    VALUES (source.ProductId, source.ProviderId, source.LastReviewDt, source.PackageUnits, source.IVA, source.IsValidated);
            """)
            conn.execute(merge_provider_product_sql, {"batch_guid": batch_guid})
            logging.info(f"Successfully merged staging data to fact tables for batch {batch_guid}")
    except Exception as e:
        logging.error(f"Error merging staging to fact tables: {str(e)}")
        raise


def extract_invoice_data_with_openai(image_content, image_name):
    """
    Extract invoice data from image using Azure OpenAI.
    
    Args:
        image_content: Binary content of the image
        image_name: Name of the image file
    
    Returns:
        list: List of product dictionaries extracted from the invoice
    """
    try:
        import base64
        import requests
        
        # Get OpenAI configuration from environment variables
        openai_endpoint = os.environ.get('AZURE_OPENAI_ENDPOINT')
        openai_key = os.environ.get('AZURE_OPENAI_KEY')
        openai_model = os.environ.get('AZURE_OPENAI_MODEL', 'gpt-4-vision-preview')
        
        if not openai_endpoint or not openai_key:
            logging.warning("Azure OpenAI configuration not found, falling back to mock extraction")
            return extract_invoice_data_from_image(image_content, image_name)
        
        logging.info(f"Processing invoice image with Azure OpenAI: {image_name}")
        
        # Encode image to base64
        encoded_image = base64.b64encode(image_content).decode('utf-8')
        
        # Prepare the API request
        headers = {
            "Content-Type": "application/json",
            "api-key": openai_key
        }
        
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """Analyze this invoice image and extract product information. 
                            Return a JSON array of products with the following structure:
                            [{"Producto": "product name", "Provedor": "provider name", "Precio": "price", "Porcentaje de IVA": "iva percentage"}]
                            Only return the JSON array, no additional text."""
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{encoded_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1000,
            "temperature": 0.1
        }
        
        # Make API request
        response = requests.post(
            f"{openai_endpoint}/openai/deployments/{openai_model}/chat/completions?api-version=2024-02-15-preview",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # Parse JSON response
            import json
            try:
                products_data = json.loads(content)
                logging.info(f"Extracted {len(products_data)} products from invoice {image_name} using Azure OpenAI")
                return products_data
            except json.JSONDecodeError:
                logging.warning(f"Failed to parse OpenAI response as JSON, falling back to mock extraction")
                return extract_invoice_data_from_image(image_content, image_name)
        else:
            logging.warning(f"OpenAI API request failed with status {response.status_code}, falling back to mock extraction")
            return extract_invoice_data_from_image(image_content, image_name)
            
    except Exception as e:
        logging.warning(f"Error with OpenAI extraction: {str(e)}, falling back to mock extraction")
        return extract_invoice_data_from_image(image_content, image_name)


def process_from_products_step1(server_name, database_name):
    """
    Process data directly from ProductsStep1 table through the normalization pipeline.
    This allows processing existing data in ProductsStep1 without needing new CSV files.
    
    Args:
        server_name (str): SQL Server name
        database_name (str): SQL Database name
    
    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info("Starting processing from ProductsStep1 table")
        
        # Create database engine
        engine = create_azure_sql_engine(server_name, database_name)
        result = ensure_connection_established(engine)
        if result is None:
            raise ValueError("Failed to establish connection to SQL Database")
        
        # Create staging tables if they don't exist
        create_staging_tables(engine)
        
        # Generate batch GUID for this processing batch
        batch_guid = str(uuid.uuid4())
        logging.info(f"Generated batch GUID: {batch_guid}")
        
        # Normalize data from ProductsStep1 to staging tables
        logging.info("Normalizing data to staging tables...")
        staging_summary = normalize_to_staging_tables(engine, batch_guid)
        
        if staging_summary["providers"] == 0 and staging_summary["products"] == 0:
            return {
                "status": True,
                "message": "No data found in ProductsStep1 table to process",
                "batch_guid": batch_guid,
                "staging_summary": staging_summary
            }
        
        # Merge staging data to fact tables
        logging.info("Merging staging data to fact tables...")
        merge_staging_to_fact_tables(engine, batch_guid)
        
        logging.info("Processing from ProductsStep1 completed successfully")
        
        return {
            "status": True,
            "message": "Processing from ProductsStep1 completed successfully",
            "batch_guid": batch_guid,
            "staging_summary": staging_summary
        }
        
    except Exception as e:
        error_message = f"Processing from ProductsStep1 failed: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }



def process_csv_from_stream(csv_data, blob_name, server_name, database_name, table_name):
    """
    Internal function to process CSV data (common logic for both blob and stream processing).
    This version eliminates writing to ProductsStep1 table and processes data in-memory.
    
    Args:
        csv_data: CSV data as bytes or stream
        blob_name (str): Name of the source blob (for logging)
        server_name (str): SQL Server name
        database_name (str): SQL Database name
        table_name (str): Target database table name (DEPRECATED - ProductsStep1 no longer used)

    Returns:
        dict: Processing results and summary
    """
    try:
        # Create database engine first
        engine = create_azure_sql_engine(server_name, database_name)
        result = ensure_connection_established(engine)
        if result is None:
            raise ValueError("Failed to establish connection to SQL Database")
        
        # Check ProcessFile status
        container = "products-dev"  # Default container for CSV processing
        file_status = check_process_file_status(engine, container, blob_name)
        
        if file_status["exists"] and file_status["status_id"] == 3:
            logging.info(f"File {blob_name} already processed successfully (Status 3), skipping")
            return {
                "status": True,
                "message": f"File {blob_name} already processed successfully, skipped",
                "rows_processed": 0,
                "rows_written": 0
            }
        
        # Insert or update ProcessFile record with Status 2 (In Progress)
        process_file_id = None
        if not file_status["exists"]:
            process_file_id = insert_process_file_record(
                engine, container, blob_name, 
                len(csv_data), "text/csv", datetime.now(), datetime.now(), "", "{}"
            )
        else:
            process_file_id = file_status["id"]
            update_process_file_status(engine, process_file_id, 2)  # Set to In Progress
        
        logging.info("Parsing CSV data...")
        df = pd.read_csv(io.BytesIO(csv_data))
        
        if df.empty:
            raise ValueError("CSV file is empty")
        
        logging.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Original columns: {list(df.columns)}")
        
        logging.info("Applying data transformations...")
        transformed_df = apply_transformations(df)
        
        # OPTIMIZATION: Skip writing to ProductsStep1 table, process directly in-memory
        logging.info("Processing data directly to staging tables (skipping ProductsStep1)...")
        
        # Create staging tables if they don't exist
        create_staging_tables(engine)
        
        # Generate batch GUID for this processing batch
        batch_guid = str(uuid.uuid4())
        logging.info(f"Generated batch GUID: {batch_guid}")
        
        # Normalize data directly from DataFrame to staging tables
        logging.info("Normalizing data to staging tables...")
        staging_summary = normalize_to_staging_tables_from_dataframe(engine, transformed_df, batch_guid)
        
        # Merge staging data to fact tables
        logging.info("Merging staging data to fact tables...")
        merge_staging_to_fact_tables(engine, batch_guid)
        
        # Update ProcessFile status to Success (3)
        update_process_file_status(engine, process_file_id, 3)
        
        logging.info(f"ETL process completed successfully for blob: {blob_name}")

        return {
            "status": True,
            "message": f"ETL process completed successfully for blob: {blob_name}",
            "rows_processed": len(transformed_df),
            "rows_written": staging_summary["providers"] + staging_summary["products"] + staging_summary["provider_products"],
            "batch_guid": batch_guid,
            "staging_summary": staging_summary
        }
        
    except Exception as e:
        # Update ProcessFile status to Failed if we have a process_file_id
        if 'process_file_id' in locals() and process_file_id:
            try:
                engine = create_azure_sql_engine(server_name, database_name)
                update_process_file_status(engine, process_file_id, 1)  # Assuming 1 = Failed
            except:
                pass  # Don't fail the main error handling
                
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


def process_invoice_image_direct(image_content, image_name, server_name, database_name):
    """
    Complete invoice processing pipeline that processes data directly without intermediate CSV storage.
    Ensures data convergence by using the same transformation pipeline as CSV processing.
    
    Args:
        image_content: Binary content of the invoice image
        image_name: Name of the image file
        server_name: SQL Server name
        database_name: SQL Database name
    
    Returns:
        dict: Processing results and summary
    """
    try:
        logging.info(f"Starting direct invoice processing for image: {image_name}")
        
        # Extract data from invoice image using OpenAI
        logging.info("Extracting data from invoice image...")
        products_data = extract_invoice_data_with_openai(image_content, image_name)
        
        # Generate timestamp for the processing
        trigger_date = datetime.now().strftime("%Y-%m-%d")
        
        # Convert invoice data to DataFrame (same structure as CSV)
        logging.info("Converting invoice data to DataFrame...")
        invoice_df = pd.DataFrame([
            {
                "Producto": product.get("Producto", ""),
                "Fecha 1": trigger_date,
                "Provedor": product.get("Provedor", ""), 
                "Precio": product.get("Precio", ""),
                "Porcentaje de IVA": product.get("Porcentaje de IVA", "")
            }
            for product in products_data
        ])
        
        if invoice_df.empty:
            raise ValueError("No products extracted from invoice")
        
        logging.info(f"Created DataFrame with {len(invoice_df)} rows and {len(invoice_df.columns)} columns")
        logging.info(f"Columns: {list(invoice_df.columns)}")
        
        # Apply same transformations as CSV processing (ensuring data convergence)
        logging.info("Applying data transformations...")
        transformed_df = apply_transformations(invoice_df)
        
        # Create database engine and ensure connection
        engine = create_azure_sql_engine(server_name, database_name)
        result = ensure_connection_established(engine)
        if result is None:
            raise ValueError("Failed to establish connection to SQL Database")
        
        # Create staging tables if they don't exist
        create_staging_tables(engine)
        
        # Generate batch GUID for this processing batch
        batch_guid = str(uuid.uuid4())
        logging.info(f"Generated batch GUID: {batch_guid}")
        
        # Normalize data directly from DataFrame to staging tables
        logging.info("Normalizing data to staging tables...")
        staging_summary = normalize_to_staging_tables_from_dataframe(engine, transformed_df, batch_guid)
        
        # Merge staging data to fact tables
        logging.info("Merging staging data to fact tables...")
        merge_staging_to_fact_tables(engine, batch_guid)
        
        logging.info(f"Direct invoice processing completed successfully for: {image_name}")

        return {
            "status": True,
            "message": f"Direct invoice processing completed successfully for: {image_name}",
            "products_extracted": len(products_data),
            "rows_processed": len(transformed_df),
            "batch_guid": batch_guid,
            "staging_summary": staging_summary
        }
        
    except Exception as e:
        error_message = f"Direct invoice processing failed for {image_name}: {str(e)}"
        logging.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }


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
        
        # Extract data from invoice image using OpenAI
        logging.info("Extracting data from invoice image...")
        products_data = extract_invoice_data_with_openai(image_content, image_name)
        
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