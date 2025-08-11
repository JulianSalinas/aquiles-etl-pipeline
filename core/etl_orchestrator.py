"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import base64
import io
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
from openai import AzureOpenAI
from azure.storage.blob import BlobServiceClient
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from core.entities import ProcessFile, ProviderSynonym, UnitOfMeasure

from .data_processor import (apply_transformations,
                             map_columns_to_apply_transformations)
from .database import create_azure_sql_engine, ensure_connection_established
from .storage import (get_blob_service_client, read_blob_content,
                      upload_blob_content)


@dataclass
class StagingSummary:
    providers: int
    products: int
    provider_products: int

@dataclass
class ProcessingResult:
    status: bool
    message: str

@dataclass
class InvoiceProcessingResult(ProcessingResult):
    products_extracted: int
    csv_filename: str | None
    output_container: str | None

def check_process_file_status(engine: Engine, container: str, filename: str) -> int:
    """Check if file already exists in ProcessFile table"""
    try:
        with Session(engine) as session:
            pf: ProcessFile | None = session \
                .query(ProcessFile) \
                .filter(ProcessFile.Container == container) \
                .filter(ProcessFile.FileName == filename) \
                .first()
            return 1 if pf is None else pf.StatusId
    except Exception as e:
        logging.error(f"Error checking ProcessFile status: {str(e)}")
        raise


def normalize_to_staging_tables_from_dataframe(engine: Engine, df: pd.DataFrame, batch_guid: str):
    """Normalize data from DataFrame into staging tables. """

    if df.empty:
        logging.info("No data provided to normalize")
        return
    
    try:

        normalize_df_step1: pd.DataFrame = df.copy()

        provider_synonyms_df: pd.DataFrame = get_provider_synonyms_df(engine)

        normalize_df_step1 = normalize_df_step1.join(other=provider_synonyms_df, on='ProviderName', how='left')

        units_of_measure_df: pd.DataFrame = get_units_of_measure_df(engine)

        normalize_df_step1 = normalize_df_step1.join(other=units_of_measure_df, on='UnitOfMeasure', how='left')

        # Prepare provider data
        unique_providers = df['CleanProviderName'].dropna().unique()

        providers_inserted: int = 0

        if len(unique_providers) > 0:
            providers_df = pd.DataFrame({'Name': unique_providers, 'BatchGuid': batch_guid})
            providers_df.to_sql('Provider', engine, schema='Staging', if_exists='append', index=False)
            providers_inserted = len(unique_providers)

        # Prepare product data
        product_df: pd.DataFrame = df[['CleanDescription', 'RawDescription', 'CleanPrice', 'Measure', 'UnitOfMeasure']].copy()
        product_df: pd.DataFrame = product_df.dropna(subset=['CleanDescription'])  # type: ignore

        units_of_measure_df: pd.DataFrame
        with Session(engine) as session:
            units_of_measure = session.query(UnitOfMeasure).all()
            data: list[dict[str, Any]] = [{ "Acronym": uom.Acronym,  "Id": uom.Id } for uom in units_of_measure]
            units_of_measure_df = pd.DataFrame(data)

        products_inserted: int = 0

        if not product_df.empty:

            product_df.join(units_of_measure_df.set_index('Acronym'), on='UnitOfMeasure', how='left')

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

    except Exception as e:
        logging.error(f"Error normalizing to staging tables: {str(e)}")
        raise


def get_provider_synonyms_df(engine: Engine) -> pd.DataFrame:
    with Session(engine) as session:
        provider_synonyms = session.query(ProviderSynonym).all()

        provider_synonyms_df = pd.DataFrame([{
                "Id": ps.Id,
                "ProviderId": ps.ProviderId,
                "Synonym": ps.Synonym
            } for ps in provider_synonyms])

        provider_synonyms_df = provider_synonyms_df.set_index('Synonym')

        return provider_synonyms_df


def get_units_of_measure_df(engine: Engine) -> pd.DataFrame:
    with Session(engine) as session:

        units_of_measure = session.query(UnitOfMeasure).all()

        units_of_measure_df = pd.DataFrame( [{ 
                "Id": uom.Id,
                "Acronym": uom.Acronym 
            } for uom in units_of_measure])

        units_of_measure_df = units_of_measure_df.set_index('Acronym')

        return units_of_measure_df


def merge_staging_to_fact_tables(engine: Engine, batch_guid: str):
    """Merge data from staging tables to fact tables using SQL MERGE statements."""
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


def extract_invoice_data_with_openai(image_content: bytes, image_name: str) -> pd.DataFrame:
    """Extract invoice data from image using Azure OpenAI."""
    # Get OpenAI configuration from environment variables
    openai_endpoint = os.environ.get('AZURE_OPENAI_ENDPOINT')
    openai_key = os.environ.get('AZURE_OPENAI_KEY')
    openai_model = os.environ.get('AZURE_OPENAI_MODEL', 'gpt-4-vision-preview')
    
    # Get configurable parameters from environment variables
    prompt = os.environ.get('OPENAI_PROMPT', """
        Extract product information from this invoice. Return CSV format with columns:
        Producto,Provedor,Precio,Porcentaje de IVA
        
        Include header row. Example:
        Producto,Provedor,Precio,Porcentaje de IVA
        Product Name,Provider Name,100.00,19
        
        Return only CSV data.
    """)
    max_tokens = int(os.environ.get('OPENAI_MAX_TOKENS', '800'))
    temperature = float(os.environ.get('OPENAI_TEMPERATURE', '0.1'))
    
    if not openai_endpoint or not openai_key:
        raise ValueError("Azure OpenAI configuration not found. Set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY environment variables.")
    
    encoded_image = base64.b64encode(image_content).decode('utf-8')
    
    # Initialize Azure OpenAI client
    client = AzureOpenAI(
        azure_endpoint=openai_endpoint,
        api_key=openai_key,
        api_version="2024-02-15-preview"
    )
    
    # Make API request using OpenAI library
    try:
        response = client.chat.completions.create(
            model=openai_model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
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
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        content = response.choices[0].message.content
        
        try:
            # Clean the response to remove any markdown formatting
            csv_content = content.strip()

            if '```' in csv_content:
                # Remove markdown code blocks if present
                csv_content = csv_content.split('```')[1].strip()
                if csv_content.startswith('csv'):
                    csv_content = csv_content[3:].strip()
            
            # Parse CSV into DataFrame
            df: pd.DataFrame = pd.read_csv(io.StringIO(csv_content)) # type: ignore

            logging.info(f"Extracted {len(df)} products from invoice {image_name} using Azure OpenAI")

            return df
        
        except Exception as parse_error:
            raise ValueError(f"Failed to parse OpenAI response as CSV: {str(parse_error)}. Response: {content[:200]}...")
            
    except Exception as api_error:
        raise ValueError(f"OpenAI API request failed: {str(api_error)}")


def process_csv_from_stream(csv_data: bytes, blob_name: str, server_name: str, database_name: str) -> ProcessingResult:
    """Internal function to process CSV data (common logic for both blob and stream processing)"""
    try:
        engine: Engine = create_azure_sql_engine(server_name, database_name)

        ensure_connection_established(engine)
        
        container = "products-dev"

        file_status: int = check_process_file_status(engine, container, blob_name)
        
        if file_status == 3:
            message: str = f"File {blob_name} already processed successfully (Status 3), skipped"
            logging.info(message)
            return ProcessingResult(status=True, message=message)

        pf = ProcessFile(
            Container=container,
            FileName=blob_name,
            StatusId=2,  # In Progress
            ProcessDt=datetime.now(),
            BlobSize=len(csv_data),
            ContentType="text/csv",
            CreatedDt=datetime.now(),
            LastModifiedDt=datetime.now(),
            ETag=None,
            Metadata="{}"
        )

        with Session(engine) as session:
            session.add(pf)
            session.commit()
        
        df: pd.DataFrame = pd.read_csv(io.BytesIO(csv_data))  # type: ignore

        if df.empty:
            raise ValueError("CSV file is empty")
        
        df = map_columns_to_apply_transformations(df)

        transformed_df: pd.DataFrame = apply_transformations(df)
        
        batch_guid = str(uuid.uuid4())
        
        normalize_to_staging_tables_from_dataframe(engine, transformed_df, batch_guid)
        
        merge_staging_to_fact_tables(engine, batch_guid)
        
        with Session(engine) as session:
            pf.StatusId = 3
            session.commit()

        success_message = f"ETL process completed successfully for blob: {blob_name}"
        logging.info(success_message)

        return ProcessingResult(status=True, message=success_message)

    except Exception as e:
        ## TODO: Update ProcessFile status to 4 = Failed
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)
        return ProcessingResult(status=False, message=error_message)

def process_csv_from_blob(storage_account_name: str, container_name: str, blob_name: str, server_name: str, database_name: str) -> ProcessingResult:
    """Complete ETL process: read CSV from blob, transform, and write to database."""
    try:
        logging.info(f"Starting ETL process for blob: {container_name}/{blob_name}")

        blob_service_client: BlobServiceClient = get_blob_service_client(storage_account_name)

        csv_content: bytes = read_blob_content(blob_service_client, container_name, blob_name)

        return process_csv_from_stream(csv_content, blob_name, server_name, database_name)

    except Exception as e:
        error_message = f"ETL process failed for blob {blob_name}: {str(e)}"
        logging.error(error_message)  
        return ProcessingResult(status=False, message=error_message)


def process_invoice_image(image_content: bytes, image_name: str, server_name: str, database_name: str) -> ProcessingResult:
    """Complete invoice processing pipeline: extract data from image and write directly to database."""
    try:
        logging.info(f"Starting invoice processing for image: {image_name}")
        
        products_data: pd.DataFrame = extract_invoice_data_with_openai(image_content, image_name)
        
        # Apply transformations to prepare data for database
        df = map_columns_to_apply_transformations(products_data)
        transformed_df: pd.DataFrame = apply_transformations(df)
        
        # Create database engine and process data
        engine: Engine = create_azure_sql_engine(server_name, database_name)
        ensure_connection_established(engine)
        
        batch_guid = str(uuid.uuid4())
        
        normalize_to_staging_tables_from_dataframe(engine, transformed_df, batch_guid)
        merge_staging_to_fact_tables(engine, batch_guid)
        
        logging.info(f"Invoice processing completed successfully for: {image_name}")

        return InvoiceProcessingResult(
            status=True,
            message=f"Invoice processing completed successfully for: {image_name}",
            products_extracted=len(products_data),
            csv_filename=None,
            output_container=None
        )
        
    except Exception as e:
        error_message = f"Invoice processing failed for {image_name}: {str(e)}"
        logging.error(error_message)
        return ProcessingResult(status=False, message=error_message)