"""
ETL orchestrator module for coordinating the entire ETL pipeline.
"""
import base64
import io
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from io import StringIO

import pandas as pd
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI
from openai.types.beta.threads import ImageURLParam
from openai.types.chat import ChatCompletion, ChatCompletionContentPartImageParam, ChatCompletionContentPartParam, ChatCompletionContentPartTextParam, ChatCompletionUserMessageParam
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from core.entities import ProcessFile, ProviderSynonym, UnitOfMeasure

from .data_processor import apply_transformations, map_columns_to_apply_transformations
from .database import create_azure_sql_engine, ensure_connection_established
from .storage import get_blob_service_client, read_blob_content, upload_blob_content


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


def load_data_to_staging_tables(engine: Engine, df: pd.DataFrame, batch_guid: str):
    """Normalize data from DataFrame into staging tables. """

    if df.empty:
        logging.info("No data provided to normalize")
        return
    
    try:
        providers_inserted: int = insert_providers_to_staging(engine, df, batch_guid)

        products_inserted: int = insert_products_to_staging(engine, df, batch_guid)

        provider_products_inserted: int = insert_provider_products_to_staging(engine, df, batch_guid)

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

def insert_providers_to_staging(engine: Engine, products_df: pd.DataFrame, batch_guid: str) -> int:

    unique_providers = products_df['CleanProviderName'].dropna().unique()

    providers_inserted: int = 0

    if len(unique_providers) > 0:
        providers_df = pd.DataFrame({'Name': unique_providers, 'BatchGuid': batch_guid})
        providers_df.to_sql('Provider', engine, schema='Staging', if_exists='append', index=False)
        providers_inserted = len(unique_providers)

    return providers_inserted


def insert_products_to_staging(engine: Engine, products_df: pd.DataFrame, batch_guid: str) -> int:

    product_df: pd.DataFrame = products_df[['RawDescription', 'CleanPrice', 'Measure', 'UnitOfMeasure']].copy()

    if product_df.empty: return 0

    product_staging_df = pd.DataFrame({
        'Description': product_df['RawDescription'],
        'UnitPrice': 0,  # type: ignore
        'Measure': product_df['Measure'],
        'UnitOfMeasure': product_df['UnitOfMeasure'],
        'BatchGuid': batch_guid
    })
    
    product_staging_df.to_sql('Product', engine, schema='Staging', if_exists='append', index=False)

    return len(product_staging_df)


def insert_provider_products_to_staging(engine: Engine, products_df: pd.DataFrame, batch_guid: str) -> int:

    provider_product_df: pd.DataFrame = products_df[['CleanLastReviewDt', 'PackageUnits', 'PercentageIVA', 'RawDescription', 'CleanProviderName', 'CleanPrice']].copy()

    if provider_product_df.empty: return 0

    provider_product_staging_df = pd.DataFrame({
        'ProductId': 0,  # Will be updated in merge process
        'ProviderId': 0,  # Will be updated in merge process  
        'LastReviewDt': provider_product_df['CleanLastReviewDt'],
        'PackageUnits': provider_product_df['PackageUnits'],
        'IVA': provider_product_df['PercentageIVA'],
        'ProductDescription': provider_product_df['RawDescription'],
        'ProviderName': provider_product_df['CleanProviderName'],
        'Price': provider_product_df['CleanPrice'],
        'IsValidated': 0,
        'BatchGuid': batch_guid
    })
        
    provider_product_staging_df.to_sql('Provider_Product', engine, schema='Staging', if_exists='append', index=False)
    
    return len(provider_product_staging_df)
            

def merge_staging_to_fact_tables(engine: Engine, batch_guid: str):
    """Merge data from staging tables to fact tables using stored procedures."""
    try:
        with engine.begin() as conn:
            
            merge_providers_sp = text("EXEC usp_MergeProvidersFromStaging @BatchGuid = :batch_guid")
            conn.execute(merge_providers_sp, {"batch_guid": batch_guid})
            
            merge_products_sp = text("EXEC usp_MergeProductsFromStaging @BatchGuid = :batch_guid")
            conn.execute(merge_products_sp, {"batch_guid": batch_guid})
            
            merge_provider_products_sp = text("EXEC usp_MergeProviderProductsFromStaging @BatchGuid = :batch_guid")
            conn.execute(merge_provider_products_sp, {"batch_guid": batch_guid})
            
            conn.execute(text("DELETE FROM Staging.Product WHERE BatchGuid = :batch_guid"), {"batch_guid": batch_guid})
            conn.execute(text("DELETE FROM Staging.Provider WHERE BatchGuid = :batch_guid"), {"batch_guid": batch_guid})
            conn.execute(text("DELETE FROM Staging.Provider_Product WHERE BatchGuid = :batch_guid"), {"batch_guid": batch_guid})

            logging.info(f"Successfully merged staging data to fact tables for batch {batch_guid}")
    except Exception as e:
        logging.error(f"Error merging staging to fact tables: {str(e)}")
        raise


def extract_invoice_data_with_openai(image_content: bytes, image_name: str) -> pd.DataFrame:
    """Extract invoice data from image using Azure OpenAI."""
    
    openai_endpoint: str | None = os.environ.get('AZURE_OPENAI_ENDPOINT')

    if not openai_endpoint:
        raise ValueError("Azure OpenAI endpoint not found. Set AZURE_OPENAI_ENDPOINT environment variable.")
    
    openai_key: str | None = os.environ.get('AZURE_OPENAI_KEY')

    if not openai_key:
        raise ValueError("Azure OpenAI key not found. Set AZURE_OPENAI_KEY environment variable.")

    api_version: str = os.environ.get('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')

    openai_model: str | None = os.environ.get('AZURE_OPENAI_MODEL', 'gpt-4-vision-preview')

    prompt: str | None = os.environ.get('OPENAI_PROMPT')
    
    if not prompt:
        raise ValueError("OpenAI prompt not found. Set OPENAI_PROMPT environment variable.")

    max_tokens: int = int(os.environ.get('OPENAI_MAX_TOKENS', '800'))
    temperature: float = float(os.environ.get('OPENAI_TEMPERATURE', '0.1'))

    encoded_image: str = base64.b64encode(image_content).decode('utf-8')
    
    client = AzureOpenAI(
        azure_endpoint=openai_endpoint,
        api_key=openai_key,
        api_version=api_version
    )

    image_url: ImageURLParam = ImageURLParam(url=f"data:image/jpeg;base64,{encoded_image}")

    contentPartText: ChatCompletionContentPartTextParam = ChatCompletionContentPartTextParam(
        type="text",
        text=prompt
    )

    contentPartImage: ChatCompletionContentPartImageParam = ChatCompletionContentPartImageParam(
        type="image_url",
        image_url=image_url
    )

    userMessageContent: list[ChatCompletionContentPartParam] = [
        contentPartText,
        contentPartImage
    ]

    userMessage: ChatCompletionUserMessageParam = ChatCompletionUserMessageParam(
        role="user",
        content=userMessageContent
    )
    
    # Make API request using OpenAI library
    try:
        response: ChatCompletion = client.chat.completions.create(
            model=openai_model,
            messages=[userMessage],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        content: str | None = response.choices[0].message.content

        if not content:
            raise ValueError("No content returned from OpenAI API")

        csv_content: str = content.strip()

        # Remove markdown code block (```csv ... ```) using regex
        match = re.search(r"```(?:csv)?\s*(.*?)```", csv_content, re.DOTALL | re.IGNORECASE)

        if match:
            csv_content = match.group(1).strip()

        # Parse CSV into DataFrame
        df: pd.DataFrame = pd.read_csv(io.StringIO(csv_content)) # type: ignore

        logging.info(f"Extracted {len(df)} products from invoice {image_name} using Azure OpenAI")

        return df
            
    except Exception as api_error:
        raise ValueError(f"OpenAI API request failed: {str(api_error)}")


def process_csv_from_stream(csv_data: bytes, blob_name: str, server_name: str, database_name: str) -> ProcessingResult:
    """Internal function to process CSV data (common logic for both blob and stream processing)"""

    container = "products-dev"

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
    
    try:
        engine: Engine = create_azure_sql_engine(server_name, database_name)

        ensure_connection_established(engine)

        file_status: int = check_process_file_status(engine, container, blob_name)
        
        if file_status == 3:
            message: str = f"File {blob_name} already processed successfully (Status 3), skipped"
            logging.info(message)
            return ProcessingResult(status=True, message=message)

        with Session(engine) as session:
            session.add(pf)
            session.commit()
        
        df: pd.DataFrame = pd.read_csv(io.BytesIO(csv_data))  # type: ignore

        if df.empty:
            raise ValueError("CSV file is empty")
        
        df = map_columns_to_apply_transformations(df)

        transformed_df: pd.DataFrame = apply_transformations(df)
        
        batch_guid = str(uuid.uuid4())
        
        load_data_to_staging_tables(engine, transformed_df, batch_guid)
        
        merge_staging_to_fact_tables(engine, batch_guid)
        
        with Session(engine) as session:
            pf.StatusId = 3
            session.commit()

        success_message = f"ETL process completed successfully for blob: {blob_name}"
        logging.info(success_message)

        return ProcessingResult(status=True, message=success_message)

    except Exception as e:

        engine: Engine = create_azure_sql_engine(server_name, database_name)

        with Session(engine) as session:
            pf.StatusId = 4
            session.commit()
        
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


def process_invoice_image(storage_account_name: str, container_name: str, image_content: bytes, image_name: str, server_name: str, database_name: str) -> ProcessingResult:
    """Complete invoice processing pipeline: extract data from image and write directly to database."""
    try:
        logging.info(f"Starting invoice processing for image: {image_name}")
        
        products_data: pd.DataFrame = extract_invoice_data_with_openai(image_content, image_name)

        batch_guid = str(uuid.uuid4())

        blob_service_client: BlobServiceClient = get_blob_service_client(storage_account_name)
        
        csv_filename: str = f"{os.path.splitext(image_name)[0]}_{batch_guid[:8]}.csv"

        csv_buffer = StringIO()
        products_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        upload_blob_content(blob_service_client, container_name, csv_filename, csv_buffer, content_type="text/csv")

        logging.info(f"CSV saved to {container_name}/{csv_filename}")

        df = map_columns_to_apply_transformations(products_data)
        
        transformed_df: pd.DataFrame = apply_transformations(df)
        
        engine: Engine = create_azure_sql_engine(server_name, database_name)

        ensure_connection_established(engine)
        
        load_data_to_staging_tables(engine, transformed_df, batch_guid)

        merge_staging_to_fact_tables(engine, batch_guid)
        
        logging.info(f"Invoice processing completed successfully for: {image_name}")

        return InvoiceProcessingResult(
            status=True,
            message=f"Invoice processing completed successfully for: {image_name}",
            products_extracted=len(products_data),
            csv_filename=csv_filename,
            output_container=container_name
        )
        
    except Exception as e:
        error_message = f"Invoice processing failed for {image_name}: {str(e)}"
        logging.error(error_message)
        return ProcessingResult(status=False, message=error_message)


def process_csv_string(storage_account_name: str, container_name: str, csv_content: str, filename: str) -> ProcessingResult:
    """Process CSV string: validate format and upload to invoice-csv-dev container."""
    try:
        logging.info(f"Starting CSV string processing for file: {filename}")
        
        expected_headers: list[str] = ["Producto", "Fecha", "Provedor", "Precio", "IVA"]

        csv_buffer = StringIO()
        csv_buffer.write(csv_content)
        csv_buffer.seek(0)

        df: pd.DataFrame = pd.read_csv(csv_buffer) # type: ignore
        
        if df.empty:
            raise ValueError("CSV content is empty")
        
        actual_headers: list[str] = df.columns.str.strip().tolist()

        missing_headers: list[str] = [h for h in expected_headers if h not in actual_headers]
        
        if missing_headers:
            raise ValueError(f"Missing required headers: {missing_headers}. Expected headers: {expected_headers}")
        
        extra_headers: list[str] = [h for h in actual_headers if h not in expected_headers]

        if extra_headers:
            logging.warning(f"Extra headers found (will be ignored): {extra_headers}")

        if len(df) == 0:
            raise ValueError("CSV file contains headers but no data rows")
        
        logging.info(f"CSV validation successful. Found {len(df)} data rows with required headers: {expected_headers}")

        blob_service_client: BlobServiceClient = get_blob_service_client(storage_account_name)

        upload_blob_content(blob_service_client, container_name, filename, csv_content, content_type="text/csv")

        logging.info(f"CSV uploaded successfully to {container_name}/{filename}")

        message: str = f"CSV validation and upload completed successfully. File: {filename}, Rows: {len(df)}, Container: {container_name}"

        return ProcessingResult(status=True,  message=message)
    
    except Exception as e:
        error_message = f"CSV processing failed for {filename}: {str(e)}"
        logging.error(error_message)
        return ProcessingResult(status=False, message=error_message)