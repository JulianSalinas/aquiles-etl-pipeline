# ETL Pipeline Enhancement Documentation

## Overview
This document outlines the enhancements made to the ETL pipeline to meet the new requirements for ProcessFile tracking, staging tables, normalization, and Azure OpenAI integration.

## Key Features Implemented

### 1. ProcessFile Table Tracking
- **Status 2 (In Progress)**: Files are marked as "In Progress" when processing starts
- **Status 3 (Success)**: Files are marked as "Success" when processing completes
- **Duplicate Detection**: Files with Status 3 are skipped and logged
- **Error Handling**: Failed processes can be tracked with appropriate status updates

### 2. Staging Tables
Created three staging tables with batch GUID tracking:
- `Staging.Provider`: Normalized provider data
- `Staging.Product`: Normalized product data with RawDescription mapping to Description field
- `Staging.Provider_Product`: Relationship data between providers and products

### 3. Normalization Pipeline
- **Data Source**: Reads from ProductsStep1 table (temporal table for transformation testing)
- **Batch Processing**: Each processing run gets a unique GUID for tracking
- **Unit of Measure Handling**: Automatically creates new units of measure as needed

### 4. SQL Merge Statements
Implemented MERGE statements for:
- **Provider**: Insert new providers, skip existing ones
- **Product**: Insert new products, update existing ones with latest data
- **Provider_Product**: Insert new relationships, update existing ones

### 5. Azure OpenAI Integration
- **Environment Variables**: Uses AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_MODEL
- **Fallback Support**: Falls back to mock extraction if OpenAI is not available
- **JSON Parsing**: Extracts structured product data from invoice images

## New Functions Added

### Core ETL Functions
- `check_process_file_status()`: Check if file is already processed
- `insert_process_file_record()`: Track new files with Status 2
- `update_process_file_status()`: Update file status (e.g., to Status 3 on success)
- `create_staging_tables()`: Create staging schema and tables
- `normalize_to_staging_tables()`: Transform ProductsStep1 data to staging tables
- `merge_staging_to_fact_tables()`: Merge staging data to fact tables
- `extract_invoice_data_with_openai()`: Extract products from images using Azure OpenAI
- `process_from_products_step1()`: Process existing ProductsStep1 data directly

### Modified Functions
- `process_csv_from_stream()`: Enhanced with full pipeline integration
- `process_invoice_image()`: Updated to use Azure OpenAI instead of mock data

## Environment Variables Required

```bash
# Azure SQL Database
SQL_SERVER=your-sql-server.database.windows.net
SQL_DATABASE=your-database-name

# Azure Storage
STORAGE_ACCOUNT_NAME=your-storage-account
provider24_STORAGE=your-storage-connection-string

# Azure OpenAI (optional - will fallback to mock if not provided)
AZURE_OPENAI_ENDPOINT=https://your-openai.openai.azure.com
AZURE_OPENAI_KEY=your-api-key
AZURE_OPENAI_MODEL=gpt-4-vision-preview
```

## Usage Examples

### Process CSV from Blob Storage
```python
from core.etl_orchestrator import process_csv_from_blob

result = process_csv_from_blob(
    storage_account_name="mystorageaccount",
    container_name="products-dev", 
    blob_name="products.csv",
    server_name="myserver.database.windows.net",
    database_name="mydatabase",
    table_name="ProductsStep1"
)
```

### Process Existing ProductsStep1 Data
```python
from core.etl_orchestrator import process_from_products_step1

result = process_from_products_step1(
    server_name="myserver.database.windows.net",
    database_name="mydatabase"
)
```

### Process Invoice Image
```python
from core.etl_orchestrator import process_invoice_image

result = process_invoice_image(
    image_content=image_bytes,
    image_name="invoice_001.jpg",
    storage_account_name="mystorageaccount",
    output_container="products-dev"
)
```

## Testing
The implementation includes comprehensive tests:
- **Unit Tests**: Test individual functions in isolation
- **Integration Tests**: Test complete workflows end-to-end
- **Error Handling Tests**: Verify proper error handling and fallback behavior

Run tests with:
```bash
python -m pytest tests/test_etl_pipeline.py -v
python -m pytest tests/test_integration.py -v
```

## Data Flow
1. **File Processing**: CSV/Image files trigger processing
2. **Status Tracking**: ProcessFile table tracks processing status
3. **Transformation**: Data is transformed and written to ProductsStep1
4. **Normalization**: ProductsStep1 data is normalized to staging tables
5. **Merging**: Staging data is merged to fact tables using SQL MERGE statements
6. **Completion**: ProcessFile status updated to Success (3)

## Key Design Decisions
- **Minimal Changes**: Kept existing function signatures where possible
- **Backward Compatibility**: Existing functionality continues to work
- **Error Resilience**: Comprehensive error handling with fallback options
- **Batch Tracking**: GUID-based batch tracking for audit trails
- **Schema Evolution**: Staging tables created automatically if they don't exist