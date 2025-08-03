# aquiles-etl-pipeline
Handles of ETL pipelines with Azure Functions

## Architecture

```
├── function_app.py          # Azure Function triggers only
├── core/                    # Core business logic
│   ├── __init__.py         # Core package initialization
│   ├── data_processor.py   # Data transformation logic
│   ├── database.py         # SQL Server operations
│   ├── storage.py          # Azure Storage operations
│   └── etl_orchestrator.py # ETL pipeline orchestration
├── common/                  # Shared utilities
│   ├── sql_utils.py        # SQL connection utilities
│   └── transforms.py       # Data transformation functions
└── tests/                   # Test files
```

## Core Modules

### 1. `core/data_processor.py`
**Purpose**: Data transformation and validation
**Key Functions**:
- `apply_transformations(df)` - Apply all data transformations
- `validate_dataframe(df)` - Validate DataFrame before processing
- `get_processing_summary(df)` - Generate processing statistics

### 2. `core/database.py`
**Purpose**: SQL Server database operations
**Key Functions**:
- `get_sql_connection()` - Get database connection
- `write_to_sql_database(df, table_name)` - Write data to SQL
- `test_database_connection()` - Test connection health
- `get_database_info()` - Get database metadata

### 3. `core/storage.py`
**Purpose**: Azure Storage blob operations
**Key Functions**:
- `get_blob_service_client()` - Get blob storage client
- `read_blob_content(container, blob)` - Read blob data
- `get_blob_properties(container, blob)` - Get blob metadata
- `list_blobs_in_container(container)` - List container blobs

### 4. `core/etl_orchestrator.py`
**Purpose**: Coordinate the entire ETL pipeline
**Key Functions**:
- `process_csv_from_blob(container, blob, table)` - Full ETL from blob
- `process_csv_from_stream(stream, blob_name, table)` - ETL from stream
- `get_pipeline_health()` - Pipeline health status

## Azure Function Triggers

### 1. Blob Trigger: `provider24_elt_blob_trigger`
- **Path**: `products-dev` container
- **Purpose**: Automatically process CSV files when uploaded
- **Uses**: `process_csv_from_stream()`

### 2. HTTP Trigger: `provider24_http_trigger`
- **Route**: `/process-csv` (POST)
- **Purpose**: Manual CSV processing from specific blob
- **Body**: `{"container": "name", "blob": "file.csv", "table_name": "optional"}`
- **Uses**: `process_csv_from_blob()`

### 3. Health Check: `health_check`
- **Route**: `/health` (GET)
- **Purpose**: Monitor pipeline health status
- **Uses**: `get_pipeline_health()`

## Benefits of New Architecture

### 1. **Separation of Concerns**
- Triggers handle only Azure Function-specific logic
- Core modules handle business logic
- Clear boundaries between components

### 2. **Testability**
- Core logic can be unit tested independently
- Mock triggers for integration testing
- Easier to test individual components

### 3. **Maintainability**
- Changes to business logic don't affect triggers
- Easier to understand and modify code
- Better code organization

### 4. **Reusability**
- Core modules can be used in other contexts
- Business logic is framework-agnostic
- Easier to migrate or extend

### 5. **Monitoring**
- Health check endpoint for monitoring
- Detailed logging and error handling
- Processing statistics and summaries

## Usage Examples

### Process CSV via HTTP
```bash
curl -X POST https://your-function-app.azurewebsites.net/api/process-csv \
  -H "Content-Type: application/json" \
  -d '{"container": "products-dev", "blob": "sample.csv"}'
```

### Check Pipeline Health
```bash
curl https://your-function-app.azurewebsites.net/api/health
```

### Blob Upload (automatic)
Simply upload a CSV file to the `products-dev` container, and the blob trigger will automatically process it.

## Environment Variables

The pipeline uses the following environment variables:
- `SQL_SERVER` - Azure SQL Server name (default: provider24-dev.database.windows.net)
- `SQL_DATABASE` - Database name (default: provider24)
- `STORAGE_ACCOUNT_NAME` - Storage account name (default: provider24)
- `provider24_STORAGE` - Storage connection string (fallback auth)

## Future Enhancements

1. **Error Handling**: Dead letter queue for failed processing
2. **Monitoring**: Application Insights integration
3. **Scaling**: Batch processing for large files
4. **Security**: Enhanced authentication and authorization
5. **Performance**: Parallel processing and optimization
