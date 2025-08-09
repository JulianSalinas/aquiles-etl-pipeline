# aquiles-etl-pipeline
Optimized ETL pipeline with Azure Functions for processing CSV files and invoice images.

## Key Optimizations (2024)

### 🚀 Performance Improvements
- **Eliminated ProductsStep1 Database I/O**: Removed intermediate database table writes/reads
- **In-Memory Processing**: CSV → Transform → Direct to Staging tables
- **Pandas Bulk Operations**: Replaced row-by-row inserts with DataFrame.to_sql()
- **Database Awakening**: Automatic retry logic for serverless databases

### 📊 Architecture Overview

#### Optimized Flow (Current)
```
CSV/Image → Transform → In-Memory DataFrame → Staging Tables → Fact Tables
```

#### Legacy Flow (Deprecated)  
```
CSV/Image → Transform → ProductsStep1 Table → Read → Staging Tables → Fact Tables
```

### 🔄 Data Convergence
Both CSV files and Invoice images converge to the same data structure before processing:
- **CSV Files**: Direct column mapping (`Producto` → `Description`, `Porcentaje de IVA` → `PercentageIVA`)
- **Invoice Images**: Extract → Same CSV structure → Same transformation pipeline

## Architecture

```
├── function_app.py          # Azure Function triggers (optimized)
├── core/                    # Core business logic
│   ├── __init__.py         # Core package initialization
│   ├── data_processor.py   # Data transformation logic (enhanced)
│   ├── database.py         # SQL Server operations (with awakening)
│   ├── storage.py          # Azure Storage operations
│   └── etl_orchestrator.py # ETL pipeline orchestration (optimized)
├── models/                 # Database models with proper relationships
└── tests/                  # Comprehensive test suite
```

## Key Functions

### Optimized ETL Functions
- `process_csv_from_stream()` - Direct CSV processing (no ProductsStep1)
- `process_invoice_image_direct()` - Direct invoice processing 
- `normalize_to_staging_tables_from_dataframe()` - Pandas bulk operations
- `ensure_connection_established()` - Database awakening with retry logic

### Legacy Functions (Maintained for Compatibility)
- `process_from_products_step1()` - Processes existing ProductsStep1 data
- `normalize_to_staging_tables()` - Legacy normalization (uses ProductsStep1 table)