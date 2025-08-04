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
│   └── transforms.py       # Data transformation functions
└── tests/                   # Test files
```