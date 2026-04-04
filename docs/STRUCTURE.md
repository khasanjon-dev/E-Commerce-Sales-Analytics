# 📁 Project Structure

## Directory Layout

```
E-Commerce-Sales-Analytics/
├── Makefile                          # Build automation & pipeline execution
├── README.md                         # Project documentation
├── requirements.txt                  # Python dependencies
│
├── data/
│   ├── raw/                          # Raw, unprocessed data
│   │   ├── online_retail.parquet     # Original transactional data
│   │   └── validation_report.json    # Data quality validation results
│   │
│   ├── clean/                        # Cleaned & standardized data
│   │   └── online_retail_clean.parquet
│   │
│   ├── processed/                    # Feature-engineered data & reports
│   │   ├── online_retail_features.parquet
│   │   ├── report_revenue_trends.csv
│   │   ├── report_top_products.csv
│   │   ├── report_top_customers.csv
│   │   ├── report_geographic_analysis.csv
│   │   └── report_returns_analysis.csv
│   │
│   └── warehouse/                    # SQLite data warehouse
│       └── warehouse.db
│
├── docs/
│   ├── index.html                    # GitHub Pages dashboard
│   ├── style.css                     # Dashboard styling
│   ├── TARGET.md                     # Project requirements & specifications
│   ├── STRUCTURE.md                  # This file
│   └── assets/
│       ├── project-architecture.png
│       └── project-architecture-gemini.png
│
└── src/
    ├── __init__.py
    ├── config.py                     # Global configuration & logging setup
    ├── pipeline.py                   # Main ETL orchestration
    │
    ├── extract/
    │   ├── __init__.py
    │   ├── download_data.py          # Fetch data from Kaggle API
    │   └── validate_data.py          # Data quality checks & validation
    │
    ├── transform/
    │   ├── __init__.py
    │   ├── cleand_data.py            # Data cleaning (nulls, duplicates, types)
    │   └── feature_engineering.py    # Derived fields (total_price, dates, flags)
    │
    ├── load/
    │   ├── __init__.py
    │   ├── create_tables.py          # Star schema DDL (dimensions & facts)
    │   ├── load_dimensions.py        # Load customer, product, date, country dims
    │   └── load_facts.py             # Load transaction fact table
    │
    ├── analytics/
    │   ├── __init__.py
    │   ├── aggregations.py           # Business metrics aggregation queries
    │   └── reports.py                # Generate analytical reports (CSV)
    │
    └── utils/
        ├── __init__.py
        └── db.py                     # Database connection & transaction management
```

## Module Descriptions

### `extract/` - Data Ingestion
- **download_data.py**: Fetches raw online retail dataset from Kaggle
- **validate_data.py**: Validates data schema, checks for nulls, duplicates, data types

### `transform/` - Data Cleaning & Feature Engineering
- **cleand_data.py**: 
  - Removes null records
  - Handles negative quantities (returns)
  - Normalizes column names
  - Converts timestamps to proper format
  
- **feature_engineering.py**:
  - `total_price` = quantity × unit_price
  - Time features: year, month, day, day_of_week
  - `is_return` flag for negative quantities

### `load/` - Data Warehouse & Schema
- **create_tables.py**: Creates star schema tables with constraints
- **load_dimensions.py**: Loads dimension tables (customers, products, dates, countries)
- **load_facts.py**: Loads sales transaction fact table

### `analytics/` - Business Intelligence Layer
- **aggregations.py**: Revenue, product performance, customer lifetime value, geographic metrics
- **reports.py**: Generates 5 analytical reports as CSV files

### `utils/` - Utilities
- **db.py**: SQLite connection pooling, transaction handling, error logging

## Data Flow

```
Raw Data (CSV/Parquet)
    ↓
[extract] → validate_data.py
    ↓
[transform] → cleand_data.py → feature_engineering.py
    ↓
[load] → create_tables.py → load_dimensions.py → load_facts.py
    ↓
[analytics] → aggregations.py → reports.py
    ↓
Reports (CSV) + Warehouse (SQLite DB)
```

## Star Schema

### Dimension Tables
- **customers**: customer_id, customer_name
- **products**: product_id, product_name, unit_price
- **dates**: date_id, year, month, day, day_of_week
- **countries**: country_id, country_name

### Fact Table
- **sales**: transaction_id, customer_id, product_id, date_id, country_id, quantity, unit_price, total_price, is_return

## Output Files

| File | Purpose |
|------|---------|
| `report_revenue_trends.csv` | Monthly revenue, order count trends |
| `report_top_products.csv` | Top 10 products by revenue & quantity |
| `report_top_customers.csv` | Top 10 customers by lifetime value |
| `report_geographic_analysis.csv` | Revenue & order volume by country |
| `report_returns_analysis.csv` | Return rates, returned order counts |

## Execution

```bash
make run          # Run full ETL pipeline
make clean        # Reset database
make test         # Run validation checks
```

## Configuration

See `config.py` for:
- Log level & format
- Database paths
- Data file locations
- API credentials


