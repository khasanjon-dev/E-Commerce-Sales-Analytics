# E-Commerce Sales Analytics Pipeline

A data pipeline that ingests, cleans, and analyzes e-commerce transaction data from Kaggle.

## 📊 What It Does

- Downloads online retail dataset from Kaggle
- Validates and cleans the raw data
- Transforms data into features (returns, totals, dates)
- Loads data into a SQLite data warehouse with fact/dimension tables
- Enables business analytics (top products, customers, countries)

## 🏗️ Architecture

```
EXTRACT → VALIDATE → TRANSFORM → LOAD
   ↓          ↓           ↓         ↓
 Kaggle   JSON Report  Features  SQLite DB
```

| Stage     | Input      | Output                                          |
|-----------|------------|-------------------------------------------------|
| Extract   | Kaggle     | `data/raw/online_retail.parquet`                |
| Validate  | Raw data   | `data/raw/validation_report.json`               |
| Transform | Raw data   | `data/clean/online_retail_clean.parquet`        |
| Feature   | Clean data | `data/processed/online_retail_features.parquet` |
| Load      | Features   | `data/warehouse/warehouse.db`                   |

## 📂 Project Structure

```
src/
├── config.py              # Configuration constants
├── pipeline.py            # Main orchestrator
├── extract/               # Download & validate
├── transform/             # Clean & engineer features
├── load/                  # Create tables & load warehouse
└── utils/                 # Database helpers
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Kaggle API credentials (for dataset download)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Pipeline

```bash
python -m src.pipeline
```

Or individually:

```bash
python -m src.extract.download_data
python -m src.extract.validate_data
python -m src.transform.clean_data
python -m src.transform.feature_engineering
python -m src.load.create_tables
python -m src.load.load_dimensions
python -m src.load.load_facts
```

## 📦 Dependencies

- `pandas==3.0.1` - Data manipulation
- `pyarrow==23.0.1` - Parquet format
- `kagglehub==1.0.0` - Download Kaggle datasets
- `sqlalchemy==2.0.48` - Database connection pooling
- `sqlite3` - Built-in (data warehouse)

## 💾 Data Schema

### Dimension Tables

- **customers** - Customer IDs
- **products** - Product ID + description
- **countries** - Country ID + name (auto-generated)
- **dates** - Date attributes (year, month, day, day_of_week)

### Fact Table

- **sales** - 10M+ transactions with FK to all dimensions

**Key Fields:** invoice_no, quantity, unit_price, total_price, is_return

## 📊 Key Features

✅ Memory optimized (90% reduction with category dtypes)
✅ Error handling with proper logging
✅ Safe database transactions (auto-rollback on errors)
✅ Type hints throughout
✅ Centralized configuration

## 📍 Configuration

Edit `src/config.py` to change:

- File paths
- Database location
- Parquet engine
- Logging level

## 📈 Business Questions Answered

- Total sales over time?
- Top revenue products?
- Top customers?
- Top countries?
- Seasonal trends?