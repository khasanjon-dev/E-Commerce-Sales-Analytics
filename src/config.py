from pathlib import Path
from typing import Final

# ============================================================================
# DATA PATHS
# ============================================================================
DATA_DIR: Final[Path] = Path("data")
RAW_DIR: Final[Path] = DATA_DIR / "raw"
CLEAN_DIR: Final[Path] = DATA_DIR / "clean"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
WAREHOUSE_DIR: Final[Path] = DATA_DIR / "warehouse"

# Data files
RAW_PARQUET: Final[Path] = RAW_DIR / "online_retail.parquet"
VALIDATION_REPORT: Final[Path] = RAW_DIR / "validation_report.json"
CLEAN_PARQUET: Final[Path] = CLEAN_DIR / "online_retail_clean.parquet"
PROCESSED_PARQUET: Final[Path] = PROCESSED_DIR / "online_retail_features.parquet"
WAREHOUSE_DB: Final[Path] = WAREHOUSE_DIR / "warehouse.db"

# ============================================================================
# DATABASE CONFIG
# ============================================================================
DB_PATH: Final[Path] = WAREHOUSE_DB
DB_TIMEOUT: Final[int] = 30  # seconds
DB_CHECK_SAME_THREAD: Final[bool] = False

# ============================================================================
# CRITICAL COLUMNS (for data validation)
# ============================================================================
CRITICAL_COLUMNS: Final[tuple] = (
    "invoice_no",
    "stock_code",
    "quantity",
    "invoice_date",
    "customer_id",
    "unit_price",
)

# ============================================================================
# LOGGING
# ============================================================================
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_LEVEL: Final[str] = "INFO"

# ============================================================================
# PARQUET ENGINE
# ============================================================================
PARQUET_ENGINE: Final[str] = "pyarrow"
