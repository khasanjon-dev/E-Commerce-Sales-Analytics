import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Starting data cleaning...")

    # Read raw Parquet
    raw_path = "data/raw/online_retail.parquet"
    df = pd.read_parquet(raw_path, engine="pyarrow")
    logger.info(f"Raw data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

    # 1️⃣ Remove rows with nulls in critical fields
    critical_cols = [
        "InvoiceNo",
        "StockCode",
        "Quantity",
        "InvoiceDate",
        "CustomerID",
        "UnitPrice",
    ]
    initial_rows = df.shape[0]
    df = df.dropna(subset=critical_cols)
    removed_nulls = initial_rows - df.shape[0]
    logger.info(f"Removed {removed_nulls} rows with null critical fields")

    # 2️⃣ Handle negative quantities (returns)
    # Option: keep them but flag
    df["is_return"] = df["Quantity"] < 0
    negative_count = df["is_return"].sum()
    logger.info(f"Flagged {negative_count} returned rows (negative quantity)")

    # 3️⃣ Convert timestamps
    try:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%m/%d/%y %H:%M")
        logger.info("Converted InvoiceDate to datetime")
    except Exception as e:
        logger.warning(
            f"Failed to convert InvoiceDate with specific format, falling back: {e}"
        )
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # Optional: extract date features
    df["year"] = df["InvoiceDate"].dt.year
    df["month"] = df["InvoiceDate"].dt.month
    df["day"] = df["InvoiceDate"].dt.day
    df["day_of_week"] = df["InvoiceDate"].dt.day_name()

    # Save cleaned Parquet
    clean_dir = "data/clean"
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, "online_retail_clean.parquet")
    df.to_parquet(clean_path, engine="pyarrow", index=False)
    logger.info(f"Cleaned data saved to {clean_path} ({df.shape[0]} rows)")
