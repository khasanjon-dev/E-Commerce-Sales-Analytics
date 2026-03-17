import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Starting data cleaning...")
    df = pd.read_csv("data/raw/online_retail.csv")
    original_count = len(df)
    logger.info(f"Loaded {original_count} rows")

    # Clean column names
    df.columns = (
        df.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    # Drop missing customer_id
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    logger.info(f"Dropped {before - len(df)} rows with missing customer_id")

    # Convert invoice_date
    df["invoice_date"] = pd.to_datetime(
        df["invoice_date"], format="%d-%m-%Y %H:%M", errors="coerce"
    )
    before = len(df)
    df = df.dropna(subset=["invoice_date"])
    logger.info(f"Dropped {before - len(df)} rows with invalid dates")

    # Ensure numeric types
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["quantity", "unit_price"])
    logger.info(f"Dropped {before - len(df)} rows with non-numeric quantity/price")

    # Flag returns
    df["is_return"] = df["quantity"] < 0
    logger.info(f"Flagged {df['is_return'].sum()} return rows")

    os.makedirs("data/cleaned", exist_ok=True)
    df.to_parquet("data/cleaned/online_retail_cleaned.parquet", index=False)
    logger.info(f"Cleaned data saved with {len(df)} rows")
