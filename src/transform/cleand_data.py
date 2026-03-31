import logging

import pandas as pd

from config import (
    CLEAN_DIR,
    CLEAN_PARQUET,
    CRITICAL_COLUMNS,
    PARQUET_ENGINE,
    RAW_PARQUET,
)

logger = logging.getLogger(__name__)


def run() -> None:
    """Clean raw data and save cleaned parquet file."""
    logger.info("Starting data cleaning...")

    try:
        # 𝟏️⃣ Read raw Parquet
        df = pd.read_parquet(RAW_PARQUET, engine=PARQUET_ENGINE)
        logger.info(f"Raw data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # 𝟐️⃣ Normalize column names
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(" ", "_")
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(r"[^\w_]", "", regex=True)

        df.rename(
            columns={
                "invoicedate": "invoice_date",
                "invoiceno": "invoice_no",
                "stockcode": "stock_code",
                "customerid": "customer_id",
                "unitprice": "unit_price",
            },
            inplace=True,
        )

        # 𝟑️⃣ Remove nulls in critical columns
        before = df.shape[0]
        df = df.dropna(subset=CRITICAL_COLUMNS)
        logger.info(f"Removed {before - df.shape[0]} rows with null critical fields")

        # 𝟒️⃣ Remove invalid values
        before = df.shape[0]
        df = df[(df["quantity"] != 0) & (df["unit_price"] > 0)]
        logger.info(f"Removed {before - df.shape[0]} invalid rows")

        # 𝟓️⃣ Convert timestamps
        try:
            df["invoice_date"] = pd.to_datetime(
                df["invoice_date"], format="%m/%d/%y %H:%M"
            )
            logger.info("Converted invoice_date using specified format")
        except Exception as e:
            logger.warning(f"Fallback datetime parsing: {e}")
            df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

        # Drop rows where datetime failed
        before = df.shape[0]
        df = df.dropna(subset=["invoice_date"])
        logger.info(f"Dropped {before - df.shape[0]} rows with invalid dates")

        # 𝟔️⃣ Sort for consistency
        df = df.sort_values("invoice_date")

        # 𝟕️⃣ Save cleaned Parquet
        CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(CLEAN_PARQUET, engine=PARQUET_ENGINE, index=False)

        logger.info(f"Cleaned data saved to {CLEAN_PARQUET}")
        logger.info(f"Final dataset shape: {df.shape}")
    except Exception as e:
        logger.error(f"Error cleaning data: {e}", exc_info=True)
        raise
