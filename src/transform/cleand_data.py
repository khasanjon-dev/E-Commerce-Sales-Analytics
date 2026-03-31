import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Starting data cleaning...")

    # 𝟏️⃣ Read raw Parquet
    raw_path = "data/raw/online_retail.parquet"
    df = pd.read_parquet(raw_path, engine="pyarrow")
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
    critical_cols = [
        "invoice_no",
        "stock_code",
        "quantity",
        "invoice_date",
        "customer_id",
        "unit_price",
    ]

    before = df.shape[0]
    df = df.dropna(subset=critical_cols)
    logger.info(f"Removed {before - df.shape[0]} rows with null critical fields")

    # 𝟒️⃣ Remove invalid values
    before = df.shape[0]
    df = df[(df["quantity"] != 0) & (df["unit_price"] > 0)]
    logger.info(f"Removed {before - df.shape[0]} invalid rows")

    # 𝟓️⃣ Convert timestamps
    try:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], format="%m/%d/%y %H:%M")
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
    clean_dir = "data/clean"
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, "online_retail_clean.parquet")

    df.to_parquet(clean_path, engine="pyarrow", index=False)

    logger.info(f"Cleaned data saved to {clean_path}")
    logger.info(f"Final dataset shape: {df.shape}")
