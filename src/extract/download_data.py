import logging
import os

import kagglehub
import pandas as pd

from config import PARQUET_ENGINE, RAW_DIR, RAW_PARQUET

logger = logging.getLogger(__name__)


def run() -> None:
    """Download online retail dataset from Kaggle and save as Parquet."""
    logger.info("Downloading dataset from Kaggle...")

    try:
        path = kagglehub.dataset_download("tunguz/online-retail")
        csv_path = os.path.join(path, "online_retail.csv")

        logger.info("Reading CSV into memory...")
        df = pd.read_csv(csv_path, encoding="ISO-8859-1")

        # Optimize dtypes for memory efficiency
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%m/%d/%y %H:%M")
        df["CustomerID"] = df["CustomerID"].astype("Int64")
        df["Quantity"] = df["Quantity"].astype("int32")

        # Optimize string columns
        for col in ["InvoiceNo", "StockCode", "Country"]:
            df[col] = df[col].astype("category")

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("Writing Parquet file...")
        df.to_parquet(RAW_PARQUET, engine=PARQUET_ENGINE, index=False)

        logger.info(f"Parquet saved to {RAW_PARQUET}")
    except Exception as e:
        logger.error(f"Error downloading/processing data: {e}", exc_info=True)
        raise
