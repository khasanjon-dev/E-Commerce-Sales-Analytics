import logging
import os

import kagglehub
import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Downloading dataset from Kaggle...")

    path = kagglehub.dataset_download("tunguz/online-retail")

    csv_path = os.path.join(path, "online_retail.csv")

    logger.info("Reading CSV into memory...")
    df = pd.read_csv(csv_path, encoding="ISO-8859-1")

    # Optional: basic dtype optimization
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%m/%d/%y %H:%M")
    df["CustomerID"] = df["CustomerID"].astype("Int64")

    os.makedirs("data/raw", exist_ok=True)
    parquet_path = "data/raw/online_retail.parquet"

    logger.info("Writing Parquet file...")
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    logger.info(f"Parquet saved to {parquet_path}")
