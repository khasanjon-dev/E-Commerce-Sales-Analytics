import logging

import pandas as pd

from utils.db import get_connection

logger = logging.getLogger(__name__)


def run():
    logger.info("Loading fact table...")

    df = pd.read_parquet("data/processed/online_retail_features.parquet")

    conn = get_connection()

    # Load country mapping
    countries_df = pd.read_sql("SELECT * FROM countries", conn)
    country_map = dict(zip(countries_df["country_name"], countries_df["country_id"]))

    # Prepare fact table
    fact = df.copy()

    fact["product_id"] = fact["stock_code"]
    fact["date_id"] = fact["invoice_date"].dt.strftime("%Y%m%d").astype(int)
    fact["country_id"] = fact["country"].map(country_map)

    fact_table = fact[
        [
            "invoice_no",
            "product_id",
            "customer_id",
            "country_id",
            "date_id",
            "quantity",
            "unit_price",
            "total_price",
            "is_return",
        ]
    ]

    fact_table.to_sql("sales", conn, if_exists="append", index=False)

    logger.info(f"Loaded sales rows: {len(fact_table)}")

    conn.close()

    logger.info("Fact table loaded")
