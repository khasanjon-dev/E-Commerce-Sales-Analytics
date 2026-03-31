import logging

import pandas as pd

from config import PARQUET_ENGINE, PROCESSED_PARQUET
from utils.db import db_connection

logger = logging.getLogger(__name__)


def run() -> None:
    """Load fact table from processed data with FK validation."""
    logger.info("Loading fact table...")

    try:
        df = pd.read_parquet(PROCESSED_PARQUET, engine=PARQUET_ENGINE)

        with db_connection() as conn:
            cursor = conn.cursor()

            # 🔥 𝟏️⃣ Clear fact table (important)
            cursor.execute("DELETE FROM sales")

            # 🌍 𝟐️⃣ Load country mapping
            countries_df = pd.read_sql(
                "SELECT rowid as country_id, country_name FROM countries", conn
            )
            country_map = dict(
                zip(countries_df["country_name"], countries_df["country_id"])
            )

            # 🧾 𝟑️⃣ Prepare fact data
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

            # 🚨 Optional safety: drop rows with missing FK
            before = fact_table.shape[0]
            fact_table = fact_table.dropna()
            dropped_rows = before - fact_table.shape[0]
            if dropped_rows > 0:
                logger.info(f"Dropped {dropped_rows} rows due to missing FK mapping")

            # 💾 𝟒️⃣ Insert into DB
            fact_table.to_sql("sales", conn, if_exists="append", index=False)

            logger.info(f"Loaded sales rows: {len(fact_table)}")
            logger.info("Fact table loaded successfully")
    except Exception as e:
        logger.error(f"Error loading fact table: {e}", exc_info=True)
        raise
