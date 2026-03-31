import logging

import pandas as pd

from config import PARQUET_ENGINE, PROCESSED_PARQUET
from utils.db import db_connection

logger = logging.getLogger(__name__)


def run() -> None:
    """Load dimension tables from processed data."""
    logger.info("Loading dimension tables...")

    try:
        df = pd.read_parquet(PROCESSED_PARQUET, engine=PARQUET_ENGINE)

        with db_connection() as conn:
            # 🔥 𝟏️⃣ Clear tables (make pipeline rerunnable)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM customers")
            cursor.execute("DELETE FROM products")
            cursor.execute("DELETE FROM countries")
            cursor.execute("DELETE FROM dates")

            # 🧍 𝟐️⃣ Customers
            customers = df[["customer_id"]].drop_duplicates()
            customers.to_sql("customers", conn, if_exists="replace", index=False)
            logger.info(f"Loaded customers: {len(customers)}")

            # 📦 𝟑️⃣ Products
            products = df[["stock_code", "description"]].drop_duplicates()
            products = products.rename(columns={"stock_code": "product_id"})
            products.to_sql("products", conn, if_exists="replace", index=False)
            logger.info(f"Loaded products: {len(products)}")

            # 🌍 𝟒️⃣ Countries
            countries = df[["country"]].drop_duplicates()
            countries = countries.rename(columns={"country": "country_name"})
            countries.to_sql("countries", conn, if_exists="replace", index=False)
            logger.info(f"Loaded countries: {len(countries)}")

            # 📅 𝟓️⃣ Dates (optimized: create date_id once)
            dates_dim = df[
                ["invoice_date", "year", "month", "day", "day_of_week"]
            ].drop_duplicates()
            dates_dim["date_id"] = (
                dates_dim["invoice_date"].dt.strftime("%Y%m%d").astype(int)
            )
            dates_dim = dates_dim.rename(columns={"invoice_date": "full_date"})
            # Reorder columns to match table schema
            dates_dim = dates_dim[
                ["date_id", "full_date", "year", "month", "day", "day_of_week"]
            ]
            dates_dim.to_sql("dates", conn, if_exists="replace", index=False)
            logger.info(f"Loaded dates: {len(dates_dim)}")

        logger.info("Dimension tables loaded successfully")
    except Exception as e:
        logger.error(f"Error loading dimension tables: {e}", exc_info=True)
        raise
