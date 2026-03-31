import logging

import pandas as pd

from utils.db import get_connection

logger = logging.getLogger(__name__)


def run():
    logger.info("Loading dimension tables...")

    df = pd.read_parquet("data/processed/online_retail_features.parquet")

    conn = get_connection()

    # 🧍 Customers
    customers = df[["customer_id"]].drop_duplicates()
    customers.to_sql("customers", conn, if_exists="append", index=False)
    logger.info(f"Loaded customers: {len(customers)}")

    # 📦 Products
    products = df[["stock_code", "description"]].drop_duplicates()
    products.rename(columns={"stock_code": "product_id"}, inplace=True)
    products.to_sql("products", conn, if_exists="append", index=False)
    logger.info(f"Loaded products: {len(products)}")

    # 🌍 Countries
    countries = df[["country"]].drop_duplicates()
    countries.rename(columns={"country": "country_name"}, inplace=True)
    countries.to_sql("countries", conn, if_exists="append", index=False)
    logger.info(f"Loaded countries: {len(countries)}")

    # 📅 Dates
    dates = df.copy()
    dates["date_id"] = dates["invoice_date"].dt.strftime("%Y%m%d").astype(int)

    dates_dim = dates[
        ["date_id", "invoice_date", "year", "month", "day", "day_of_week"]
    ].drop_duplicates()

    dates_dim.rename(columns={"invoice_date": "full_date"}, inplace=True)

    dates_dim.to_sql("dates", conn, if_exists="append", index=False)
    logger.info(f"Loaded dates: {len(dates_dim)}")

    conn.close()

    logger.info("Dimension tables loaded")
