import logging

from utils.db import get_connection

logger = logging.getLogger(__name__)


def run():
    logger.info("Creating warehouse tables...")

    conn = get_connection()
    cursor = conn.cursor()

    # 🧍 Customers
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY
    )
    """)

    # 📦 Products
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        description TEXT
    )
    """)

    # 🌍 Countries
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS countries
                   (
                       country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                       country_name TEXT UNIQUE
                   )
                   """)

    # 📅 Dates
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dates (
        date_id INTEGER PRIMARY KEY,
        full_date TEXT,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week TEXT
    )
    """)

    # 🧾 Fact Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_no TEXT,
        product_id TEXT,
        customer_id INTEGER,
        country_id INTEGER,
        date_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        total_price REAL,
        is_return INTEGER,

        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (country_id) REFERENCES countries(country_id),
        FOREIGN KEY (date_id) REFERENCES dates(date_id)
    )
    """)

    conn.commit()
    conn.close()

    logger.info("Tables created successfully")
