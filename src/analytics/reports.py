import logging

import pandas as pd

from config import PROCESSED_DIR
from utils.db import db_connection

logger = logging.getLogger(__name__)


def run() -> None:
    """Generate all business reports."""
    logger.info("Generating business reports...")

    try:
        with db_connection() as conn:
            # 📈 Report 1: Revenue Trends
            df = pd.read_sql("""
                SELECT full_date, total_revenue 
                FROM sales_summary 
                ORDER BY full_date
            """, conn)
            df.to_csv(PROCESSED_DIR / "report_revenue_trends.csv", index=False)
            logger.info(f"✅ Revenue Trends Report ({len(df)} days)")

            # 🏆 Report 2: Top Products (by revenue)
            df = pd.read_sql("""
                SELECT product_id, description, total_sales, quantity_sold, order_count
                FROM product_performance
                LIMIT 20
            """, conn)
            df.to_csv(PROCESSED_DIR / "report_top_products.csv", index=False)
            logger.info(f"✅ Top Products Report ({len(df)} products)")

            # 👥 Report 3: Top Customers (by lifetime value)
            df = pd.read_sql("""
                SELECT customer_id, lifetime_value, total_orders, last_purchase_date
                FROM customer_analytics
                ORDER BY lifetime_value DESC
                LIMIT 20
            """, conn)
            df.to_csv(PROCESSED_DIR / "report_top_customers.csv", index=False)
            logger.info(f"✅ Top Customers Report ({len(df)} customers)")

            # 🌍 Report 4: Geographic Analysis
            df = pd.read_sql("""
                SELECT country_name, revenue_per_country, order_count, unique_customers
                FROM country_analysis
                ORDER BY revenue_per_country DESC
            """, conn)
            df.to_csv(PROCESSED_DIR / "report_geographic_analysis.csv", index=False)
            logger.info(f"✅ Geographic Analysis Report ({len(df)} countries)")

            # 🔄 Report 5: Returns Analysis
            df = pd.read_sql("""
                SELECT 
                    p.description,
                    SUM(CASE WHEN s.is_return = 1 THEN 1 ELSE 0 END) as return_count,
                    COUNT(*) as total_orders,
                    ROUND(100.0 * SUM(CASE WHEN s.is_return = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as return_rate_pct
                FROM sales s
                JOIN products p ON s.product_id = p.product_id
                GROUP BY p.description
                ORDER BY return_rate_pct DESC
                LIMIT 30
            """, conn)
            df.to_csv(PROCESSED_DIR / "report_returns_analysis.csv", index=False)
            logger.info(f"✅ Returns Analysis Report ({len(df)} products)")

            logger.info("All business reports generated successfully")
    except Exception as e:
        logger.error(f"Error generating reports: {e}", exc_info=True)
        raise

