import logging

from utils.db import db_connection

logger = logging.getLogger(__name__)


def run() -> None:
    """Create aggregated business metrics tables."""
    logger.info("Creating aggregation tables...")

    try:
        with db_connection() as conn:
            cursor = conn.cursor()

            # 📊 Sales Summary (daily)
            cursor.execute("DROP TABLE IF EXISTS sales_summary")
            cursor.execute("""
            CREATE TABLE sales_summary AS
            SELECT 
                d.full_date,
                SUM(s.total_price) as total_revenue,
                COUNT(DISTINCT s.invoice_no) as total_orders,
                ROUND(AVG(s.total_price), 2) as avg_order_value
            FROM sales s
            JOIN dates d ON s.date_id = d.date_id
            GROUP BY d.full_date
            ORDER BY d.full_date
            """)
            logger.info("✅ Created sales_summary table")

            # 📦 Product Performance
            cursor.execute("DROP TABLE IF EXISTS product_performance")
            cursor.execute("""
            CREATE TABLE product_performance AS
            SELECT 
                p.product_id,
                p.description,
                SUM(s.total_price) as total_sales,
                SUM(s.quantity) as quantity_sold,
                COUNT(DISTINCT s.invoice_no) as order_count,
                ROUND(AVG(s.unit_price), 2) as avg_price
            FROM sales s
            JOIN products p ON s.product_id = p.product_id
            GROUP BY p.product_id, p.description
            ORDER BY total_sales DESC
            """)
            logger.info("✅ Created product_performance table")

            # 👥 Customer Analytics
            cursor.execute("DROP TABLE IF EXISTS customer_analytics")
            cursor.execute("""
            CREATE TABLE customer_analytics AS
            SELECT 
                c.customer_id,
                SUM(s.total_price) as lifetime_value,
                COUNT(DISTINCT s.invoice_no) as total_orders,
                MAX(d.full_date) as last_purchase_date,
                ROUND(AVG(s.total_price), 2) as avg_order_value
            FROM sales s
            JOIN customers c ON s.customer_id = c.customer_id
            JOIN dates d ON s.date_id = d.date_id
            GROUP BY c.customer_id
            ORDER BY lifetime_value DESC
            """)
            logger.info("✅ Created customer_analytics table")

            # 🌍 Country Analysis
            cursor.execute("DROP TABLE IF EXISTS country_analysis")
            cursor.execute("""
            CREATE TABLE country_analysis AS
            SELECT 
                co.country_name,
                SUM(s.total_price) as revenue_per_country,
                COUNT(DISTINCT s.invoice_no) as order_count,
                COUNT(DISTINCT s.customer_id) as unique_customers,
                ROUND(AVG(s.total_price), 2) as avg_order_value
            FROM sales s
            JOIN (SELECT rowid as country_id, country_name FROM countries) co 
              ON s.country_id = co.country_id
            GROUP BY co.country_name
            ORDER BY revenue_per_country DESC
            """)
            logger.info("✅ Created country_analysis table")

            logger.info("All aggregation tables created successfully")
    except Exception as e:
        logger.error(f"Error creating aggregation tables: {e}", exc_info=True)
        raise

