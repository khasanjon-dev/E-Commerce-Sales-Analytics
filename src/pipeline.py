import logging
import sys

from src.aggregate import create_aggregates
from src.reports import generate_reports

from src.extract import download_data, validate_data
from src.load import db_setup, load_dimensions, load_fact
from src.transform import clean_data, feature_engineering


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("pipeline.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def run_pipeline():
    logger = logging.getLogger(__name__)
    logger.info("Pipeline started")
    try:
        download_data.run()
        validate_data.run()
        clean_data.run()
        feature_engineering.run()
        db_setup.run()
        load_dimensions.run()
        load_fact.run()
        create_aggregates.run()
        generate_reports.run()
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    setup_logging()
    run_pipeline()
