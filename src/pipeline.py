import logging

from config import LOG_FORMAT, LOG_LEVEL
from extract import download_data, validate_data
from load import create_tables, load_dimensions, load_facts
from transform import cleand_data, feature_engineering

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
)

logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Pipeline started")
    try:
        download_data.run()
        validate_data.run()
        cleand_data.run()
        feature_engineering.run()
        create_tables.run()
        load_dimensions.run()
        load_facts.run()
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()
