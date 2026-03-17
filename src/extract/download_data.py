import logging
import os
import shutil
import kagglehub

logger = logging.getLogger(__name__)


def run():
    logger.info("Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download("tunguz/online-retail")
    os.makedirs("data/raw", exist_ok=True)
    shutil.move(os.path.join(path, "online_retail.csv"), "data/raw/online_retail.csv")
    logger.info("Dataset saved to data/raw/online_retail.csv")
