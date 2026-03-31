import json
import logging

import pandas as pd

from config import PARQUET_ENGINE, RAW_DIR, RAW_PARQUET, VALIDATION_REPORT

logger = logging.getLogger(__name__)


def run() -> None:
    """Validate raw data and generate validation report."""
    logger.info("Validating raw data...")

    try:
        df = pd.read_parquet(RAW_PARQUET, engine=PARQUET_ENGINE)
        report = {
            "shape": df.shape,
            "columns": list(df.columns),
            "missing_values": df.isnull().sum().to_dict(),
            "duplicate_rows": int(df.duplicated().sum()),
            "sample": df.head(3).to_dict(orient="records"),
        }

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        with open(VALIDATION_REPORT, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Validation report saved to {VALIDATION_REPORT}")
    except Exception as e:
        logger.error(f"Error validating data: {e}", exc_info=True)
        raise
