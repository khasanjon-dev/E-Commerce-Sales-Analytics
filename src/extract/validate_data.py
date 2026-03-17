# src/extract/validate_data.py
import json
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Validating raw data...")
    df = pd.read_csv("data/raw/online_retail.csv")
    report = {
        "shape": df.shape,
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
        "sample": df.head(3).to_dict(orient="records"),
    }
    os.makedirs("data/raw", exist_ok=True)
    with open("data/raw/validation_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Validation report saved to data/raw/validation_report.json")
