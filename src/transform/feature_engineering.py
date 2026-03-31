import logging

import pandas as pd

from config import PARQUET_ENGINE, PROCESSED_DIR, PROCESSED_PARQUET, CLEAN_PARQUET

logger = logging.getLogger(__name__)


def run() -> None:
    """Apply feature engineering transformations to cleaned data."""
    logger.info("Starting feature engineering...")

    try:
        # 𝟏️⃣ Read cleaned data
        df = pd.read_parquet(CLEAN_PARQUET, engine=PARQUET_ENGINE)
        logger.info(f"Cleaned data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # 𝟐️⃣ Validate datetime column
        if not pd.api.types.is_datetime64_any_dtype(df["invoice_date"]):
            raise ValueError("invoice_date must be datetime type")

        # 𝟑️⃣ Returns flag
        df["is_return"] = (df["quantity"] < 0).astype(int)
        logger.info(f"Flagged {df['is_return'].sum()} return rows")

        # 𝟒️⃣ total_price
        df["total_price"] = df["quantity"] * df["unit_price"]

        # 𝟓️⃣ Date features
        df["year"] = df["invoice_date"].dt.year
        df["month"] = df["invoice_date"].dt.month
        df["day"] = df["invoice_date"].dt.day
        df["day_of_week"] = df["invoice_date"].dt.day_name()

        # 𝟔️⃣ Sort for consistency
        df = df.sort_values("invoice_date")

        # 𝟕️⃣ Save processed data
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(PROCESSED_PARQUET, engine=PARQUET_ENGINE, index=False)

        logger.info(f"Feature-engineered data saved to {PROCESSED_PARQUET}")
        logger.info(f"Final dataset shape: {df.shape}")
    except Exception as e:
        logger.error(f"Error in feature engineering: {e}", exc_info=True)
        raise
