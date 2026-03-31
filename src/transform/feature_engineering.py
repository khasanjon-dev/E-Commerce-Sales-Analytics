import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def run():
    logger.info("Starting feature engineering...")

    # 𝟏️⃣ Read cleaned data
    clean_path = "data/clean/online_retail_clean.parquet"
    df = pd.read_parquet(clean_path, engine="pyarrow")

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
    processed_dir = "data/processed"
    os.makedirs(processed_dir, exist_ok=True)

    fe_path = os.path.join(processed_dir, "online_retail_features.parquet")

    df.to_parquet(fe_path, engine="pyarrow", index=False)

    logger.info(f"Feature-engineered data saved to {fe_path}")
    logger.info(f"Final dataset shape: {df.shape}")
