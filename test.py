import pandas as pd
df = pd.read_parquet("data/clean/online_retail_clean.parquet", engine="pyarrow")
print(df["invoice_date"].head())
# 2010-12-01 08:26:00