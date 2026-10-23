import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]
password = os.environ["POSTGRES_PASSWORD"]

storage = {"account_name": account, "account_key": key}
engine = create_engine(
    f"postgresql+psycopg2://ecommerce:{password}@localhost:5433/ecommerce"
)

tables = ["revenue_by_country", "revenue_by_day", "top_products"]

for name in tables:
    path = f"abfs://gold@{account}.dfs.core.windows.net/{name}"
    df = pd.read_parquet(path, storage_options=storage)
    df.to_sql(f"gold_{name}", engine, if_exists="replace", index=False)
    print(f"gold_{name} : {len(df)} lignes chargées")