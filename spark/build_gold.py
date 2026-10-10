import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, round as _round, to_date

# --- Azure (la clé vient du .env via les variables d'environnement) ---
ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
KEY = os.environ["AZURE_STORAGE_KEY"]
SILVER_PATH = f"abfss://silver@{ACCOUNT}.dfs.core.windows.net/silver_stream"
GOLD_BASE = f"abfss://gold@{ACCOUNT}.dfs.core.windows.net"

spark = (
    SparkSession.builder
    .appName("build_gold")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config(f"spark.hadoop.fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", KEY)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Lire Silver (batch) depuis Azure
silver = spark.read.parquet(SILVER_PATH)
print("lignes dans silver:", silver.count())

# 2. CA par pays
revenue_by_country = (
    silver.groupBy("Country")
    .agg(
        _round(_sum("Revenue"), 2).alias("revenue"),
        countDistinct("InvoiceNo").alias("orders"),
    )
    .orderBy(col("revenue").desc())
)

# 3. CA par jour
revenue_by_day = (
    silver.withColumn("day", to_date("ts"))
    .groupBy("day")
    .agg(
        _round(_sum("Revenue"), 2).alias("revenue"),
        countDistinct("InvoiceNo").alias("orders"),
    )
    .orderBy("day")
)

# 4. Top produits (sans frais de port, saisies manuelles...)
non_produits = ["DOTCOM POSTAGE", "POSTAGE", "Manual", "Discount", "CARRIAGE", "Bank Charges"]
top_products = (
    silver.filter(~col("Description").isin(non_produits))
    .groupBy("Description")
    .agg(
        _round(_sum("Revenue"), 2).alias("revenue"),
        _sum("Quantity").alias("quantity"),
    )
    .orderBy(col("revenue").desc())
    .limit(50)
)

# 5. Écrire Gold dans Azure
revenue_by_country.write.mode("overwrite").parquet(f"{GOLD_BASE}/revenue_by_country")
revenue_by_day.write.mode("overwrite").parquet(f"{GOLD_BASE}/revenue_by_day")
top_products.write.mode("overwrite").parquet(f"{GOLD_BASE}/top_products")

# 6. Aperçu
revenue_by_country.show(5, truncate=False)
top_products.show(5, truncate=False)
print("Gold terminé ->", GOLD_BASE)

spark.stop()