from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, round as _round, to_date

spark=(
    SparkSession.builder
    .appName("build_gold")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
#1 lire silver(batch)
silver=spark.read.parquet("/data/silver_stream")
print("lignes dans silver:",silver.count())

#2 CA PAR PAYS
revenue_by_country = (
    silver.groupBy("Country")
    .agg(
        _round(_sum("Revenue"), 2).alias("revenue"),
        countDistinct("InvoiceNo").alias("orders"),
    )
    .orderBy(col("revenue").desc())
)
# 3 CA par jour
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

# 5. Écrire Gold
revenue_by_country.write.mode("overwrite").parquet("/data/gold/revenue_by_country")
revenue_by_day.write.mode("overwrite").parquet("/data/gold/revenue_by_day")
top_products.write.mode("overwrite").parquet("/data/gold/top_products")

# 6. Aperçu
revenue_by_country.show(5, truncate=False)
top_products.show(5, truncate=False)
print("Gold terminé")

spark.stop()
