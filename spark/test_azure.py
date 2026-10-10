import os
from pyspark.sql import SparkSession

account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]

spark = (
    SparkSession.builder
    .appName("test_azure")
    .master("local[*]")
    .config(f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net", key)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = f"abfss://silver@{account}.dfs.core.windows.net/test_connexion"
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.write.mode("overwrite").parquet(path)
print("Relu depuis Azure :", spark.read.parquet(path).count(), "lignes")
spark.stop()