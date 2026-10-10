from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, regexp_replace, round as _round,
    to_timestamp, trim, when, lower
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("write_silver_stream")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "sales_events")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 20000)
    .load()
)

sales = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

clean = (
    sales
    .withColumn("CustomerID", when(
        col("CustomerID").isNull()
        | (lower(trim(regexp_replace(col("CustomerID"), '"', ''))) == "nan"),
        "Unknown").otherwise(col("CustomerID")))
    .filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
    .withColumn("ts", to_timestamp("InvoiceDate"))
    .drop("InvoiceDate")
    .withColumn("Revenue", _round(col("Quantity") * col("UnitPrice"), 2))
    .dropna(subset=["InvoiceNo", "ts"])
)

query = (
    clean.writeStream
    .format("parquet")
    .option("path", "/data/silver_stream")
    .option("checkpointLocation", "/tmp/checkpoint_silver_stream")
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print("Écriture terminée")