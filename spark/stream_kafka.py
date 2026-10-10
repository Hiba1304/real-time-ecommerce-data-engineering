from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
#demarer spark avec le contenneur kafka

spark = (
    SparkSession.builder
    .appName("sales_streaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# 2. Le schéma : la structure d'une vente (comme dans  JSON)
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])
# 3. Lire le flux de Kafka
df =( spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092")\
    .option("subscribe", "sales_events") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()
)
# 4. octets → colonnes 
sales = (
    df.select(
        from_json(col("value").cast("string"),
                  schema).alias("data"))
    .select("data.*")
)
# 5. Afficher dans la console
query = (
    sales.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
