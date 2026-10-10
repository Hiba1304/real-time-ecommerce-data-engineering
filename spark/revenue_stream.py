from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json,col,regexp_replace,round as _round,to_timestamp,trim,when,lower,window,sum as spark_sum)
from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,DoubleType)
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
    .option("maxOffsetsPerTrigger", 20000) \
    .load()
)
# 4. octets → colonnes 
sales = (
    df.select(
        from_json(col("value").cast("string"),
                  schema).alias("data"))
    .select("data.*")
)
#nettoyage 


step1= sales.withColumn("CustomerID",when(col("CustomerID").isNull() | (lower(trim(regexp_replace(col("CustomerID"), '"', ''))) == "nan"),"Unknown").otherwise(col("CustomerID")))
step2=step1.filter((col("Quantity")>0)&(col("UnitPrice")>0))
step3 = (step2.withColumn("ts", to_timestamp("InvoiceDate")).drop("InvoiceDate"))
#InvoiceDate est du texte. On crée une nouvelle colonne ts qui est une vraie date, indispensable pour découper par jour avec window."2010-12-01 08:26:00" (texte)  →  ts = 2010-12-01 08:26:00 (date)
step4=step3.withColumn("Revenue", _round(col("Quantity")*col("UnitPrice"),2))
clean=step4.dropna(subset=["InvoiceNo","ts"])
aggregated = (
    clean
    .withWatermark("ts", "1 minute")
    .groupBy(window(col("ts"), "1 minute"))
    .agg(
        _round(spark_sum("Revenue"), 2).alias("total_revenue")
    )
)
query = (
    aggregated.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoint_sales_v3")
    .start()
)
query.awaitTermination()