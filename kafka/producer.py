import json
import time
import pandas as pd
from kafka import KafkaProducer

producer=KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
    
)
df=pd.read_parquet("data/silver_sales.parquet")
df["InvoiceDate"]=df["InvoiceDate"].astype(str)
for i, row in df.iterrows():
    key=str(row["InvoiceNo"])
    value=row.to_dict()
    producer.send("sales_events", key=key, value=value)
    print("Envoyé :", key)
    time.sleep(0.01)
    
producer.flush()
print("Terminé")