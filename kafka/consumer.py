from kafka import KafkaConsumer
import json 

consumer=KafkaConsumer(
    "sales_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="test_consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=5000
)

count=0
for message in consumer:
    event=message.value
    count+=1
    if count<=10:
        print(f"partition={message.partition} offset={message.offset} "
              f"facture={event['InvoiceNo']} produit={event['Description']}")

print("Messages lus :", count)