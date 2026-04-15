from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

producer.send("osint.raw.maigret.v1", {"status": "ok"})
producer.flush()

print("sent")