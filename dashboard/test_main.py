from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "detections",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="test-group"
)

for msg in consumer:
    print("Received:", msg.value)
