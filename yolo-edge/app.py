import os
import time
import json
import cv2
from ultralytics import YOLO
from kafka import KafkaProducer

STREAM_URL = os.environ.get("CAMERA_STREAM_URL")
MODEL_PATH = os.environ.get("MODEL_PATH", "yolo11n.pt")
CONF_THRESHOLD = float(os.environ.get("CONF_THRESHOLD", "0.5"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")


def connect_producer():
    for _ in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
        except Exception as e:
            print("[kafka] retrying...", e)
            time.sleep(3)

    raise RuntimeError("Kafka not reachable")


def main():
    model = YOLO(MODEL_PATH)
    producer = connect_producer()

    cap = cv2.VideoCapture(STREAM_URL)

    if not cap.isOpened():
        print("[error] camera not reachable")
        return

    print("[startup] YOLO edge running")

    while True:
        ret, frame = cap.read()
        if not ret:
            time.sleep(2)
            continue

        results = model(frame, verbose=False)[0]

        detections = []

        for box in results.boxes:
            conf = float(box.conf[0])
            if conf < CONF_THRESHOLD:
                continue

            cls_id = int(box.cls[0])
            label = model.names[cls_id]

            detections.append({
                "label": label,
                "confidence": round(conf, 3)
            })

        event = {
            "timestamp": time.time(),
            "device_id": os.environ.get("DEVICE_ID", "edge-01"),
            "detections": detections,
            "detection_count": len(detections)
        }

        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()

        print("[sent]", len(detections))

        time.sleep(0.03)


if __name__ == "__main__":
    main()
