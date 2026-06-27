import os
import time
import json
import cv2
from ultralytics import YOLO
from kafka import KafkaProducer

STREAM_URL = os.environ.get("CAMERA_STREAM_URL", "http://192.168.0.104:8080/video")
MODEL_PATH = os.environ.get("MODEL_PATH", "yolo11n.pt")
CONF_THRESHOLD = float(os.environ.get("CONF_THRESHOLD", "0.5"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "detections")

def connect_producer():
    """Retries connecting to Kafka since it may still be starting up."""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"[startup] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"[startup] Kafka not ready yet ({e}), retrying in 3s...")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after multiple attempts")

def main():
    print(f"[startup] Loading model: {MODEL_PATH}")
    model = YOLO(MODEL_PATH)

    producer = connect_producer()

    print(f"[startup] Connecting to camera stream: {STREAM_URL}")
    cap = cv2.VideoCapture(STREAM_URL)

    if not cap.isOpened():
        print("[error] Could not open camera stream. Check URL and network.")
        return

    print("[startup] Connected. Starting inference loop...")

    while True:
        ret, frame = cap.read()
        if not ret:
            print("[warn] Lost frame, retrying in 2s...")
            time.sleep(2)
            cap = cv2.VideoCapture(STREAM_URL)
            continue

        results = model(frame, verbose=False)[0]

        detections = []
        for box in results.boxes:
            conf = float(box.conf[0])
            if conf < CONF_THRESHOLD:
                continue
            cls_id = int(box.cls[0])
            label = model.names[cls_id]
            xyxy = box.xyxy[0].tolist()
            detections.append({
                "label": label,
                "confidence": round(conf, 3),
                "bbox": [round(x, 1) for x in xyxy]
            })

        event = {
            "timestamp": time.time(),
            "device_id": os.environ.get("DEVICE_ID", "edge-device-01"),
            "detections": detections,
            "detection_count": len(detections)
        }

        producer.send(KAFKA_TOPIC, value=event)
        print(f"[sent] {event['detection_count']} detections")

if __name__ == "__main__":
    main()
