import os
import json
import asyncio
import threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

# Environment variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "detections")

app = FastAPI()

# Track connected clients
clients = set()
latest_event = {"detection_count": 0, "detections": [], "device_id": None}


# ---------------- Kafka Listener ----------------
def kafka_worker(loop):
    global latest_event

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-group"
    )

    for msg in consumer:
        latest_event = msg.value

        # Broadcast to all connected WebSocket clients
        for ws in list(clients):
            asyncio.run_coroutine_threadsafe(
                ws.send_json(latest_event),
                loop
            )


@app.on_event("startup")
def startup():
    loop = asyncio.get_event_loop()
    thread = threading.Thread(target=kafka_worker, args=(loop,), daemon=True)
    thread.start()


# ---------------- WebSocket ----------------
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)

    try:
        # Send the latest snapshot immediately
        await websocket.send_json(latest_event)

        # Keep connection alive without blocking
        while True:
            await asyncio.sleep(10)

    except WebSocketDisconnect:
        clients.remove(websocket)


# ---------------- UI ----------------
@app.get("/")
def ui():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Live AI Dashboard</title>
        <style>
            body { font-family: Arial; background:#111; color:#eee; padding:20px; }
            h1 { color:#38bdf8; }
            .count { font-size: 48px; color:#22c55e; }
            ul { list-style:none; padding:0; }
        </style>
    </head>

    <body>
        <h1>Live Detection Dashboard</h1>

        <div>Objects detected:</div>
        <div class="count" id="count">0</div>

        <div>Device:</div>
        <div id="device">-</div>

        <h3>Detections</h3>
        <ul id="list"></ul>

        <script>
            const ws = new WebSocket("ws://" + location.host + "/ws");

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);

                document.getElementById("count").innerText = data.detection_count;
                document.getElementById("device").innerText = data.device_id || "-";

                const list = document.getElementById("list");
                list.innerHTML = "";

                data.detections.forEach(d => {
                    const li = document.createElement("li");
                    li.textContent = `${d.label} (${(d.confidence*100).toFixed(1)}%)`;
                    list.appendChild(li);
                });
            };
        </script>
    </body>
    </html>
    """)
