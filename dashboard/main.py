import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

# ---------------- CONFIG ----------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "detections")

app = FastAPI()

clients = set()
latest_event = {
    "detection_count": 0,
    "detections": [],
    "device_id": None
}


# ---------------- KAFKA CONSUMER ----------------
async def kafka_loop():
    """
    Runs Kafka consumer in a safe async loop using a thread executor internally.
    This avoids thread-to-async websocket race conditions.
    """

    global latest_event

    loop = asyncio.get_running_loop()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="dashboard-consumer"
    )

    def consume():
        nonlocal consumer
        for msg in consumer:
            loop.call_soon_threadsafe(handle_message, msg.value)

    def handle_message(value):
        global latest_event
        latest_event = value

        dead_clients = set()

        for ws in clients:
            try:
                asyncio.create_task(ws.send_json(latest_event))
            except Exception:
                dead_clients.add(ws)

        for d in dead_clients:
            clients.discard(d)

    await asyncio.to_thread(consume)


# ---------------- STARTUP ----------------
@app.on_event("startup")
async def startup():
    asyncio.create_task(kafka_loop())


# ---------------- WEBSOCKET ----------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)

    try:
        # immediate state sync
        await websocket.send_json(latest_event)

        while True:
            await asyncio.sleep(60)

    except WebSocketDisconnect:
        clients.discard(websocket)


# ---------------- UI ----------------
@app.get("/")
def dashboard():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>AI Detection Dashboard</title>
        <style>
            body { font-family: Arial; background:#0f172a; color:#e2e8f0; padding:20px; }
            h1 { color:#38bdf8; }
            .count { font-size: 50px; color:#22c55e; }
        </style>
    </head>

    <body>
        <h1>Live Detection Stream</h1>

        <div>Count:</div>
        <div class="count" id="count">0</div>

        <div>Device:</div>
        <div id="device">-</div>

        <h3>Objects</h3>
        <ul id="list"></ul>

        <script>
            const ws = new WebSocket("ws://" + location.host + "/ws");

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);

                document.getElementById("count").innerText = data.detection_count || 0;
                document.getElementById("device").innerText = data.device_id || "-";

                const list = document.getElementById("list");
                list.innerHTML = "";

                (data.detections || []).forEach(d => {
                    const li = document.createElement("li");

                    const label = d.label || "unknown";
                    const conf = d.confidence
                        ? (d.confidence * 100).toFixed(1)
                        : "0";

                    li.textContent = `${label} (${conf}%)`;
                    list.appendChild(li);
                });
            };
        </script>
    </body>
    </html>
    """)
