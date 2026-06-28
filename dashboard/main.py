import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

app = FastAPI()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("KAFKA_TOPIC")

clients = set()
latest_event = {}


async def kafka_loop():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id=None
    )

    while True:
        for msg in consumer.poll(timeout_ms=1000).values():
            for record in msg:
                data = record.value

                for ws in list(clients):
                    try:
                        await ws.send_json(data)
                    except:
                        clients.discard(ws)

        await asyncio.sleep(0.01)


@app.on_event("startup")
async def startup():
    asyncio.create_task(kafka_loop())


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)

    try:
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        clients.discard(websocket)


@app.get("/")
def ui():
    return HTMLResponse("""
    <html>
    <body style="background:#0f172a;color:white;font-family:Arial">
        <h1>Live Detection</h1>
        <h2 id="count">0</h2>
        <ul id="list"></ul>

        <script>
            const ws = new WebSocket("ws://" + location.host + "/ws");

            ws.onmessage = (e) => {
                const d = JSON.parse(e.data);

                document.getElementById("count").innerText = d.detection_count || 0;

                const list = document.getElementById("list");
                list.innerHTML = "";

                (d.detections || []).forEach(x => {
                    const li = document.createElement("li");
                    li.innerText = x.label + " " + (x.confidence * 100).toFixed(1) + "%";
                    list.appendChild(li);
                });
            };
        </script>
    </body>
    </html>
    """)
