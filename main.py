from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json

app = FastAPI()
clients = {}

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    print(f"✅ Połączono: {client_id} (online: {list(clients.keys())})")
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        if client_id in clients:
            del clients[client_id]
        print(f"❌ Rozłączono: {client_id} (online: {list(clients.keys())})")

@app.post("/command/{client_id}")
async def send_command(client_id: str, url: str):
    if client_id in clients:
        await clients[client_id].send_text(json.dumps({"action": "request", "url": url}))
        return {"status": "sent", "client": client_id}
    return {"error": "client not found", "client": client_id}

@app.get("/")
async def root():
    return {"online": list(clients.keys())}
