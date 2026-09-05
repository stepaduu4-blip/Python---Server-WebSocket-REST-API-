import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI(title="Game Server API", version="1.0")

# Хранилище активных игровых комнат
rooms: Dict[str, Set[WebSocket]] = {}
players: Dict[WebSocket, str] = {}  # ws -> player_id

# --- REST эндпоинты ---
@app.get("/")
async def root():
    return {"status": "Game Server Online", "rooms": len(rooms)}

@app.post("/create_room")
async def create_room():
    room_id = str(uuid.uuid4())[:6]
    rooms[room_id] = set()
    return {"room_id": room_id}

@app.delete("/close_room/{room_id}")
async def close_room(room_id: str):
    if room_id not in rooms:
        raise HTTPException(404, "Room not found")
    for ws in rooms[room_id]:
        await ws.close(code=1000)
    del rooms[room_id]
    return {"ok": True}

# --- WebSocket игровой эндпоинт ---
@app.websocket("/ws/{room_id}/{player_name}")
async def game_websocket(websocket: WebSocket, room_id: str, player_name: str):
    await websocket.accept()
    
    if room_id not in rooms:
        rooms[room_id] = set()
    
    player_id = str(uuid.uuid4())[:8]
    rooms[room_id].add(websocket)
    players[websocket] = player_id
    
    # Оповещаем всех в комнате о новом игроке
    await broadcast(room_id, {
        "type": "join",
        "player": player_name,
        "player_id": player_id,
        "timestamp": datetime.now().isoformat()
    })
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                msg["from"] = player_name
                msg["from_id"] = player_id
                msg["timestamp"] = datetime.now().isoformat()
                await broadcast(room_id, msg)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
    except WebSocketDisconnect:
        rooms[room_id].remove(websocket)
        players.pop(websocket, None)
        await broadcast(room_id, {
            "type": "leave",
            "player": player_name,
            "timestamp": datetime.now().isoformat()
        })
        if not rooms[room_id]:
            del rooms[room_id]

# --- Вспомогательная функция рассылки ---
async def broadcast(room_id: str, message: dict):
    if room_id not in rooms:
        return
    dead = set()
    for ws in rooms[room_id]:
        try:
            await ws.send_text(json.dumps(message))
        except:
            dead.add(ws)
    for ws in dead:
        rooms[room_id].discard(ws)
        players.pop(ws, None)

# --- Запуск (если файл запущен напрямую) ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)