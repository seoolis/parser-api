# websocket_manager.py

import logging
from fastapi import WebSocket

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Управляет подключёнными WebSocket-клиентами:
    - connect: принимает соединение
    - disconnect: удаляет из списка
    - broadcast: отправляет сообщение всем
    """
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("WebSocket client connected")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("WebSocket client disconnected")

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                logger.warning("WebSocket send failed")

# Единый менеджер для всего приложения
manager = ConnectionManager()