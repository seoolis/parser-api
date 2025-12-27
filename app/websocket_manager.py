# websocket_manager.py

# Управление всеми активными WebSocket-подключениями: подключение, отключение, рассылка

import logging
from fastapi import WebSocket

# Логирование
logger = logging.getLogger(__name__)

# Класс для управления WebSocket-клиентами
class ConnectionManager:
    """
    - connect: принимает соединение
    - disconnect: удаляет из списка
    - broadcast: отправляет сообщение всем
    """
    def __init__(self):
        # Список всех активных WebSocket-соединений
        self.active_connections: list[WebSocket] = []

    # Принимает новое подключение
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("WebSocket client connected")

    # Обрабатывает отключение клиента
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("WebSocket client disconnected")

    # Рассылает одно и то же сообщение всем подключённым клиентам
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                # Отправление текстового сообщения
                await connection.send_text(message)
            except Exception:
                # Если отправка не удалась (клиент отключился), то происходит логирование
                # предупреждения и продолжение работы
                logger.warning("WebSocket send failed")

# Единый менеджер для всего приложения
# Используется в websocket.py и nats_handler.py
manager = ConnectionManager()