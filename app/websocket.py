# websocket.py

# Обработка WebSocket-подключений: отправка текущих курсов и поддержание соединения

import json
from fastapi import WebSocket
from sqlalchemy import select

from .database import AsyncSessionLocal
from .models import CurrencyRate, CurrencyRateResponse
from .websocket_manager import manager

# Обработчик одного WebSocket-подключения
async def websocket_items(websocket: WebSocket):
    # 1. Регистрируем нового клиента в менеджере подключений
    await manager.connect(websocket)

    # 2. Отправляем клиенту все текущие курсы валют при подключении
    async with AsyncSessionLocal() as db:
        # Запрос всех записей из таблицы курсов
        result = await db.execute(select(CurrencyRate))
        items = result.scalars().all()

        # Преобразование записей в формат для отправки (pydantic-модель)
        # и упаковывание в JSON-сообщение
        await websocket.send_text(
            json.dumps(
                {
                    "type": "current_valute",
                    "items": [
                        # Валидация и превращение в словарь
                        CurrencyRateResponse.model_validate(i).model_dump()
                        for i in items
                    ],
                },
                ensure_ascii=False, # кириллица без экранирования
            )
        )

    # 3. Поддерживаем соединение: просто читаем входящие сообщения, иначе соединение может закрыться
    try:
        while True:
            await websocket.receive_text()  # keep-alive
    except Exception:
        manager.disconnect(websocket)