# websocket.py

import json
from fastapi import WebSocket
from sqlalchemy import select


from .database import AsyncSessionLocal
from .models import CurrencyRate, CurrencyRateResponse
from .websocket_manager import manager

async def websocket_items(websocket: WebSocket):
    """
    WebSocket-эндпоинт:
    1. Принимает подключение
    2. Отправляет текущие курсы валют
    3. Ждёт сообщения (для keep-alive)
    4. Обрабатывает отключение
    """
    await manager.connect(websocket)

    # Отправка текущего состояния при подключении
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(CurrencyRate))
        items = result.scalars().all()

        await websocket.send_text(
            json.dumps(
                {
                    "type": "current_valute",
                    "items": [
                        CurrencyRateResponse.model_validate(i).model_dump()
                        for i in items
                    ],
                },
                ensure_ascii=False,
            )
        )

    try:
        while True:
            await websocket.receive_text()  # keep-alive
    except Exception:
        manager.disconnect(websocket)