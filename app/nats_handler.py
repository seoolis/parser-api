# nats_handler.py

import json
import logging
from datetime import datetime
from nats.aio.client import Client as NATS

from sqlalchemy import select

from .database import AsyncSessionLocal
from .models import CurrencyRate
from .websocket_manager import manager

logger = logging.getLogger(__name__)

# Глобальный клиент NATS (доступен из других модулей)
nats_client = NATS()
NATS_SUBJECT = "currency.updates"

async def nats_message_handler(msg):
    """
    Обработчик входящих сообщений из NATS:
    - логирует событие
    - обновляет локальную БД (если нужно)
    - рассылает событие всем WebSocket-клиентам
    """
    try:
        data = json.loads(msg.data.decode())
        logger.info(f"NATS message received: {data}")

        async with AsyncSessionLocal() as db:
            code = data.get("code")
            if code:
                result = await db.execute(select(CurrencyRate).where(CurrencyRate.code == code))
                item = result.scalar_one_or_none()
                event_type = data.get("type")

                if event_type in ["auto_update", "manual_patch", "external_update"]:
                    if item:
                        old_rate = item.rate
                        item.rate = data.get("rate", item.rate)
                        item.updated_at = data.get("time", datetime.now().strftime("%H:%M:%S"))
                        await db.commit()
                        logger.info(f"DB updated: {code} {old_rate} -> {item.rate}")
                    else:
                        new_item = CurrencyRate(
                            code=code,
                            name=data.get("name", "Unknown"),
                            rate=data.get("rate", 0.0),
                            updated_at=data.get("time", datetime.now().strftime("%H:%M:%S"))
                        )
                        db.add(new_item)
                        await db.commit()
                        await db.refresh(new_item)
                        logger.info(f"DB created: {code} {new_item.rate}")

        # Рассылка через WebSocket
        await manager.broadcast(json.dumps(data, ensure_ascii=False))

    except Exception as e:
        logger.error(f"NATS handler error: {e}", exc_info=True)


async def setup_nats():
    """
    Подключается к локальному NATS-серверу и подписывается на тему обновлений.
    """
    try:
        await nats_client.connect("nats://127.0.0.1:4222")
        logger.info("Connected to NATS")
        await nats_client.subscribe(NATS_SUBJECT, cb=nats_message_handler)
    except Exception as e:
        logger.error(f"Ошибка NATS: {e}. Проверьте, что nats-server запущен.")