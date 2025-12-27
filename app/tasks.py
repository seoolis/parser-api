# tasks.py

# Фоновые задачи: автоматическое обновление курсов валют с сайта ЦБ РФ

import asyncio

import httpx
import json
import logging
from datetime import datetime
from sqlalchemy import select

from .models import CurrencyRate
from .database import AsyncSessionLocal
from .nats_handler import nats_client, NATS_SUBJECT

# Логирование

logger = logging.getLogger(__name__)

# Основная логика обновления курсов - можно вызвать вручную или каждые 2 минуты автоматически
async def update_all_currencies_logic(db: AsyncSessionLocal):
    """
    Основная логика фоновой задачи:
    1. Делает HTTP-запрос к API ЦБ РФ
    2. Сравнивает полученные курсы с текущими в БД
    3. Обновляет или создаёт записи
    4. Публикует события в NATS для уведомления других компонентов
    """
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    timestamp = datetime.now().strftime("%H:%M:%S")

    try:
        # Асинхронный HTTP-запрос к API ЦБ
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url)
            response.raise_for_status()

        # Парсинг JSON-ответа
        data = response.json()
        valutes = data["Valute"]

        # Список новых валют, которые нужно будет опубликовать
        new_items_for_publish = []

        # Обход валют из ответа ЦБ
        for code, info in valutes.items():
            result = await db.execute(select(CurrencyRate).where(CurrencyRate.code == code))
            db_item = result.scalar_one_or_none()
            new_rate = info["Value"]

            if db_item:
                if db_item.rate == new_rate:
                    continue  # Если курс не изменился, то пропуск

                # Старый курс валюты для логирования
                old_rate = db_item.rate
                # Обновление данных в БД
                db_item.rate = new_rate
                db_item.updated_at = timestamp

                # Событие для NATS: курс обновлён автоматически
                payload = {
                    "type": "auto_update",
                    "id": db_item.id,
                    "code": code,
                    "name": info["Name"],
                    "rate": new_rate,
                    "old": old_rate,
                    "new": new_rate,
                    "time": timestamp,
                }

                # Публикация события в NATS
                # другие сервисы (WebSocket и др.) должны получить уведомление
                await nats_client.publish(
                    NATS_SUBJECT,
                    json.dumps(payload, ensure_ascii=False).encode(),
                )
            else:
                # Если валюты ещё нет в базе, то происходит создание новой записи
                db_item = CurrencyRate(
                    code=code,
                    name=info["Name"],
                    rate=new_rate,
                    updated_at=timestamp,
                )
                db.add(db_item)
                new_items_for_publish.append((db_item, info["Name"], new_rate))

        # Сохранение всех изменений в базе (обновления + новые записи)
        await db.commit()


        # Публикация событий для только что созданных валют
        for item, name, rate in new_items_for_publish:
            await db.refresh(item)
            payload = {
                "type": "created",
                "id": item.id,
                "code": item.code,
                "name": name,
                "rate": rate,
                "time": timestamp,
            }
            await nats_client.publish(
                NATS_SUBJECT,
                json.dumps(payload, ensure_ascii=False).encode(),
            )

        logger.info("Currency rates updated successfully")

    except Exception:
        logger.error("Parser execution failed", exc_info=True)


# Бесконечная фоновая задача: запускает обновление каждые 2 минуты
async def periodic_fetch_task():
    """
    Фоновая задача: запускает парсинг каждые 120 секунд.
    """
    logger.info("Periodic parser task started")
    while True:
        try:
            # Открытие новой сессии с БД и запуск обновления
            async with AsyncSessionLocal() as db:
                await update_all_currencies_logic(db)
        except Exception:
            # Если произошла ошибка, например, задача упала, то происходит логирование,
            # но цикл продолжается
            logger.error("Periodic task crashed", exc_info=True)
        # Ожидание 120 секунд (2 минуты) перед следующим запуском
        await asyncio.sleep(120)