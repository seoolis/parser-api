# main.py

# Точка входа в приложение: запуск, маршруты, события старта/остановки

import asyncio
import logging
import time
from fastapi import FastAPI, Request
from nats.aio.client import Client as NATS
from starlette.websockets import WebSocketDisconnect

from app.models import CurrencyRate
from app.database import engine, AsyncSessionLocal
from app.nats_handler import setup_nats, NATS_SUBJECT
from app.websocket_manager import manager
from app.tasks import periodic_fetch_task
from app.api import api_router
from app.websocket import websocket_items


# Настройка логирования
logging.basicConfig(
    level=logging.INFO, # Показы INFO, WARNING, ERROR
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Основное приложение FastAPI с названием и версией
app = FastAPI(
    title="Currency Real-time Parser",
    version="2.0",
)

# Middleware: автоматически добавляет заголовок со временем обработки каждого HTTP-запроса
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time() # Запоминаем время начала
    response = await call_next(request)  # Обрабатываем запрос
    response.headers["X-Process-Time"] = str(time.time() - start_time) # Считаем, сколько он занял
    return response # Добавляем в заголовок ответа

# Подключение маршрутов REST API (из api.py)
app.include_router(api_router)

# WebSocket-эндпоинт, по нему клиенты будут подключаться по /ws/items
app.websocket("/ws/items")(websocket_items)

# Действия при запуске приложения
@app.on_event("startup")
async def on_startup():
    # Создание таблиц в SQLite (если не существуют)
    from sqlmodel import SQLModel
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Application started")

    # Подключение к NATS и подписка на тему обновлений
    await setup_nats()

    # Запуск фоновой задачи парсинга курсов
    # asyncio.create_task нужен, чтобы запустить задачу "в фоне", не блокируя запуск сервера
    asyncio.create_task(periodic_fetch_task())
    logger.info("Periodic parser task started")

# Действия при завершении работы приложения
@app.on_event("shutdown")
async def on_shutdown():
    # Корректное закрытие NATS-соединения
    from app.nats_handler import nats_client
    if nats_client.is_connected:
        await nats_client.close()

    # Освобождение ресурсов БД (закрытие пула соединений)
    await engine.dispose()
    logger.info("Application shutdown complete")