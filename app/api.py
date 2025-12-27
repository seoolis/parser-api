# api.py

# Основной модуль API маршрутов для управления курсами валют.
# Использует FastAPI для реализации RESTful-интерфейса и NATS для публикации событий.

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Body
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json
from datetime import datetime

from .models import CurrencyRate, CurrencyRateCreate, CurrencyRateResponse
from .database import get_db, AsyncSessionLocal
from .tasks import update_all_currencies_logic
from .nats_handler import nats_client, NATS_SUBJECT

# Маршрутизатор

api_router = APIRouter()

# Получение всех записей о курсах валют

@api_router.get("/items", response_model=list[CurrencyRateResponse])
async def get_all_items(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(CurrencyRate))
    return result.scalars().all()

# Получение конкретной валюты по ID

@api_router.get("/items/{item_id}", response_model=CurrencyRateResponse)
async def get_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(404, "Currency not found")
    return item

# Создание новой записи о валюте

@api_router.post("/items", response_model=CurrencyRateResponse, status_code=201)
async def create_item(currency: CurrencyRateCreate, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(CurrencyRate).where(CurrencyRate.code == currency.code))

    # Проверка на существование валюты с таким же кодом

    if existing.scalar_one_or_none():
        raise HTTPException(400, "Currency already exists")

    timestamp = datetime.now().strftime("%H:%M:%S")

    # Создание и сохранение новой записи

    item = CurrencyRate(
        code=currency.code,
        name=currency.name,
        rate=currency.rate,
        updated_at=timestamp,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item) # Обновление объект, чтобы получить присвоенный ID

    payload = {
        "type": "created",
        "id": item.id,
        "code": item.code,
        "name": item.name,
        "rate": item.rate,
        "time": timestamp,
    }

    # Публикация события в NATS

    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return item

# Обновление курса валюты вручную

@api_router.patch("/items/{item_id}", response_model=CurrencyRateResponse)
async def update_item(
    item_id: int,
    new_rate: float = Body(..., embed=True),
    db: AsyncSession = Depends(get_db),
):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(404, "Item not found")

    old_rate = item.rate
    item.rate = new_rate
    item.updated_at = datetime.now().strftime("%H:%M:%S")
    await db.commit()
    await db.refresh(item)

    payload = {
        "type": "manual_patch",
        "id": item.id,
        "code": item.code,
        "old": old_rate,
        "new": new_rate,
        "time": item.updated_at,
    }

    # Публикация события в NATS

    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return item

# Удаление записи о валюте

@api_router.delete("/items/{item_id}")
async def delete_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(404, "Item not found")

    await db.delete(item)
    await db.commit()

    payload = {
        "type": "deleted",
        "id": item_id,
        "code": item.code,
        "time": datetime.now().strftime("%H:%M:%S"),
    }

    # Публикация события в NATS

    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return {"status": "deleted", "id": item_id}

# Запуск фоновой задачи обновления всех курсов

@api_router.post("/tasks/run")
async def run_manual_task(background_tasks: BackgroundTasks):
    async def job():
        async with AsyncSessionLocal() as db:
            await update_all_currencies_logic(db)

    background_tasks.add_task(job)
    return {"status": "manual task started"}