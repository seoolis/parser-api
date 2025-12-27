# api.py

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Body
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json
from datetime import datetime

from .models import CurrencyRate, CurrencyRateCreate, CurrencyRateResponse
from .database import get_db, AsyncSessionLocal
from .tasks import update_all_currencies_logic
from .nats_handler import nats_client, NATS_SUBJECT

api_router = APIRouter()

@api_router.get("/items", response_model=list[CurrencyRateResponse])
async def get_all_items(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(CurrencyRate))
    return result.scalars().all()

@api_router.get("/items/{item_id}", response_model=CurrencyRateResponse)
async def get_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(404, "Currency not found")
    return item

@api_router.post("/items", response_model=CurrencyRateResponse, status_code=201)
async def create_item(currency: CurrencyRateCreate, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(CurrencyRate).where(CurrencyRate.code == currency.code))
    if existing.scalar_one_or_none():
        raise HTTPException(400, "Currency already exists")

    timestamp = datetime.now().strftime("%H:%M:%S")
    item = CurrencyRate(
        code=currency.code,
        name=currency.name,
        rate=currency.rate,
        updated_at=timestamp,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)

    payload = {
        "type": "created",
        "id": item.id,
        "code": item.code,
        "name": item.name,
        "rate": item.rate,
        "time": timestamp,
    }
    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return item

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
    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return item

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
    await nats_client.publish(NATS_SUBJECT, json.dumps(payload, ensure_ascii=False).encode())
    return {"status": "deleted", "id": item_id}

@api_router.post("/tasks/run")
async def run_manual_task(background_tasks: BackgroundTasks):
    async def job():
        async with AsyncSessionLocal() as db:
            await update_all_currencies_logic(db)

    background_tasks.add_task(job)
    return {"status": "manual task started"}