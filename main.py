import asyncio
import json
import time
import logging
from datetime import datetime

import httpx
from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    BackgroundTasks,
    Depends,
    WebSocket,
    Body,
)
from sqlmodel import SQLModel, Field, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from starlette.websockets import WebSocketDisconnect
from nats.aio.client import Client as NATS
from pydantic import BaseModel

# =========================================================
# ЛОГИРОВАНИЕ
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# =========================================================
# БАЗА ДАННЫХ
# =========================================================

DB_URL = "sqlite+aiosqlite:///./currency_database.db"
engine = create_async_engine(DB_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


# =========================================================
# МОДЕЛЬ
# =========================================================

class CurrencyRate(SQLModel, table=True):
    __tablename__ = "currency_rates"

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(index=True, unique=True)
    name: str
    rate: float
    updated_at: str


# =========================================================
# WEBSOCKET MANAGER
# =========================================================

class ConnectionManager:
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


manager = ConnectionManager()

# =========================================================
# NATS
# =========================================================

nats_client = NATS()
NATS_SUBJECT = "currency.updates"

# =========================================================
# Pydantic схемы (фиксированный порядок полей)
# =========================================================

class CurrencyRateResponse(BaseModel):
    id: int
    code: str
    name: str
    rate: float
    updated_at: str

    model_config = {"from_attributes": True}


class CurrencyRateCreate(BaseModel):
    code: str
    name: str
    rate: float


# =========================================================
# БИЗНЕС-ЛОГИКА ПАРСЕРА
# =========================================================

async def update_all_currencies_logic(db: AsyncSession):
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    timestamp = datetime.now().strftime("%H:%M:%S")

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url)
            response.raise_for_status()

        data = response.json()
        valutes = data["Valute"]

        new_items_for_publish = []

        for code, info in valutes.items():
            result = await db.execute(
                select(CurrencyRate).where(CurrencyRate.code == code)
            )
            db_item = result.scalar_one_or_none()

            new_rate = info["Value"]

            if db_item:
                if db_item.rate == new_rate:
                    continue

                old_rate = db_item.rate
                db_item.rate = new_rate
                db_item.updated_at = timestamp

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

                await nats_client.publish(
                    NATS_SUBJECT,
                    json.dumps(payload, ensure_ascii=False).encode(),
                )

            else:
                db_item = CurrencyRate(
                    code=code,
                    name=info["Name"],
                    rate=new_rate,
                    updated_at=timestamp,
                )
                db.add(db_item)
                new_items_for_publish.append((db_item, info["Name"], new_rate))

        await db.commit()

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


async def periodic_fetch_task():
    logger.info("Periodic parser task started")

    while True:
        try:
            async with AsyncSessionLocal() as db:
                await update_all_currencies_logic(db)
        except Exception:
            logger.error("Periodic task crashed", exc_info=True)

        await asyncio.sleep(120)


# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI(
    title="Currency Real-time Parser",
    version="2.0",
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    response.headers["X-Process-Time"] = str(time.time() - start_time)
    return response


# =========================================================
# STARTUP / SHUTDOWN
# =========================================================

@app.on_event("startup")
async def on_startup():
    # --- Создание таблиц в БД ---
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Application started")

    # --- Подключение к NATS ---
    try:
        await nats_client.connect("nats://127.0.0.1:4222")
        logger.info("Connected to NATS")

        # --- Обработчик входящих NATS сообщений ---
        async def nats_handler(msg):
            try:
                data = json.loads(msg.data.decode())
                logger.info(f"NATS message received: {data}")

                # Обновление локальной БД
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

                # Рассылка в WebSocket
                await manager.broadcast(json.dumps(data, ensure_ascii=False))

            except Exception as e:
                logger.error(f"NATS handler error: {e}", exc_info=True)

        # Подписка на канал
        await nats_client.subscribe("currency.updates", cb=nats_handler)

    except Exception as e:
        logger.error(f"Ошибка NATS: {e}. Проверьте, что nats-server запущен.")

    # --- Запуск фоновой задачи парсера ---
    asyncio.create_task(periodic_fetch_task())
    logger.info("Periodic parser task started")



@app.on_event("shutdown")
async def on_shutdown():
    if nats_client.is_connected:
        await nats_client.close()

    await engine.dispose()
    logger.info("Application shutdown complete")


# =========================================================
# REST API
# =========================================================

@app.get("/items", response_model=list[CurrencyRateResponse])
async def get_all_items(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(CurrencyRate))
    return result.scalars().all()


@app.get("/items/{item_id}", response_model=CurrencyRateResponse)
async def get_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(404, "Currency not found")
    return item


@app.post("/items", response_model=CurrencyRateResponse, status_code=201)
async def create_item(
    currency: CurrencyRateCreate,
    db: AsyncSession = Depends(get_db),
):
    existing = await db.execute(
        select(CurrencyRate).where(CurrencyRate.code == currency.code)
    )
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

    await nats_client.publish(
        NATS_SUBJECT,
        json.dumps(payload, ensure_ascii=False).encode(),
    )

    return item


@app.patch("/items/{item_id}", response_model=CurrencyRateResponse)
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

    await nats_client.publish(
        NATS_SUBJECT,
        json.dumps(payload, ensure_ascii=False).encode(),
    )

    return item


@app.delete("/items/{item_id}")
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

    await nats_client.publish(
        NATS_SUBJECT,
        json.dumps(payload, ensure_ascii=False).encode(),
    )

    return {"status": "deleted", "id": item_id}


@app.post("/tasks/run")
async def run_manual_task(background_tasks: BackgroundTasks):
    async def job():
        async with AsyncSessionLocal() as db:
            await update_all_currencies_logic(db)

    background_tasks.add_task(job)
    return {"status": "manual task started"}


# =========================================================
# WEBSOCKET
# =========================================================

@app.websocket("/ws/items")
async def websocket_items(websocket: WebSocket):
    await manager.connect(websocket)

    # current_valute при подключении
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
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
