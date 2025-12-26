import asyncio
import json
import time
from datetime import datetime
import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Depends, WebSocket, Body
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel, Field, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from starlette.websockets import WebSocketDisconnect
from nats.aio.client import Client as NATS
import socket
import traceback

# --- НАСТРОЙКИ БАЗЫ ДАННЫХ ---
# Используем асинхронный драйвер aiosqlite
DB_URL = "sqlite+aiosqlite:///./currency_database.db"
engine = create_async_engine(DB_URL)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# --- МОДЕЛЬ ДАННЫХ (SQLModel) ---
class CurrencyRate(SQLModel, table=True):
    __tablename__ = 'currency_rates'

    id: int | None = Field(default=None, primary_key=True)  # ID для управления
    code: str = Field(index=True, unique=True)  # Код (USD, EUR)
    name: str  # Название (Доллар США)
    rate: float  # Текущий курс
    updated_at: str  # Время последнего обновления


from pydantic import BaseModel
from typing import Optional

class CurrencyRateResponse(BaseModel):
    id: int
    code: str
    name: str
    rate: float
    updated_at: str

    model_config = {
        "from_attributes": True  # позволяет сериализовать ORM-объекты (SQLModel)
    }

# Зависимость для получения сессии БД в эндпоинтах
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


# --- МЕНЕДЖЕР WEBSOCKET СОЕДИНЕНИЙ ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        """Отправка сообщения всем подключенным клиентам"""
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                continue


manager = ConnectionManager()
nats_client = NATS()


# --- ЛОГИКА ПАРСЕРА (HTTPX + NATS) ---

async def update_all_currencies_logic(db: AsyncSession):
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            if response.status_code != 200:
                print("Ошибка при запросе к API ЦБ")
                return

            data = response.json()
            valutes = data['Valute']
            timestamp = datetime.now().strftime("%H:%M:%S")

            updated_count = 0
            changes_detected = []

            for code, info in valutes.items():
                statement = select(CurrencyRate).where(CurrencyRate.code == code)
                result = await db.execute(statement)
                db_item = result.scalar_one_or_none()

                new_rate = info['Value']

                # ПРОВЕРКА: Изменился ли курс?
                if db_item:
                    if db_item.rate == new_rate:
                        continue  # Пропускаем, если курс такой же

                    old_rate = db_item.rate
                    db_item.rate = new_rate
                    db_item.updated_at = timestamp
                    changes_detected.append(f"{code}: {old_rate} -> {new_rate}")
                else:
                    db_item = CurrencyRate(
                        code=code,
                        name=info['Name'],
                        rate=new_rate,
                        updated_at=timestamp
                    )
                    db.add(db_item)
                    changes_detected.append(f"{code}: NEW -> {new_rate}")

                updated_count += 1

                # Публикуем в NATS только если изменилось
                payload = {"id": db_item.id, "code": code, "rate": new_rate, "time": timestamp}
                await nats_client.publish("currency.updates", json.dumps(payload).encode())

            if updated_count > 0:
                await db.commit()
                print(f"[{timestamp}] Обновлено валют: {updated_count}")
                for change in changes_detected:
                    print(f"  - {change}")
            else:
                print(f"[{timestamp}] Новых курсов не найдено.")


        except Exception:

            print("Ошибка в работе парсера:")

            traceback.print_exc()


async def periodic_fetch_task():
    """Фоновая задача: запускается раз в 120 секунд"""
    while True:
        async with AsyncSessionLocal() as db:
            await update_all_currencies_logic(db)
        await asyncio.sleep(120)  # ИНТЕРВАЛ ОБНОВЛЕНИЯ


# --- FASTAPI ПРИЛОЖЕНИЕ ---

app = FastAPI(title="Currency Real-time Parser", version="2.0")


# Middleware для замера времени обработки запроса
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.on_event("startup")
async def on_startup():
    # 1. Создаем таблицы в БД
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    # 2. Подключаемся к NATS
    try:
        await nats_client.connect("nats://127.0.0.1:4222")

        # Подписываемся на канал NATS и транслируем в WebSocket
        async def nats_handler(msg):
            await manager.broadcast(msg.data.decode())

        await nats_client.subscribe("currency.updates", cb=nats_handler)
    except Exception as e:
        print(f"Ошибка NATS: {e}. Проверьте, запущен ли nats-server.exe")

    # 3. Запускаем автоматический сбор данных в фоновом потоке asyncio
    asyncio.create_task(periodic_fetch_task())


# --- REST API ЭНДПОИНТЫ ---

@app.get("/items", response_model=list[CurrencyRateResponse])
async def get_all_items(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(CurrencyRate))
    return result.scalars().all()


@app.get("/items/{item_id}", response_model=CurrencyRateResponse)
async def get_item_by_id(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Валюта не найдена")
    return item


@app.post("/tasks/run")
async def run_manual_parser(background_tasks: BackgroundTasks):
    """Принудительный запуск сбора данных"""

    async def manual_job():
        async with AsyncSessionLocal() as db:
            await update_all_currencies_logic(db)

    background_tasks.add_task(manual_job)
    return {"status": "Task manual start triggered"}


@app.patch("/items/{item_id}")
async def update_item_manual(item_id: int, new_rate: float = Body(..., embed=True), db: AsyncSession = Depends(get_db)):
    """Изменить курс валюты вручную по ID"""
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Запись не найдена")

    item.rate = new_rate
    item.updated_at = datetime.now().strftime("%H:%M:%S")

    await db.commit()
    await db.refresh(item)

    # Сообщаем об изменении через NATS
    await nats_client.publish("currency.updates",
                              json.dumps({"id": item_id, "event": "manual_patch", "new_rate": new_rate}).encode())
    return item


@app.delete("/items/{item_id}")
async def delete_item(item_id: int, db: AsyncSession = Depends(get_db)):
    """Удалить валюту из базы по ID"""
    item = await db.get(CurrencyRate, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="ID не существует")

    await db.delete(item)
    await db.commit()
    return {"status": "Deleted", "id": item_id}


# --- WEBSOCKET ЭНДПОИНТ ---

@app.websocket("/ws/items")
async def websocket_currency(websocket: WebSocket):
    """Канал для получения живых обновлений курсов"""
    await manager.connect(websocket)
    try:
        while True:
            # Ожидаем данных (пингов) от клиента, чтобы не закрылось соединение
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)